#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script will get deployed in the bin directory of the
users' virtualenv when the parent module is installed using pip.
"""

import argparse
import csv
import logging
import sys
import traceback
from enum import Enum
from typing import Any, Dict, List, NamedTuple, Tuple
from zipfile import ZipFile

from distributed import LocalCluster
from prefect import Flow, flatten, task, unmapped
from prefect.tasks.control_flow import FilterTask
from prefect.engine.executors import DaskExecutor
from pymongo import ASCENDING

from siglatools import get_module_version

from ..databases.constants import DatabaseCollection, Environment, VariableType
from ..databases.mongodb_database import MongoDBDatabase
from ..institution_extracters.constants import GoogleSheetsFormat
from ..institution_extracters.utils import FormattedSheetData
from .run_sigla_pipeline import _extract, _transform

###############################################################################

logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)
log = logging.getLogger()

###############################################################################


class Datasource(Enum):
    googlesheet = "GoogleSheet"
    database = "Database"


class GsInstitution(NamedTuple):
    spreadsheet_id: str
    spreadsheet_title: str
    sheet_id: str
    sheet_title: str
    data: Any

    def get_key(self) -> str:
        country = self.data.get("country")
        category = self.data.get("category")
        name = self.data.get("name")
        return f"{country}{category}{name}"


class FieldComparison(NamedTuple):
    field: str
    actual: Tuple[str, Any]
    expected: Tuple[str, Any]

    def has_error(self) -> bool:
        return self.actual[1] == self.expected[1]

    def get_msg(self) -> str:
        return f"{self.field}| {self.actual[0]}: {self.actual[1]}, {self.expected[0]}: {self.expected[1]}"


class ObjectComparison(NamedTuple):
    name: str
    field_comparisons: List[FieldComparison]

    def has_error(self) -> bool:
        return any([comparison.has_error() for comparison in self.field_comparisons])

    def get_error_msgs(self) -> List[str]:
        return [
            comparison.get_msg()
            for comparison in self.field_comparisons
            if comparison.has_error()
        ]


class Comparison(NamedTuple):
    spreadsheet_title: str
    sheet_title: str
    name: str
    data_comparisons: List[ObjectComparison]

    def has_error(self) -> bool:
        return any([comparison.has_error() for comparison in self.data_comparisons])

    def get_gs_source(self):
        return f"Spreadsheet: {self.spreadsheet_title}, Sheet: {self.sheet_title}"

    def get_filename(self):
        return f"/tmp/{self.spreadsheet_title}|{self.sheet_title}|{self.name}.txt"

    def write(self) -> str:
        if self.has_error():
            with open(self.get_filename(), "w") as error_file:
                # titles
                error_file.write(f"{self.get_gs_source()}\n")
                error_file.write(f"{self.name}\n")

                # errors
                error_comparisons = [
                    comparison
                    for comparison in self.data_comparisons
                    if comparison.has_error()
                ]
                for comparison in error_comparisons:
                    error_file.write("\n")
                    error_file.write(f"{comparison.name}\n")
                    for error_msg in comparison.get_error_msgs():
                        error_file.write(f"{error_msg}\n")
            return self.get_filename()
        else:
            return None


@task
def _gather_db_institutions(
    spreadsheet_id: str,
    db_connection_url: str,
) -> List[Dict[str, str]]:
    db = MongoDBDatabase(db_connection_url)
    return db.find(
        collection=DatabaseCollection.institutions,
        filter={"spreadsheet_id": spreadsheet_id},
    )


@task
def _gather_db_variables(
    institution: Dict[str, Any],
    db_connection_url: str,
) -> Dict[str, Any]:
    db = MongoDBDatabase(db_connection_url)
    db_institution = institution.copy()
    db_variables = db.find(
        collection=DatabaseCollection.variables,
        filter={"institution": db_institution.get("_id")},
        sort=[["variable_index", ASCENDING]],
    )
    for db_variable in db_variables:
        if db_variable.get("type") == VariableType.composite:
            composite_variable_data = db.find(
                collection=db_variable.get("hyperlink"),
                filter={"variable": db_variable.get("_id")},
                sort=[("index", ASCENDING)],
            )
            db_variable.update(composite_variable_data=composite_variable_data)
    db_institution.update(variables=db_variables)
    return db_institution


@task
def _group_db_institutions(db_institutions: List[Dict[str, Any]]) -> Dict[str, Any]:
    db_institutions_group = {}
    for db_institution in db_institutions:
        country = db_institution.get("country")
        category = db_institution.get("category")
        name = db_institution.get("name")
        db_institutions_group.update({f"{country}{category}{name}": db_institution})
    return db_institutions_group


@task
def _gather_gs_institutions(
    formatted_sheet_data: FormattedSheetData,
) -> List[GsInstitution]:
    gs_format = formatted_sheet_data.meta_data.get("format")
    if gs_format == GoogleSheetsFormat.institution_and_composite_variable:
        country = formatted_sheet_data.meta_data.get("country")
        category = formatted_sheet_data.meta_data.get("category")
        name = formatted_sheet_data.meta_data.get("variable_heading")
        return [
            GsInstitution(
                spreadsheet_id=formatted_sheet_data.spreadsheet_id,
                spreadsheet_title=formatted_sheet_data.spreadsheet_title,
                sheet_id=formatted_sheet_data.sheet_id,
                sheet_title=formatted_sheet_data.sheet_title,
                data={
                    "country": country,
                    "category": category,
                    "name": name,
                    "variables": formatted_sheet_data.formatted_data,
                },
            )
        ]
    else:
        return [
            GsInstitution(
                spreadsheet_id=formatted_sheet_data.spreadsheet_id,
                spreadsheet_title=formatted_sheet_data.spreadsheet_title,
                sheet_id=formatted_sheet_data.sheet_id,
                sheet_title=formatted_sheet_data.sheet_title,
                data=institution,
            )
            for institution in formatted_sheet_data.formatted_data
        ]


@task
def _group_gs_institutions(
    gs_institutions: List[GsInstitution],
) -> Dict[str, Any]:
    return {
        gs_institution.get_key(): gs_institution for gs_institution in gs_institutions
    }


@task
def _compare_gs_institution(
    gs_institution: GsInstitution,
    db_institutions_group: Dict[str, Any],
) -> Comparison:
    variable_comparisons = []
    # get the db institution
    db_institution = db_institutions_group.get(gs_institution.get_key())

    # compare instituion existence
    institution_field_comparisons = [
        FieldComparison(
            "Institution exists in",
            (Datasource.database, True if db_institution else False),
            (Datasource.googlesheet, True),
        )
    ]
    if db_institution:
        # compare institution name
        institution_field_comparisons.append(
            FieldComparison(
                "Institution name",
                (Datasource.database, db_institution.get("name")),
                (Datasource.googlesheet, gs_institution.data.get("name")),
            )
        )
        # compare institution country
        institution_field_comparisons.append(
            FieldComparison(
                "Institution country",
                (Datasource.database, db_institution.get("country")),
                (Datasource.googlesheet, gs_institution.data.get("country")),
            )
        )
        # compare institution category
        institution_field_comparisons.append(
            FieldComparison(
                "Institution category",
                (Datasource.database, db_institution.get("category")),
                (Datasource.googlesheet, gs_institution.data.get("category")),
            )
        )

        if db_institution.get("variables")[0].get("type") != VariableType.aggregate:
            # compare number of variables
            institution_field_comparisons.append(
                FieldComparison(
                    "Number of variables",
                    (Datasource.database, len(db_institution.get("variables"))),
                    (Datasource.googlesheet, len(gs_institution.data.get("variables"))),
                )
            )
            for (db_variable, gs_variable) in enumerate(
                zip(
                    db_institution.get("variables"),
                    gs_institution.data.get("variables"),
                )
            ):
                # compare the required variable fields
                variable_field_comparisons = [
                    FieldComparison(
                        "Variable heading",
                        (Datasource.database, db_variable.get("heading")),
                        (Datasource.googlesheet, gs_variable.get("heading")),
                    ),
                    FieldComparison(
                        "Variable name",
                        (Datasource.database, db_variable.get("name")),
                        (Datasource.googlesheet, gs_variable.get("name")),
                    ),
                    FieldComparison(
                        "Variable type",
                        (Datasource.database, db_variable.get("type")),
                        (Datasource.googlesheet, gs_variable.get("type")),
                    ),
                    FieldComparison(
                        "Variable index",
                        (Datasource.database, db_variable.get("variable_index")),
                        (Datasource.googlesheet, gs_variable.get("variable_index")),
                    ),
                    FieldComparison(
                        "Sigla's answer",
                        (Datasource.database, db_variable.get("sigla_answer")),
                        (Datasource.googlesheet, gs_variable.get("sigla_answer")),
                    ),
                    FieldComparison(
                        "Original text",
                        (Datasource.database, db_variable.get("orig_text")),
                        (Datasource.googlesheet, gs_variable.get("orig_text")),
                    ),
                    FieldComparison(
                        "Source",
                        (Datasource.database, db_variable.get("source")),
                        (Datasource.googlesheet, gs_variable.get("source")),
                    ),
                ]

                if db_variable.get("type") == VariableType.composite:
                    # compare the hyperlink
                    variable_field_comparisons.append(
                        FieldComparison(
                            "Variable hyperlink",
                            (Datasource.database, db_variable.get("hyperlink")),
                            (Datasource.googlesheet, gs_variable.get("hyperlink")),
                        )
                    )
                    # compare if there is a link between variable and composite variable collection
                    variable_field_comparisons.append(
                        FieldComparison(
                            f"""{gs_variable.get("name")} exists in""",
                            (
                                Datasource.database,
                                True
                                if db_variable.get("composite_variable_data", [])
                                else False,
                            ),
                            (Datasource.googlesheet, True),
                        )
                    )

                # create a comparison for the variable and append to the list of variable comparisons
                variable_comparisons.append(
                    ObjectComparison(
                        gs_variable.get("name"), variable_field_comparisons
                    )
                )

        # create a institution comparison
        institution_comparison = ObjectComparison(
            "Institution", institution_field_comparisons
        )
        # create a general comparison to contain institution and variable comparisons
        return Comparison(
            spreadsheet_title=gs_institution.spreadsheet_title,
            sheet_title=gs_institution.sheet_title,
            name=gs_institution.get("name"),
            data_comparisons=[institution_comparison, *variable_comparisons],
        )


@task
def _compare_gs_composite_variable(
    formatted_sheet_data: FormattedSheetData,
    db_connection_url: str,
) -> List[Comparison]:
    institution_country = formatted_sheet_data.meta_data.get("country")
    institution_category = formatted_sheet_data.meta_data.get("category")
    institution_names = [
        name.strip() for name in formatted_sheet_data.meta_data.get("name").split(";")
    ]
    institution_names.sort()
    variable_heading = formatted_sheet_data.meta_data.get("variable_heading")
    variable_name = formatted_sheet_data.meta_data.get("variable_name")
    variable_hyperlink = formatted_sheet_data.meta_data.get("data_type")

    db = MongoDBDatabase(db_connection_url)
    comparisons = []
    for institution_name in institution_names:
        logic_field_comparisons = []
        row_comparisons = []
        db_institutions = db.find(
            collection=DatabaseCollection.institutions,
            filter={
                "country": institution_country,
                "category": institution_category,
                "name": institution_name,
            },
        )

        # compare matched db institutions
        num_matched_institutions_comparison = FieldComparison(
            "Number of matched institutions",
            (Datasource.database, len(db_institutions)),
            (Datasource.googlesheet, 1),
        )
        logic_field_comparisons.append(num_matched_institutions_comparison)

        if not num_matched_institutions_comparison.has_error():
            db_institution = db_institutions[0]
            # compare institution name
            institution_name_comparison = FieldComparison(
                "Institution name",
                (Datasource.database, db_institution.get("name")),
                (Datasource.googlesheet, institution_name),
            )
            logic_field_comparisons.append(institution_name_comparison)

            if not institution_name_comparison.has_error():
                # get the variable
                db_variables = db.find(
                    collection=DatabaseCollection.variables,
                    filter={
                        "institution": db_institution.get("_id"),
                        "heading": variable_heading,
                        "name": variable_name,
                        "type": VariableType.composite,
                        "hyperlink": variable_hyperlink,
                    },
                )

                # compare the number of matched variables
                num_matched_variables_comparison = FieldComparison(
                    "Number of matched variables",
                    (Datasource.database, len(db_variables)),
                    (Datasource.googlesheet, 1),
                )
                logic_field_comparisons.append(num_matched_variables_comparison)

                if not num_matched_variables_comparison.has_error():
                    db_variable = db_variables[0]

                    # get the rows
                    db_composite_variable_data = db.find(
                        collection=db_variable.get("hyperlink"),
                        filter={"variable": db_variable.get("_id")},
                        sort=[("index", ASCENDING)],
                    )

                    # compare the number of rows
                    num_rows_comparison = FieldComparison(
                        f"Number of {variable_hyperlink}",
                        (Datasource.database, len(db_composite_variable_data)),
                        (
                            Datasource.googlesheet,
                            len(formatted_sheet_data.formatted_data),
                        ),
                    )
                    logic_field_comparisons.append(num_rows_comparison)

                    for db_row, gs_row in enumerate(
                        zip(
                            db_composite_variable_data,
                            formatted_sheet_data.formatted_data,
                        )
                    ):
                        # compare each cell
                        cell_comparisons = [
                            FieldComparison(
                                gs_cell.get("name"),
                                (Datasource.database, db_cell.get("answer")),
                                (Datasource.googlesheet, gs_cell.get("answer")),
                            )
                            for db_cell, gs_cell in enumerate(
                                zip(
                                    db_row.get("sigla_answers"),
                                    gs_row.get("sigla_answers"),
                                )
                            )
                        ]

                        # compare row
                        row_comparisons.append(
                            ObjectComparison(
                                name=gs_row.get("index"),
                                field_comparisons=cell_comparisons,
                            )
                        )

        # create general comparison that  contains the logic comparison and row comparisons
        comparisons.append(
            Comparison(
                spreadsheet_title=formatted_sheet_data.spreadsheet_title,
                sheet_title=formatted_sheet_data.sheet_title,
                name=f"{variable_name} for {institution_name}",
                data_comparisons=[
                    ObjectComparison(
                        name="Composite variable checks",
                        field_comparisons=logic_field_comparisons,
                    ),
                    *row_comparisons,
                ],
            )
        )

    return comparisons


@task
def _write_comparison(
    comparison: Comparison,
) -> str:
    return comparison.write()


@task
def _write_extra_db_institutions(
    db_institutions: List[Dict[str, Any]], gs_institutions_group: Dict[str, Any]
) -> str:
    extra_db_institutions = []
    for db_institution in db_institutions:
        country = db_institution.get("country")
        category = db_institution.get("category")
        name = db_institution.get("name")
        if gs_institutions_group.get(f"{country}{category}{name}") is None:
            extra_db_institutions.append(db_institution)

    filename = "/tmp/extra-institutions.csv"
    with open(filename, "w") as error_file:
        fieldnames = [
            "_id",
            "spreadsheet_id",
            "sheet_id",
            "country",
            "category",
            "name",
        ]
        writer = csv.DictWriter(error_file, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for db_institution in extra_db_institutions:
            writer.writerow(
                {
                    "_id": db_institution.get("_id"),
                    "spreadsheet_id": db_institution.get("spreadsheet_id"),
                    "sheet_id": db_institution.get("sheet_id"),
                    "country": db_institution.get("country"),
                    "category": db_institution.get("category"),
                    "name": db_institution.get("name"),
                }
            )
    return filename


def run_qa_test(
    spreadsheet_ids: List[str],
    db_connection_url: str,
    google_api_credentials_path: str,
):
    """
    Run the the external link checker

    Parameters
    ----------
    master_spreadsheet_id: str
        The master spreadsheet id
    google_api_credentials_path: str
        The path to Google API credentials file needed to read Google Sheets.
    """

    cluster = LocalCluster()
    # Log the dashboard link
    log.info(f"Dashboard available at: {cluster.dashboard_link}")
    # Setup workflow
    with Flow("Run QA Test") as flow:
        # list of list of db institutions
        db_institutions_data = _gather_db_institutions.map(
            spreadsheet_ids, unmapped(db_connection_url)
        )
        # db institutions with their db variables and composite variable data
        db_institutions = _gather_db_variables.map(
            flatten(db_institutions_data), unmapped(db_connection_url)
        )
        # group db institutions
        db_institutions_group = _group_db_institutions(db_institutions)

        # extract list of list of sheet data
        spreadsheets_data = _extract.map(
            spreadsheet_ids, unmapped(google_api_credentials_path)
        )
        # transform to list of formatted sheet data
        formatted_spreadsheets_data = _transform.map(flatten(spreadsheets_data))
        # create institutional filter
        gs_institution_filter = FilterTask(
            filter_func=lambda x: x.meta_data.get("format")
            in [
                GoogleSheetsFormat.standard_institution,
                GoogleSheetsFormat.multiple_sigla_answer_variable,
                GoogleSheetsFormat.institution_and_composite_variable,
            ]
        )
        # filter to list of institutional formatted sheet data
        gs_institutions_data = gs_institution_filter(formatted_spreadsheets_data)
        # get list of list of gs institution
        gs_institutions = _gather_gs_institutions.map(gs_institutions_data)
        # create composite filter
        gs_composite_filter = FilterTask(
            filter_func=lambda x: x.meta_data.get("format")
            in [
                GoogleSheetsFormat.composite_variable,
                GoogleSheetsFormat.institution_and_composite_variable,
            ]
        )
        # filter to list of composite formatted sheet data
        gs_composites = gs_composite_filter(formatted_spreadsheets_data)

        # group gs institutions
        gs_institutions_group = _group_gs_institutions(flatten(gs_institutions))

        # compare gs institutions against db
        # get list of comparisons
        gs_institution_comparisons = _compare_gs_institution.map(
            flatten(gs_institutions), unmapped(db_institutions_group)
        )
        # compare gs composite variables against db institutions
        # get list of list of comparisons
        gs_composite_comparisons = _compare_gs_composite_variable.map(
            gs_composites, unmapped(db_institutions_group)
        )

        # write gs institution comparisons
        _write_comparison.map(gs_institution_comparisons)
        # write gs composite comparisons
        _write_comparison.map(flatten(gs_composite_comparisons))
        # write extra db institution
        _write_extra_db_institutions(db_institutions, gs_institutions_group)

    # Run the flow
    state = flow.run(executor=DaskExecutor(cluster.scheduler_address))
    # get write comparison tasks
    _write_comparison_tasks = flow.get_tasks(name="_write_comparison")
    # get the filenames result
    gs_filenames = [
        *state.result[_write_comparison_tasks[0]].result
        * state.result[_write_comparison_tasks[0]].result
    ]
    # filter to error filenames
    gs_error_filenames = [filename for filename in gs_filenames if filename]
    # get extra db institution filename
    extra_db_institutions_filename = state.result[
        flow.get_tasks(name="_write_extra_db_institutions")[0]
    ].result
    # write zip file
    with ZipFile("qa-test.zip", "w") as zip_file:
        for filename in [*gs_error_filenames, extra_db_institutions_filename]:
            zip_file.write(filename)


###############################################################################
# Args


class Args(argparse.Namespace):
    def __init__(self):
        self.__parse()

    def __parse(self):
        # Set up parser
        p = argparse.ArgumentParser(
            prog="run_qa_test",
            description="A script to run quality assurance test on the database",
        )
        # Arguments
        p.add_argument(
            "-v",
            "--version",
            action="version",
            version="%(prog)s " + get_module_version(),
        )
        p.add_argument(
            "-ssi",
            "--speadsheet-ids",
            action="store",
            dest="spreadsheet_ids",
            type=str,
            help="The list of spreadsheet ids, delimited by comma",
        )
        p.add_argument(
            "-dbe",
            "--db-env",
            action="store",
            dest="db_env",
            type=str,
            help="The environment of the database, staging or production",
        )
        p.add_argument(
            "-gacp",
            "--google_api_credentials_path",
            action="store",
            dest="google_api_credentials_path",
            type=str,
            help="The google api credentials path",
        )
        p.add_argument(
            "-sdbcu",
            "--staging_db_connection_url",
            action="store",
            dest="staging_db_connection_url",
            type=str,
            help="The Staging Database Connection URL",
        )
        p.add_argument(
            "-pdbcu",
            "--prod_db_connection_url",
            action="store",
            dest="prod_db_connection_url",
            type=str,
            help="The Production Database Connection URL",
        )
        p.add_argument(
            "--debug", action="store_true", dest="debug", help=argparse.SUPPRESS
        )
        # Parse
        p.parse_args(namespace=self)


###############################################################################


def main():
    try:
        args = Args()
        dbg = args.debug
        spreadsheet_ids = [
            spreadsheet_id.trim() for spreadsheet_id in args.spreadsheet_ids.split(",")
        ]
        if not spreadsheet_ids:
            raise Exception("No spreadsheet ids found.")
        if not args.db_env.strip() not in [env.value for env in Environment]:
            raise Exception(
                "Incorrect database enviroment specification. Use 'staging' or 'production'."
            )
        run_qa_test(
            spreadsheet_ids,
            args.staging_db_connection_url
            if args.db_env == Environment.staging
            else args.prod_db_connection_url,
            args.google_api_credentials_path,
        )
    except Exception as e:
        log.error("=============================================")
        if dbg:
            log.error("\n\n" + traceback.format_exc())
            log.error("=============================================")
        log.error("\n\n" + str(e) + "\n")
        log.error("=============================================")
        sys.exit(1)


###############################################################################
# Allow caller to directly run this module (usually in development scenarios)

if __name__ == "__main__":
    main()

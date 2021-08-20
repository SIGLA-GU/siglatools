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
from typing import Any, Dict, List, NamedTuple, Optional, Tuple
from zipfile import ZipFile

from distributed import LocalCluster
from prefect import Flow, flatten, task, unmapped
from prefect.executors import DaskExecutor
from pymongo import ASCENDING

from siglatools import get_module_version

from ..databases.constants import (
    CompositeVariableField,
    DatabaseCollection,
    Environment,
    InstitutionField,
    SiglaAnswerField,
    VariableField,
    VariableType,
)
from ..databases.mongodb_database import MongoDBDatabase
from ..institution_extracters.constants import GoogleSheetsFormat, MetaDataField
from ..institution_extracters.utils import FormattedSheetData
from ..pipelines.utils import (
    _create_filter_task,
    _extract,
    _get_spreadsheet_ids,
    _transform,
)

###############################################################################

logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)
log = logging.getLogger()

###############################################################################


class Datasource:
    googlesheet = "GoogleSheet"
    database = "Database"


class FieldComparisonType:
    meta = "meta"
    data = "data"


class GsInstitution(NamedTuple):
    """
    An institution from GoogleSheet.

    Attributes:
        spreadsheet_id: str
            The spreadsheet id.
        spreadsheet_title: str
            The spreadsheet title.
        sheet_id: str
            The sheet id.
        sheet_title: str
            The sheet title.
        data: Any
            The institution object and its variables.
    """

    spreadsheet_id: str
    spreadsheet_title: str
    sheet_id: str
    sheet_title: str
    data: Any

    def get_key(self) -> str:
        "Get the key of the institution."
        country = self.data.get(InstitutionField.country)
        category = self.data.get(InstitutionField.category)
        name = self.data.get(InstitutionField.name)
        return f"{country}{category}{name}"


class FieldComparison(NamedTuple):
    """
    A comparison of two values for a field.

    Attributes:
        field: str
            The field name.
        comparison_type: str
            The field comparison type.
        actual: Tuple[str, Any]
            A value and its source.
        expected: Tuple[str, Any]
            A value and its source.
    """

    field: str
    comparison_type: str
    actual: Tuple[str, Any]
    expected: Tuple[str, Any]

    def has_error(self) -> bool:
        "Does the two values mismatch?"
        return self.actual[1] != self.expected[1]

    def get_str_rep(self) -> str:
        "Get str of the two values."
        return f"{self.field}| {self.actual[0]}: {self.actual[1]}, {self.expected[0]}: {self.expected[1]}"

    def get_error_msg(self) -> str:
        "Get error msg."
        if self.comparison_type == FieldComparisonType.meta:
            return self.get_str_rep()
        else:
            return f"Incorrect value for {self.field}"


class ObjectComparison(NamedTuple):
    """
    A group of FieldComparison.

    Attributes:
        name: str
            The name of the group.
        field_comparisons: List[FieldComparison]
            The group of FieldComparison.
    """

    name: str
    field_comparisons: List[FieldComparison]

    def has_error(self) -> bool:
        "Does any of the group of FieldComparison have a mismatch?"
        return any([comparison.has_error() for comparison in self.field_comparisons])

    def get_error_msgs(self) -> List[str]:
        "Get a list of msg for the mistmatch FieldComparison."
        return [
            comparison.get_error_msg()
            for comparison in self.field_comparisons
            if comparison.has_error()
        ]


class Comparison(NamedTuple):
    """
    A group of ObjectComparison.

    Attributes:
        spreadsheet_title: str
            The spreadsheet source of the GoogleSheet data.
        sheet_title: str
            The sheet source of the GoogleSheet data.
        name: str
            The name of the group.
        data_comparisons: List[ObjectComparison]
            The group of ObjectComparison.

    """

    spreadsheet_title: str
    sheet_title: str
    name: str
    data_comparisons: List[ObjectComparison]

    def get_name(self) -> str:
        "Return the name of the comparison with / replaced with |."
        return self.name.replace("/", "|")

    def has_error(self) -> bool:
        "Does any of the group of ObjectCOmparison has a mismatch?"
        return any([comparison.has_error() for comparison in self.data_comparisons])

    def get_gs_source(self):
        "Get the GoogleSheet source."
        return f"Spreadsheet: {self.spreadsheet_title}, Sheet: {self.sheet_title}"

    def get_filename(self):
        "Get the file name."
        return f"tmp/{self.spreadsheet_title},{self.sheet_title},{self.get_name()}.txt"

    def write(self) -> str:
        """
        Write the mistmatch data_comparisons to a file.

        Returns
        -------
        filename: str
            The filename.
        """
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
) -> List[Dict[str, Any]]:
    """
    Gather institutions from the database.

    Parameters
    ----------
    spreadsheet_id: str
        The spreadsheet id source of the institutions.
    db_connection_url: str
        The DB's connection url str.

    Returns
    -------
    institutions: List[Dict[str, Any]]
        The list of institutions.

    """
    db = MongoDBDatabase(db_connection_url)
    institutions = db.find(
        collection=DatabaseCollection.institutions,
        filters={InstitutionField.spreadsheet_id: spreadsheet_id},
    )
    db.close_connection()
    return institutions


@task
def _gather_db_variables(
    institution: Dict[str, Any],
    db_connection_url: str,
) -> Dict[str, Any]:
    """
    Gather variables for a given institution from the database.

    Parameters
    ----------
    institution: Dict[str, Any]
        The institution.
    db_connection_url: str
        The DB's connection url str.

    Returns
    -------
    institution: Dict[str, Any]
        The instiution with its variables (and any composite variable data).
    """
    db = MongoDBDatabase(db_connection_url)
    db_institution = institution.copy()
    db_variables = db.find(
        collection=DatabaseCollection.variables,
        filters={VariableField.institution: db_institution.get(InstitutionField._id)},
        sort=[[VariableField.variable_index, ASCENDING]],
    )
    for db_variable in db_variables:
        if db_variable.get(VariableField.type) == VariableType.composite:
            variable_str = (
                CompositeVariableField.variables
                if db_variable.get(VariableField.hyperlink)
                == DatabaseCollection.body_of_law
                else CompositeVariableField.variable
            )
            composite_variable_data = db.find(
                collection=db_variable.get(VariableField.hyperlink),
                filters={f"{variable_str}": db_variable.get(VariableField._id)},
                sort=[(CompositeVariableField.index, ASCENDING)],
            )
            db_variable.update(composite_variable_data=composite_variable_data)
    db_institution.update(childs=db_variables)
    db.close_connection()
    return db_institution


@task
def _group_db_institutions(db_institutions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Group database institutions according to their unique key.

    Parameters
    ----------
    db_instituions: List[Dict[str, Any]]
        The list of institutions.

    Returns
    -------
    grouped_institutions: Dict[str, Any]
        A dictionary of institutions grouped by their unique key.
    """
    db_institutions_group = {}
    for db_institution in db_institutions:
        country = db_institution.get(InstitutionField.country)
        category = db_institution.get(InstitutionField.category)
        name = db_institution.get(InstitutionField.name)
        db_institutions_group.update({f"{country}{category}{name}": db_institution})
    return db_institutions_group


@task
def _gather_gs_institutions(
    formatted_sheet_data: FormattedSheetData,
) -> List[GsInstitution]:
    """
    Gather institutions from GoogleSheet.

    Parameters
    ----------
    formatted_sheet_data: FormattedSheetData,
        The list of sheet.

    Returns
    -------
    institutions: List[GsInstitution]
        The list of institutions.
    """
    gs_format = formatted_sheet_data.meta_data.get(MetaDataField.format)
    if gs_format == GoogleSheetsFormat.institution_and_composite_variable:
        country = formatted_sheet_data.meta_data.get(InstitutionField.country)
        category = formatted_sheet_data.meta_data.get(InstitutionField.category)
        name = formatted_sheet_data.meta_data.get(MetaDataField.variable_heading)
        return [
            GsInstitution(
                spreadsheet_id=formatted_sheet_data.spreadsheet_id,
                spreadsheet_title=formatted_sheet_data.spreadsheet_title,
                sheet_id=formatted_sheet_data.sheet_id,
                sheet_title=formatted_sheet_data.sheet_title,
                data={
                    InstitutionField.country: country,
                    InstitutionField.category: category,
                    InstitutionField.name: name,
                    "childs": formatted_sheet_data.formatted_data,
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
    """
    Group GoogleSheet institutions according to their unique key.

    Parameters
    ----------
    gs_instituions: List[GsInstitution]
        The list of institutions.

    Returns
    -------
    grouped_institutions: Dict[str, Any]
        A dictionary of institutions grouped by their unique key.
    """
    return {
        gs_institution.get_key(): gs_institution for gs_institution in gs_institutions
    }


@task
def _compare_gs_institution(
    gs_institution: GsInstitution,
    db_institutions_group: Dict[str, Any],
) -> Comparison:
    """
    Compare GoogleSheet institution against its database institution.

    Parameters
    ----------
    gs_institution: GsInstitution
        The GoogleSheet Institution.
    db_institutions_group: Dict[str, Any]
        Db institutions grouped by their unique key.

    Returns
    -------
    comparison: Comparison
        The comparison result of the two institutions.
    """
    variable_comparisons = []
    # get the db institution
    db_institution = db_institutions_group.get(gs_institution.get_key())

    # compare instituion existence
    institution_field_comparisons = [
        FieldComparison(
            "Institution exists in",
            FieldComparisonType.meta,
            (Datasource.database, True if db_institution else False),
            (Datasource.googlesheet, True),
        )
    ]
    if db_institution:
        # compare institution name
        institution_field_comparisons.append(
            FieldComparison(
                "Institution name",
                FieldComparisonType.meta,
                (Datasource.database, db_institution.get(InstitutionField.name)),
                (
                    Datasource.googlesheet,
                    gs_institution.data.get(InstitutionField.name),
                ),
            )
        )
        # compare institution country
        institution_field_comparisons.append(
            FieldComparison(
                "Institution country",
                FieldComparisonType.meta,
                (Datasource.database, db_institution.get(InstitutionField.country)),
                (
                    Datasource.googlesheet,
                    gs_institution.data.get(InstitutionField.country),
                ),
            )
        )
        # compare institution category
        institution_field_comparisons.append(
            FieldComparison(
                "Institution category",
                FieldComparisonType.meta,
                (Datasource.database, db_institution.get(InstitutionField.category)),
                (
                    Datasource.googlesheet,
                    gs_institution.data.get(InstitutionField.category),
                ),
            )
        )

        # compare institution sub category
        institution_field_comparisons.append(
            FieldComparison(
                "Institution sub category",
                FieldComparisonType.meta,
                (
                    Datasource.database,
                    db_institution.get(InstitutionField.sub_category, []),
                ),
                (
                    Datasource.googlesheet,
                    gs_institution.data.get(InstitutionField.sub_category, []),
                ),
            )
        )

        if (
            db_institution.get("childs")[0].get(VariableField.type)
            != VariableType.aggregate
        ):
            # compare number of variables
            institution_field_comparisons.append(
                FieldComparison(
                    "Total number of variables",
                    FieldComparisonType.meta,
                    (Datasource.database, len(db_institution.get("childs"))),
                    (Datasource.googlesheet, len(gs_institution.data.get("childs"))),
                )
            )
            for db_variable, gs_variable in zip(
                db_institution.get("childs"),
                gs_institution.data.get("childs"),
            ):
                # compare the required variable fields
                variable_field_comparisons = [
                    FieldComparison(
                        "Variable heading",
                        FieldComparisonType.meta,
                        (Datasource.database, db_variable.get(VariableField.heading)),
                        (
                            Datasource.googlesheet,
                            gs_variable.get(VariableField.heading),
                        ),
                    ),
                    FieldComparison(
                        "Variable name",
                        FieldComparisonType.meta,
                        (Datasource.database, db_variable.get(VariableField.name)),
                        (Datasource.googlesheet, gs_variable.get(VariableField.name)),
                    ),
                    FieldComparison(
                        "Variable index",
                        FieldComparisonType.meta,
                        (
                            Datasource.database,
                            db_variable.get(VariableField.variable_index),
                        ),
                        (
                            Datasource.googlesheet,
                            gs_variable.get(VariableField.variable_index),
                        ),
                    ),
                    FieldComparison(
                        "Sigla's answer",
                        FieldComparisonType.data,
                        (
                            Datasource.database,
                            db_variable.get(VariableField.sigla_answer),
                        ),
                        (
                            Datasource.googlesheet,
                            gs_variable.get(VariableField.sigla_answer),
                        ),
                    ),
                    FieldComparison(
                        "Original text",
                        FieldComparisonType.data,
                        (Datasource.database, db_variable.get(VariableField.orig_text)),
                        (
                            Datasource.googlesheet,
                            gs_variable.get(VariableField.orig_text),
                        ),
                    ),
                    FieldComparison(
                        "Source",
                        FieldComparisonType.data,
                        (Datasource.database, db_variable.get(VariableField.source)),
                        (Datasource.googlesheet, gs_variable.get(VariableField.source)),
                    ),
                ]

                if db_variable.get(VariableField.type) == VariableType.composite:
                    # compare if there is a link between variable and composite variable collection
                    variable_field_comparisons.append(
                        FieldComparison(
                            f"""{gs_variable.get(VariableField.name)} exists in""",
                            FieldComparisonType.meta,
                            (
                                Datasource.database,
                                True
                                if db_variable.get("composite_variable_data", [])
                                else False,
                            ),
                            (Datasource.googlesheet, True),
                        )
                    )
                else:
                    # compare variable type
                    variable_field_comparisons.append(
                        FieldComparison(
                            "Variable type",
                            FieldComparisonType.meta,
                            (Datasource.database, db_variable.get(VariableField.type)),
                            (
                                Datasource.googlesheet,
                                gs_variable.get(VariableField.type),
                            ),
                        )
                    )

                # create a comparison for the variable and append to the list of variable comparisons
                variable_comparisons.append(
                    ObjectComparison(
                        f"""Variable: {gs_variable.get(VariableField.name)}""",
                        variable_field_comparisons,
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
        name=gs_institution.data.get(InstitutionField.name),
        data_comparisons=[institution_comparison, *variable_comparisons],
    )


@task
def _compare_gs_composite_variable(
    formatted_sheet_data: FormattedSheetData,
    db_connection_url: str,
) -> List[Comparison]:
    """
    Compare GoogleSheet composite variable data against its Database counterpart.

    Parameters
    ----------
    formatted_sheet_data: FormattedSheetData
        The sheet data thats contains the composite variable data.
    db_connection_url: str
        The DB's connection url str.

    Returns
    -------
    comparisons: List[Comparison]
        A list of comparisons for the two composite variable data. One for each of its parent institution.
    """

    institution_country = formatted_sheet_data.meta_data.get(InstitutionField.country)
    institution_category = formatted_sheet_data.meta_data.get(InstitutionField.category)
    institution_names = [
        name.strip()
        for name in formatted_sheet_data.meta_data.get(InstitutionField.name).split(";")
    ]
    institution_names.sort()
    variable_heading = formatted_sheet_data.meta_data.get(
        MetaDataField.variable_heading
    )
    variable_name = formatted_sheet_data.meta_data.get(MetaDataField.variable_name)
    variable_hyperlink = formatted_sheet_data.meta_data.get(MetaDataField.data_type)

    db = MongoDBDatabase(db_connection_url)
    comparisons = []
    for institution_name in institution_names:
        logic_field_comparisons = []
        row_comparisons = []
        db_institutions = db.find(
            collection=DatabaseCollection.institutions,
            filters={
                InstitutionField.country: institution_country,
                InstitutionField.category: institution_category,
                InstitutionField.name: institution_name,
            },
        )

        # compare matched db institutions
        num_matched_institutions_comparison = FieldComparison(
            "Number of found institutions",
            FieldComparisonType.meta,
            (Datasource.database, len(db_institutions)),
            (Datasource.googlesheet, 1),
        )
        logic_field_comparisons.append(num_matched_institutions_comparison)

        if not num_matched_institutions_comparison.has_error():
            db_institution = db_institutions[0]
            # compare institution name
            institution_name_comparison = FieldComparison(
                "Institution name",
                FieldComparisonType.meta,
                (Datasource.database, db_institution.get(InstitutionField.name)),
                (Datasource.googlesheet, institution_name),
            )
            logic_field_comparisons.append(institution_name_comparison)

            if not institution_name_comparison.has_error():
                # get the variable
                db_variables = db.find(
                    collection=DatabaseCollection.variables,
                    filters={
                        VariableField.institution: db_institution.get(
                            InstitutionField._id
                        ),
                        VariableField.heading: variable_heading,
                        VariableField.name: variable_name,
                        VariableField.type: VariableType.composite,
                        VariableField.hyperlink: variable_hyperlink,
                    },
                )

                # compare the number of matched variables
                num_matched_variables_comparison = FieldComparison(
                    "Number of found variables",
                    FieldComparisonType.meta,
                    (Datasource.database, len(db_variables)),
                    (Datasource.googlesheet, 1),
                )
                logic_field_comparisons.append(num_matched_variables_comparison)

                if not num_matched_variables_comparison.has_error():
                    db_variable = db_variables[0]

                    # get the rows
                    variable_str = (
                        CompositeVariableField.variables
                        if db_variable.get(VariableField.hyperlink)
                        == DatabaseCollection.body_of_law
                        else CompositeVariableField.variable
                    )
                    db_composite_variable_data = db.find(
                        collection=db_variable.get(VariableField.hyperlink),
                        filters={f"{variable_str}": db_variable.get(VariableField._id)},
                        sort=[(CompositeVariableField.index, ASCENDING)],
                    )

                    # compare the number of rows
                    num_rows_comparison = FieldComparison(
                        f"Total number of {variable_hyperlink}",
                        FieldComparisonType.meta,
                        (Datasource.database, len(db_composite_variable_data)),
                        (
                            Datasource.googlesheet,
                            len(formatted_sheet_data.formatted_data),
                        ),
                    )
                    logic_field_comparisons.append(num_rows_comparison)

                    for db_row, gs_row in zip(
                        db_composite_variable_data,
                        formatted_sheet_data.formatted_data,
                    ):
                        # compare each cell
                        cell_comparisons = [
                            FieldComparison(
                                gs_cell.get(SiglaAnswerField.name),
                                FieldComparisonType.data,
                                (
                                    Datasource.database,
                                    db_cell.get(SiglaAnswerField.answer),
                                ),
                                (
                                    Datasource.googlesheet,
                                    gs_cell.get(SiglaAnswerField.answer),
                                ),
                            )
                            for db_cell, gs_cell in zip(
                                db_row.get(CompositeVariableField.sigla_answers),
                                gs_row.get(CompositeVariableField.sigla_answers),
                            )
                        ]

                        # compare row
                        row_comparisons.append(
                            ObjectComparison(
                                name=gs_row.get(CompositeVariableField.index),
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
    db.close_connection()
    return comparisons


@task
def _write_comparison(
    comparison: Comparison,
) -> Comparison:
    "Write a comparison to a file."
    comparison.write()
    return comparison


@task
def _write_extra_db_institutions(
    db_institutions: List[Dict[str, Any]], gs_institutions_group: Dict[str, Any]
) -> str:
    """
    Write extra database institutions doesn't have an institution in the GoogleSheet to a file.

    Parameters
    ----------
    db_institutions: List[Dict[str, Any]]
        The list of database institutions.
    gs_institutions_group: Dict[str, Any]
        GoogleSheet institutions grouped by their unique key.

    Returns
    -------
    filename: str
        The filename.

    """

    extra_db_institutions = []
    for db_institution in db_institutions:
        country = db_institution.get(InstitutionField.country)
        category = db_institution.get(InstitutionField.category)
        name = db_institution.get(InstitutionField.name)
        if gs_institutions_group.get(f"{country}{category}{name}") is None:
            extra_db_institutions.append(db_institution)

    filename = "tmp/extra-institutions.csv"
    if extra_db_institutions:
        with open(filename, "w") as error_file:
            fieldnames = [
                InstitutionField._id,
                InstitutionField.spreadsheet_id,
                InstitutionField.sheet_id,
                InstitutionField.country,
                InstitutionField.category,
                InstitutionField.name,
            ]
            writer = csv.DictWriter(error_file, fieldnames=fieldnames, delimiter="\t")
            writer.writeheader()
            for db_institution in extra_db_institutions:
                writer.writerow(
                    {
                        InstitutionField._id: db_institution.get(InstitutionField._id),
                        InstitutionField.spreadsheet_id: db_institution.get(
                            InstitutionField.spreadsheet_id
                        ),
                        InstitutionField.sheet_id: db_institution.get(
                            InstitutionField.sheet_id
                        ),
                        InstitutionField.country: db_institution.get(
                            InstitutionField.country
                        ),
                        InstitutionField.category: db_institution.get(
                            InstitutionField.category
                        ),
                        InstitutionField.name: db_institution.get(
                            InstitutionField.name
                        ),
                    }
                )
        return filename
    else:
        return None


def run_qa_test(
    master_spreadsheet_id: str,
    db_connection_url: str,
    google_api_credentials_path: str,
    spreadsheet_ids_str: Optional[str] = None,
):
    """
    Run QA test

    Parameters
    ----------
    master_spreadsheet_id: str
        The master spreadsheet id.
    db_connection_url: str
        The DB's connection url str.
    google_api_credentials_path: str
        The path to Google API credentials file needed to read Google Sheets.
    spreadsheet_ids_str: Optional[str] = None
        The list of spreadsheet ids.
    """

    cluster = LocalCluster()
    # Log the dashboard link
    log.info(f"Dashboard available at: {cluster.dashboard_link}")
    # Setup workflow
    with Flow("Run QA Test") as flow:
        # get a list of spreadsheet ids
        spreadsheet_ids = _get_spreadsheet_ids(
            master_spreadsheet_id, google_api_credentials_path, spreadsheet_ids_str
        )
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
        gs_institution_filter = _create_filter_task(
            [
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
        gs_composite_filter = _create_filter_task(
            [
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
            gs_composites, unmapped(db_connection_url)
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
    # get the comparisons
    comparisons = [
        *state.result[_write_comparison_tasks[0]].result,
        *state.result[_write_comparison_tasks[1]].result,
    ]
    # filter to error comparisons
    gs_error_comparisons = [
        comparison for comparison in comparisons if comparison.has_error()
    ]
    # get extra db institution filename
    extra_db_institutions_filename = state.result[
        flow.get_tasks(name="_write_extra_db_institutions")[0]
    ].result
    # write zip file
    with ZipFile("qa-test.zip", "w") as zip_file:
        for comp in gs_error_comparisons:
            zip_file.write(
                comp.get_filename(),
                f"{comp.spreadsheet_title}/{comp.sheet_title},{comp.name}",
            )
        if extra_db_institutions_filename:
            zip_file.write(extra_db_institutions_filename, "extra-institutions.csv")


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
            "-msi",
            "--master_spreadsheet_id",
            action="store",
            dest="master_spreadsheet_id",
            type=str,
            help="The master spreadsheet id",
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
        if args.db_env.strip() not in [Environment.staging, Environment.production]:
            raise Exception(
                "Incorrect database enviroment specification. Use 'staging' or 'production'."
            )
        run_qa_test(
            args.master_spreadsheet_id,
            args.staging_db_connection_url
            if args.db_env == Environment.staging
            else args.prod_db_connection_url,
            args.google_api_credentials_path,
            args.spreadsheet_ids,
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

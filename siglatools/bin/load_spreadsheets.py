#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script will get deployed in the bin directory of the
users' virtualenv when the parent module is installed using pip.
"""

import argparse
import logging
import sys
import traceback
from typing import Any, Dict, List

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
    VariableField,
    VariableType,
)
from ..databases.mongodb_database import MongoDBDatabase
from ..institution_extracters.constants import GoogleSheetsFormat as gs_format
from ..pipelines.utils import (
    _create_filter_task,
    _extract,
    _load_composites_data,
    _load_institutions_data,
    _log_spreadsheets,
    _transform,
)

###############################################################################

logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)
log = logging.getLogger()

######################################################


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
def _delete_db_institutions(
    db_institutions: List[Dict[str, Any]],
    db_connection_url: str,
):
    """
    Delete institutions and their variables from the database.

    Parameters
    ----------
    db_institutions: List[Dict[str, Any]]
        The list of institutions and their variables.
    db_connection_url: str
        The DB's connection url str.
    """
    institution_ids = []
    variable_ids = []
    composite_ids = {
        DatabaseCollection.rights: set(),
        DatabaseCollection.amendments: set(),
        DatabaseCollection.body_of_law: set(),
    }
    for db_institution in db_institutions:
        institution_ids.append(db_institution.get(InstitutionField._id))
        for db_variable in db_institution.get("childs"):
            variable_ids.append(db_variable.get(VariableField._id))
            if db_variable.get(VariableField.type) == VariableType.composite:
                variable_hyperlink = db_variable.get(VariableField.hyperlink)
                for row in db_variable.get("composite_variable_data"):
                    composite_ids.get(variable_hyperlink).add(
                        row.get(VariableField._id)
                    )

    db = MongoDBDatabase(db_connection_url)
    db.delete_many(DatabaseCollection.institutions, institution_ids)
    db.delete_many(DatabaseCollection.variables, variable_ids)
    for collection, ids in composite_ids.items():
        db.delete_many(collection, list(ids))
    db.close_connection()


def load_spreadsheets(
    spreadsheet_ids: List[str],
    db_connection_url: str,
    google_api_credentials_path: str,
):
    """
    Load spreadsheets to the database.

    Parameters
    ----------
    spreadsheet_ids: List[str]
        The list of spreadsheet ids.
    db_connection_url: str
        The DB's connection url str.
    google_api_credentials_path: str
        The path to Google API credentials file needed to read Google Sheets.
    """

    cluster = LocalCluster()
    # Log the dashboard link
    log.info(f"Dashboard available at: {cluster.dashboard_link}")
    # Setup workflow
    with Flow("Load spreadsheets") as flow:
        # list of list of db institutions
        db_institutions_data = _gather_db_institutions.map(
            spreadsheet_ids, unmapped(db_connection_url)
        )
        # db institutions with their db variables and composite variable data
        db_institutions = _gather_db_variables.map(
            flatten(db_institutions_data), unmapped(db_connection_url)
        )

        # use db_institutions to remove data
        delete_db_institutions_task = _delete_db_institutions(
            db_institutions, db_connection_url
        )

        # extract list of list of sheet data
        spreadsheets_data = _extract.map(
            spreadsheet_ids,
            unmapped(google_api_credentials_path),
            upstream_tasks=[unmapped(delete_db_institutions_task)],
        )
        # transform to list of formatted sheet data
        formatted_spreadsheets_data = _transform.map(flatten(spreadsheets_data))
        # create institutonal filter
        gs_institution_filter = _create_filter_task(
            [
                gs_format.standard_institution,
                gs_format.multiple_sigla_answer_variable,
            ]
        )
        # filter to list of institutional formatted sheet data
        gs_institutions_data = gs_institution_filter(formatted_spreadsheets_data)
        # Create composite filter
        gs_composite_filter = _create_filter_task(
            [
                gs_format.composite_variable,
                gs_format.institution_and_composite_variable,
            ]
        )
        # filter to list of composite formatted sheet data
        gs_composites_data = gs_composite_filter(formatted_spreadsheets_data)

        # load instutional data
        load_institutions_data_task = _load_institutions_data.map(
            gs_institutions_data, unmapped(db_connection_url)
        )
        # load composite data
        load_composites_data_task = _load_composites_data.map(
            gs_composites_data,
            unmapped(db_connection_url),
            upstream_tasks=[unmapped(load_institutions_data_task)],
        )
        # log spreadsheets that were loaded
        _log_spreadsheets(spreadsheets_data, upstream_tasks=[load_composites_data_task])

    # Run the flow
    flow.run(executor=DaskExecutor(cluster.scheduler_address))


###############################################################################
# Args


class Args(argparse.Namespace):
    def __init__(self):
        self.__parse()

    def __parse(self):
        # Set up parser
        p = argparse.ArgumentParser(
            prog="load_spreadsheets",
            description="A script to load spreadsheets to the database.",
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
            spreadsheet_id.strip() for spreadsheet_id in args.spreadsheet_ids.split(",")
        ]
        if not spreadsheet_ids:
            raise Exception("No spreadsheet ids found.")
        if args.db_env.strip() not in [Environment.staging, Environment.production]:
            raise Exception(
                "Incorrect database enviroment specification. Use 'staging' or 'production'."
            )
        db_connection_url = (
            args.staging_db_connection_url
            if args.db_env == Environment.staging
            else args.prod_db_connection_url
        )
        load_spreadsheets(
            spreadsheet_ids,
            db_connection_url,
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

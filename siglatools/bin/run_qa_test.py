#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script will get deployed in the bin directory of the
users' virtualenv when the parent module is installed using pip.
"""

import argparse
import csv
import logging
import re
import sys
import traceback
from typing import List, NamedTuple, Optional, Dict, Any

import requests
from distributed import LocalCluster
from prefect import Flow, flatten, task, unmapped
from prefect.engine.executors import DaskExecutor
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from prefect.tasks.control_flow import FilterTask
from pymongo import ASCENDING

from siglatools import get_module_version

from ..institution_extracters.google_sheets_institution_extracter import (
    GoogleSheetsInstitutionExtracter,
)
from ..institution_extracters.utils import SheetData, FormattedSheetData, convert_rowcol_to_A1_name
from .run_sigla_pipeline import _extract
from ..databases.constants import Environment, VariableType, DatabaseCollection
from ..databases.mongodb_database import MongoDBDatabase
from .run_sigla_pipeline import _extract, _transform
from ..institution_extracters.constants import GoogleSheetsFormat

###############################################################################

logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)
log = logging.getLogger()

###############################################################################

@task
def _gather_institutions(
    spreadsheet_id: str,
    db_connection_url: str,
) -> List[Dict[str, str]]:
    db = MongoDBDatabase(db_connection_url)
    return db.find(
        collection=DatabaseCollection.institutions,
        filter={"spreadsheet_id": spreadsheet_id},
    )


@task
def _gather_variables(
    institution: Dict[str, Any],
    db_connection_url: str,
) -> Dict[str, Any]:
    db = MongoDBDatabase(db_connection_url)
    variables = db.find(
        collection=DatabaseCollection.variables,
        filter={"institution": institution.get("_id")},
        sort=[["index", ASCENDING]]
    )
    for variable in variables:
        if variable.get("type") == VariableType.composite:
            composite_variable_data = db.find(
                collection=variable.get("hyperlink"),
                filter={"variable": variable.get("_id")},
                sort=[("index", ASCENDING)]
            )
            variable.update(composite_variable_data=composite_variable_data)  
    institution.update(variables=variables)
    return institution

@task
def _get_composite_variable(
    spreadsheet_id: str,
    sheet_id: str,
    google_api_credentials_path: str,
) -> FormattedSheetData:
    extracter = GoogleSheetsInstitutionExtracter(google_api_credentials_path)
    spreadsheets_data = extracter.get_spreadsheet_data(spreadsheet_id, [sheet_id])
    return GoogleSheetsInstitutionExtracter.process_sheet_data(spreadsheets_data[0])


@task
def _extract_institutions(
    formatted_sheets_data: List[FormattedSheetData],
    google_api_credentials_path: str,
) -> Dict[str, Any]:
    institutions = 
    for formatted_sheet_data in formatted_sheets_data:
        if formatted_sheet_data.meta_data.get("format") == GoogleSheetsFormat.standard_institution:
            for institution in formatted_sheet_data.data:
                institutions.update({
                    "spreadsheet_id": formatted_sheet_data.meta_data.get("spreadsheet_id")
                })


def run_qa_test(
    spreadsheet_ids: List[str],
    db_connection_url: str,
    google_api_credentials_path: str,
):
    """TODO
    Run the the external link checker

    Parameters
    ----------
    master_spreadsheet_id: str
        The master spreadsheet id
    google_api_credentials_path: str
        The path to Google API credentials file needed to read Google Sheets.
    """

    # task: get all institutions from db for given spreadsheet_id (don't use api because spreadsheet_id is not apart of api)
    # task: for each institution get variables from db (because don't want to wait using staging api), get cv data as well if needed, need to decide on returned
    # task: for each spreadsheet_id get the data using gacp, get list of formattedsheetdata
        # _extract and _transform
    # what if cv data is in spreadsheet_id, qa or no?

    cluster = LocalCluster()
    # Log the dashboard link
    log.info(f"Dashboard available at: {cluster.dashboard_link}")
    # Setup workflow
    with Flow("Run QA Test") as flow:
        # list of list of institutions
        institutions_data = _gather_institutions.map(spreadsheet_ids, unmapped(db_connection_url))
        # institutions with their variables
        institutions = _gather_variables.map(flatten(institutions_data), unmapped(db_connection_url))
        spreadsheets_data = _extract.map(spreadsheet_ids, unmapped(google_api_credentials_path))
        formatted_spreadsheets_data = _transform.map(flatten(spreadsheets_data)
        # associate each cv with its data


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
            help="The environment of the database, staging or production"
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
        spreadsheet_ids = [spreadsheet_id.trim() for spreadsheet_id in args.spreadsheet_ids.split(",")]
        if not spreadsheet_ids:
            raise Exception("No spreadsheet ids found.")
        if not args.db_env.strip() not in [env.value for env in Environment]:
            raise Exception("Incorrect database enviroment specification. Use 'staging' or 'production'.")
        run_qa_test(
            spreadsheet_ids,
            args.staging_db_connection_url if args.db_env == Environment.staging else args.prod_db_connection_url,
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

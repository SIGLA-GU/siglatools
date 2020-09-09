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
from typing import List

from distributed import LocalCluster
from prefect import Flow, task, unmapped
from prefect.engine.executors import DaskExecutor

from siglatools import get_module_version

from ..databases import MongoDBDatabase
from ..institution_extracters.constants import GoogleSheetsFormat as gs_format
from ..institution_extracters.google_sheets_institution_extracter import (
    GoogleSheetsInstitutionExtracter,
)
from ..institution_extracters.utils import FormattedSheetData, SheetData

###############################################################################

logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)
log = logging.getLogger()

###############################################################################

# Tasks


@task
def _extract(spreadsheet_id: str, google_api_credentials_path: str) -> List[SheetData]:
    """
    Prefect Task to extract data from a spreadsheet.

    Parameters
    ----------
    spreadsheet_id: str
        The spreadsheet_id.
    google_api_credentials_path: str
        The path to Google API credentials file needed to read Google Sheets.

    Returns
    -------
    spreadsheet_data: List[SheetData]
        The list of SheetData, one for each sheet in the spreadsheet. Please see the SheetData class
        to view its attributes.
    """
    # Get the spreadsheet data.
    extracter = GoogleSheetsInstitutionExtracter(google_api_credentials_path)
    return extracter.get_spreadsheet_data(spreadsheet_id)


@task
def _flatten_list(outer_list: List[List]) -> List:
    """
    Prefect Task to flatten a 2D list.

    Parameters
    ----------
    outer_list: List[List]
        The list of list.

    Returns
    -------
    flattened_list: List
        The flattened list.
    """
    # Flatten a list of list.
    return [element for inner_list in outer_list for element in inner_list]


@task
def _transform(sheet_data: SheetData) -> FormattedSheetData:
    """
    Prefect Task to transform the sheet data into formatted sheet data, in order to load it into the DB.

    Parameters
    ----------
    sheet_data: SheetData
        The sheet's data.

    Returns
    -------
    formatted_sheet_data: FormattedSheetData
        The sheet's formatted data, ready to be consumed by DB.
    """
    return GoogleSheetsInstitutionExtracter.process_sheet_data(sheet_data)


def _filter_formatted_sheet_data(
    formatted_sheets_data: List[FormattedSheetData], google_sheets_formats: List[str]
) -> List[List[FormattedSheetData]]:
    return [
        formatted_sheet_data
        for formatted_sheet_data in formatted_sheets_data
        if formatted_sheet_data.meta_data.get("format") in google_sheets_formats
    ]


@task
def _load(formatted_sheet_data: FormattedSheetData, db_connection_url: str):
    """
    Prefect Task to load the formatted sheet data into the DB.

    Parameters
    ----------
    formatted_sheet_data: FormattedSheetData
        The sheet's formatted data.
    """
    database = MongoDBDatabase(db_connection_url)
    database.load(formatted_sheet_data)
    database.close_connection()


def _clean_up(db_connection_url: str):
    """
    Prefect Task to delete all documents from db.

    Parameters
    ----------
    db_connection_url: str
        The DB's connection url str.
    """
    database = MongoDBDatabase(db_connection_url)
    database.clean_up()
    database.close_connection()


def run_sigla_pipeline(
    master_spreadsheet_id: str, google_api_credentials_path: str, db_connection_url: str
):
    """
    Run the SIGLA ETL pipeline

    Parameters
    ----------
    google_api_credentials_path: str
        The path to Google API credentials file needed to read Google Sheets.
    db_connection_url: str
        The DB's connection url str.
    """
    # Delete all documents from db
    _clean_up(db_connection_url)
    # Create a connection to the google sheets reader
    google_sheets_institution_extracter = GoogleSheetsInstitutionExtracter(
        google_api_credentials_path
    )
    # Get the list of spreadsheets ids from the master spreadsheet
    spreadsheets_id = google_sheets_institution_extracter.get_spreadsheets_id(
        master_spreadsheet_id
    )
    log.info("Finished pipeline set up, start running pipeline")
    log.info("=" * 80)
    # Spawn local dask cluster
    cluster = LocalCluster()
    # Log the dashboard link
    log.info(f"Dashboard available at: {cluster.dashboard_link}")
    # Setup workflow
    with Flow("Extract and transform") as flow:
        # Extract sheets data.
        # Get back list of list of SheetData
        spreadsheets_data = _extract.map(
            spreadsheets_id, unmapped(google_api_credentials_path),
        )

        # Flatten the list of list of SheetData
        flattened_spreadsheets_data = _flatten_list(spreadsheets_data)

        # Transform list of SheetData into FormattedSheetData
        _transform.map(flattened_spreadsheets_data)

    # Run the extract and transform flow
    state = flow.run(executor=DaskExecutor(cluster.scheduler_address))
    formatted_sheets_data = state.result[flow.get_tasks(name="_transform")[0]].result
    log.info("=" * 80)
    # Partion into institutions and non-institutions sheets
    institutions = _filter_formatted_sheet_data(
        formatted_sheets_data,
        [
            gs_format.standard_institution,
            # gs_format.institution_by_rows,
            gs_format.multiple_sigla_answer_variable,
        ],
    )
    non_institutions = _filter_formatted_sheet_data(
        formatted_sheets_data,
        [gs_format.institution_and_composite_variable, gs_format.composite_variable],
    )
    # Run load institutions flow
    with Flow("Load institutions") as load_institutions_flow:
        _load.map(institutions, unmapped(db_connection_url))

    load_institutions_flow.run(executor=DaskExecutor(cluster.scheduler_address))
    log.info("=" * 80)
    # Run load non institutions flow
    with Flow("Load non institutions") as load_non_institutions_flow:
        _load.map(non_institutions, unmapped(db_connection_url))
    load_non_institutions_flow.run(executor=DaskExecutor(cluster.scheduler_address))


###############################################################################
# Args


class Args(argparse.Namespace):
    def __init__(self):
        self.__parse()

    def __parse(self):
        # Set up parser
        p = argparse.ArgumentParser(
            prog="run_sigla_pipeline", description="A script to run sigla data pipeline"
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
            "-gacp",
            "--google_api_credentials_path",
            action="store",
            dest="google_api_credentials_path",
            type=str,
            help="The google api credentials path",
        )
        p.add_argument(
            "-dbcu",
            "--db_connection_url",
            action="store",
            dest="db_connection_url",
            type=str,
            help="The Database Connection URL",
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
        run_sigla_pipeline(
            args.master_spreadsheet_id,
            args.google_api_credentials_path,
            args.db_connection_url,
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

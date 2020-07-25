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
from ..institution_extracters import exceptions
from ..institution_extracters.google_sheets_institution_extracter import (
    FormattedSheetData,
    GoogleSheetsInstitutionExtracter,
    SheetData,
)

###############################################################################

log = logging.getLogger()
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)

###############################################################################

# Tasks


@task
def _extract(spreadsheet_id: str, google_api_credentials_path: str) -> List[SheetData]:
    # Get the spreadsheet data.
    extracter = GoogleSheetsInstitutionExtracter(google_api_credentials_path)
    return extracter.get_spreadsheet_data(spreadsheet_id)


@task
def _flatten_list(outer_list: List[List]) -> List:
    # Flatten a list of list.
    return [element for inner_list in outer_list for element in inner_list]


@task
def _transform(sheet_data: SheetData) -> FormattedSheetData:
    # Transform the sheet data into formatted sheet data, in order to load it into the database.
    try:
        return GoogleSheetsInstitutionExtracter.process_sheet_data(sheet_data)
    except exceptions.UnrecognizedGoogleSheetsFormat:
        log.error(
            f"Unable to read {sheet_data.sheet_title} "
            f"because of unknown google sheets format {sheet_data.meta_data.get('format')}"
        )


@task
def _load(formatted_sheet_data: FormattedSheetData, db_connection_url: str) -> List:
    # Load the formatted sheet data into the database.
    try:
        database = MongoDBDatabase(db_connection_url)
        loaded_document_ids = database.load(formatted_sheet_data)
        database.close_connection()
        return loaded_document_ids
    except exceptions.UnrecognizedGoogleSheetsFormat:
        log.error(
            f"Unable to load {formatted_sheet_data.sheet_title} "
            f"because of unknown google sheets format {formatted_sheet_data.meta_data.get('format')}"
        )


@task
def clean_up(db_connection_url: str, document_ids: List[List]):
    # Delete any documents from the database that are not in the list of inserted/updated document of ids.
    database = MongoDBDatabase(db_connection_url)
    database.clean_up([doc for doc_list in document_ids for doc in doc_list])
    database.close_connection()


def run_sigla_pipeline(
    master_spreadsheet_id: str, google_api_credentials_path: str, db_connection_url: str
):
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
    with Flow("ETL Pipeline") as flow:
        # Extract sheets data.
        # Get back list of List of SheetData
        spreadsheets_data = _extract.map(
            spreadsheets_id, unmapped(google_api_credentials_path),
        )
        # Flatten the list of list of SheetData
        flattened_spreadsheets_data = _flatten_list(spreadsheets_data)
        # Transform list of SheetData into FormattedSheetData
        formatted_sheets_data = _transform.map(flattened_spreadsheets_data)
        # Load list of FormattedSheetData into the database.
        # Get back List of List of doc ids
        document_ids = _load.map(formatted_sheets_data, unmapped(db_connection_url))
        # Deleted any documents that wasn't inserted/updated document from previous step.
        clean_up(db_connection_url, document_ids)
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

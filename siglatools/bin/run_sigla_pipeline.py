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

from distributed import LocalCluster
from prefect import Flow, flatten, task, unmapped
from prefect.executors import DaskExecutor

from siglatools import get_module_version

from ..databases import MongoDBDatabase
from ..databases.constants import Environment
from ..institution_extracters.constants import GoogleSheetsFormat as gs_format
from ..pipelines.exceptions import PrefectFlowFailure
from ..pipelines.utils import (
    _create_filter_task,
    _extract,
    _get_spreadsheet_ids,
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

###############################################################################

# Tasks


@task
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
    master_spreadsheet_id:
        The master spreadsheet id.
    google_api_credentials_path: str
        The path to Google API credentials file needed to read Google Sheets.
    db_connection_url: str
        The DB's connection url str.
    """
    log.info("Finished pipeline set up, start running pipeline")
    log.info("=" * 80)
    # Spawn local dask cluster
    cluster = LocalCluster()
    # Log the dashboard link
    log.info(f"Dashboard available at: {cluster.dashboard_link}")
    # Setup workflow
    with Flow("SIGLA Data Pipeline") as flow:
        # Delete all documents from db
        clean_up_task = _clean_up(db_connection_url)
        # Get spreadsheet ids
        spreadsheet_ids = _get_spreadsheet_ids(
            master_spreadsheet_id, google_api_credentials_path
        )
        # Extract sheets data.
        # Get back list of list of SheetData
        spreadsheets_data = _extract.map(
            spreadsheet_ids,
            unmapped(google_api_credentials_path),
            upstream_tasks=[unmapped(clean_up_task)],
        )

        # Transform list of SheetData into FormattedSheetData
        formatted_spreadsheets_data = _transform.map(flatten(spreadsheets_data))
        # Create instituton filter
        gs_institution_filter = _create_filter_task(
            [
                gs_format.standard_institution,
                gs_format.multiple_sigla_answer_variable,
            ]
        )
        # Filter to list of institutional formatted sheet data
        gs_institutions_data = gs_institution_filter(formatted_spreadsheets_data)
        # Create composite filter
        gs_composite_filter = _create_filter_task(
            [
                gs_format.composite_variable,
                gs_format.institution_and_composite_variable,
            ]
        )
        # Filter to list of composite formatted sheet data
        gs_composites_data = gs_composite_filter(formatted_spreadsheets_data)

        # Load instutional data
        load_institutions_data_task = _load_institutions_data.map(
            gs_institutions_data, unmapped(db_connection_url)
        )
        # Load composite data
        load_composites_data_task = _load_composites_data.map(
            gs_composites_data,
            unmapped(db_connection_url),
            upstream_tasks=[unmapped(load_institutions_data_task)],
        )
        # Log spreadsheets that were loaded
        _log_spreadsheets(spreadsheets_data, upstream_tasks=[load_composites_data_task])

    # Run the flow
    state = flow.run(executor=DaskExecutor(cluster.scheduler_address))
    if state.is_failed():
        raise PrefectFlowFailure(flow.name)


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
            "-dbe",
            "--db-env",
            action="store",
            dest="db_env",
            type=str,
            help="The environment of the database, staging or production",
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
        if args.master_spreadsheet_id is None:
            raise Exception("No main spreadsheet id found.")
        if args.db_env not in [Environment.staging, Environment.production]:
            raise Exception(
                "Incorrect database enviroment specification. Use 'staging' or 'production'."
            )
        log.info(
            f"""Loading all spreadsheets in the master spreadsheet {args.master_spreadsheet_id}""",
            f" to the {args.db_env} database.",
        )
        run_sigla_pipeline(
            args.master_spreadsheet_id,
            args.google_api_credentials_path,
            args.staging_db_connection_url
            if args.db_env == Environment.staging
            else args.prod_db_connection_url,
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

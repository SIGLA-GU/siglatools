#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script will get deployed in the bin directory of the
users' virtualenv when the parent module is installed using pip.
"""

import argparse
import logging
import re
import ssl
import sys
import traceback
import urllib

from distributed import LocalCluster
from prefect import Flow, unmapped
from prefect.engine.executors import DaskExecutor

from siglatools import get_module_version

from ..institution_extracters.google_sheets_institution_extracter import (
    GoogleSheetsInstitutionExtracter,
)
from ..institution_extracters.utils import SheetData, convert_rowcol_to_A1_name
from .run_sigla_pipeline import _extract
from .utils import _flatten_list

###############################################################################

logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)
log = logging.getLogger()

###############################################################################

URL_REGEX = r"(https?://\S+)"


def _check_external_links(sheet_data: SheetData):
    """
    Check for broken external links in a sheet

    Parameters
    ----------
    sheet_data: SheetData
        The sheet's data.
    """
    for i, row in enumerate(sheet_data.data):
        for j, cell in enumerate(sheet_data.data[i]):
            urls = re.findall(URL_REGEX, cell)
            for url in urls:
                try:
                    urllib.request.urlopen(url)
                except urllib.error.HTTPError:
                    row_index = int(sheet_data.meta_data.get("start_row")) + i - 1
                    # Assume always bounding box starts in first column
                    col_index = j
                    log.info(
                        (
                            f"BROKEN LINK: {sheet_data.spreadsheet_title}"
                            f" | {sheet_data.sheet_title}"
                            f" | {convert_rowcol_to_A1_name(row_index, col_index)}"
                            f" | {url}"
                        )
                    )


def run_external_link_checker(
    master_spreadsheet_id: str,
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
    # Create a connection to the google sheets reader
    google_sheets_institution_extracter = GoogleSheetsInstitutionExtracter(
        google_api_credentials_path
    )
    # Get the list of spreadsheets ids from the master spreadsheet
    spreadsheets_id = google_sheets_institution_extracter.get_spreadsheets_id(
        master_spreadsheet_id
    )
    log.info("Finished external link checker set up, start external link checking")
    log.info("=" * 80)
    # Spawn local dask cluster
    cluster = LocalCluster()
    # Log the dashboard link
    log.info(f"Dashboard available at: {cluster.dashboard_link}")
    # Setup workflow
    with Flow("Extract") as flow:
        # Extract sheets data.
        # Get back list of list of SheetData
        spreadsheets_data = _extract.map(
            spreadsheets_id,
            unmapped(google_api_credentials_path),
        )

        # Flatten the list of list of SheetData
        _flatten_list(spreadsheets_data)

    state = flow.run(executor=DaskExecutor(cluster.scheduler_address))
    sheets_data = state.result[flow.get_tasks(name="_flatten_list")[0]].result
    log.info("=" * 80)
    # Check external links for each sheet
    for sheet_data in sheets_data:
        _check_external_links(sheet_data)


###############################################################################
# Args


class Args(argparse.Namespace):
    def __init__(self):
        self.__parse()

    def __parse(self):
        # Set up parser
        p = argparse.ArgumentParser(
            prog="run_external_link_checker",
            description="A script to run external link checker",
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
            "--debug", action="store_true", dest="debug", help=argparse.SUPPRESS
        )
        # Parse
        p.parse_args(namespace=self)


###############################################################################


def main():
    try:
        args = Args()
        dbg = args.debug
        ssl._create_default_https_context = ssl._create_unverified_context
        run_external_link_checker(
            args.master_spreadsheet_id,
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

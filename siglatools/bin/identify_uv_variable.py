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
from datetime import date

from siglatools import get_module_version

from ..institution_extracters.exceptions import InvalidDateRange

###############################################################################

logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)
log = logging.getLogger()

###############################################################################


def _get_date_range(start_date: str, end_date: str):
    """
    Get the date range as date objects.

    Parameters
    ----------
    start_date: str
        The start date.
    end_date: str
        The end date.
    Returns
    ------
    date_range: List[date]
        The date range.
    """
    s_date = date.fromisoformat(start_date)
    e_date = date.fromisoformat(end_date)
    if s_date > e_date:
        raise InvalidDateRange(start_date, end_date)
    return [s_date, e_date]


def identify_uv_variable(
    master_spreadsheet_id: str,
    google_api_credentials_path: str,
    start_date: date,
    end_date: date,
):
    """
    Identify variables that needs updating and verifying or that variables
    that falls within the date range.

    Parameters
    ----------
    master_spreadsheet_id: str
        The master spreadsheet id
    google_api_credentials_path: str
        The path to Google API credentials file needed to read Google Sheets.
    start_date: date
        The start date.
    end_date: date
        The end date.
    """

    log.info(f"Identifying variables between {start_date} and {end_date}")


###############################################################################
# Args


class Args(argparse.Namespace):
    def __init__(self):
        self.__parse()

    def __parse(self):
        # Set up parser
        p = argparse.ArgumentParser(
            prog="identify_uv_variable",
            description="A script to identify variables that needs updating and verifying.",
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
            "-sd",
            "--start_date",
            action="store",
            dest="start_date",
            type=str,
            help="The start date",
        )
        p.add_argument(
            "-ed",
            "--end_date",
            action="store",
            dest="end_date",
            type=str,
            help="The end date",
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
        [start_date, end_date] = _get_date_range(args.start_date, args.end_date)
        identify_uv_variable(
            args.master_spreadsheet_id,
            args.google_api_credentials_path,
            start_date,
            end_date,
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

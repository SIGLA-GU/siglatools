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
from datetime import date
from typing import List, NamedTuple

from distributed import LocalCluster
from prefect import Flow, flatten, task, unmapped
from prefect.engine.executors import DaskExecutor

from siglatools import get_module_version

from ..institution_extracters.exceptions import InvalidDateRange
from ..institution_extracters.utils import SheetData
from ..pipelines.utils import _extract, _get_spreadsheet_ids

###############################################################################

logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)
log = logging.getLogger()

###############################################################################


class NextUVDateData(NamedTuple):
    """
    A next update and verify date and its context

    Attributes:
        spreadsheet_title: str
            The title of the spreadsheet the contains the next uv date.
        sheet_title: str
            The title of the sheet that contains the next uv date.
        column_name: str
            The column location of the next uv date in the sheet.
        row_index: int
            The row location of the next uv date in the sheet.
        next_uv_date: str
            The next uv date.

    """

    spreadsheet_title: str
    sheet_title: str
    column_name: str
    row_index: int
    next_uv_date: date


class NextUVDateStatus:
    """
    Possible status of a next uv date.
    """

    requires_uv = "Requires update and verify"
    incorrect_date_format = "Incorrect date format"
    irrelevant = "Irrelevant"


class CheckedNextUVDate(NamedTuple):
    """
    The status of a next uv date after checking.

    Attributes:
        status: NextUVDateStatus
            The status of a next uv date after checking.
        next_uv_date_data: NextUVDateData
            The next uv date and its context. See NextUVDateData class.
    """

    status: NextUVDateStatus
    next_uv_date_data: NextUVDateData


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


@task
def _extract_next_uv_dates(sheet_data: SheetData) -> List[NextUVDateData]:
    """
    Prefect task to gather next uv dates.

    Parameters
    ----------
    sheet_data: SheetData
        The sheet's data.

    Returns
    -------
    next_uv_dates_data: List[NextUVDateData]
        The list of next uv dates.
    """
    column_name = sheet_data.meta_data.get("date_of_next_uv_column")
    next_uv_dates_data = [
        NextUVDateData(
            spreadsheet_title=sheet_data.spreadsheet_title,
            sheet_title=sheet_data.sheet_title,
            column_name=column_name,
            row_index=int(sheet_data.meta_data.get("start_row")) + i,
            next_uv_date=next_uv_date,
        )
        for i, next_uv_date in enumerate(sheet_data.next_uv_dates)
        # Ignore empty and `Date of Next U&V` cells"
        if next_uv_date.strip() and "date" not in next_uv_date.strip().lower()
    ]
    return next_uv_dates_data


@task
def _check_next_uv_date(
    next_uv_date_data: NextUVDateData, start_date: date, end_date: date
) -> CheckedNextUVDate:
    """
    Prefect task to check if the next uv date falls within the given start_date and end_date.

    Parameters
    ----------
    next_uv_date_data: NextUVDateData
        The next uv date and its context. See NextUVDateData class.
    start_date: date
        The start date of the date range.
    end_date: date
        The end date of the date range.

    Returns
    -------
    checked_next_uv_date: CheckedNextUVDate
        The status of the next uv date.
    """
    status = None
    try:
        next_uv_date = date.fromisoformat(next_uv_date_data.next_uv_date)
        if next_uv_date >= start_date and next_uv_date <= end_date:
            status = NextUVDateStatus.requires_uv
        else:
            status = NextUVDateStatus.irrelevant
    except ValueError:
        status = NextUVDateStatus.incorrect_date_format
    return CheckedNextUVDate(status=status, next_uv_date_data=next_uv_date_data)


def get_next_uv_dates(
    master_spreadsheet_id: str,
    google_api_credentials_path: str,
    start_date: date,
    end_date: date,
):
    """
    Get next update and verify dates or uv dates that falls within the date range.

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
    log.info("Finished setup, start finding next uv dates.")
    log.info("=" * 80)
    # Spawn local dask cluster
    cluster = LocalCluster()
    # Log the dashboard link
    log.info(f"Dashboard available at: {cluster.dashboard_link}")
    # Setup workflow
    with Flow("Get next update and verify dates") as flow:
        # Get the list of spreadsheet ids from the master spreadsheet
        spreadsheet_ids = _get_spreadsheet_ids(
            master_spreadsheet_id, google_api_credentials_path
        )
        # Extract sheets data.
        # Get back list of list of SheetData
        spreadsheets_data = _extract.map(
            spreadsheet_ids,
            unmapped(google_api_credentials_path),
        )
        log.info("Finished extracting the spreadsheet data.")
        # Extract next uv dates
        next_uv_dates_data = _extract_next_uv_dates.map(flatten(spreadsheets_data))
        log.info("Finished extracting the next uv dates.")
        # Check next uv dates
        _check_next_uv_date.map(
            flatten(next_uv_dates_data), unmapped(start_date), unmapped(end_date)
        )
        log.info("Finished checking next uv dates.")

    # Run the flow
    state = flow.run(executor=DaskExecutor(cluster.scheduler_address))
    # Get the list of CheckedNextUVDates
    checked_next_uv_dates = state.result[
        flow.get_tasks(name="_check_next_uv_date")[0]
    ].result
    log.info("=" * 80)
    # Get next uv dates
    next_uv_dates = [
        next_uv_date
        for next_uv_date in checked_next_uv_dates
        if next_uv_date.status != NextUVDateStatus.irrelevant
    ]
    sorted_next_uv_dates = sorted(
        next_uv_dates,
        key=lambda x: (
            x.next_uv_date_data.spreadsheet_title,
            x.next_uv_date_data.sheet_title,
            x.next_uv_date_data.row_index,
        ),
    )
    # Write next uv dates to a csv file
    with open("next_uv_dates.csv", mode="w") as csv_file:
        fieldnames = ["spreadsheet_title", "sheet_title", "cell", "status"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for next_uv_date in sorted_next_uv_dates:
            next_uv_date_data = next_uv_date.next_uv_date_data
            writer.writerow(
                {
                    "spreadsheet_title": next_uv_date_data.spreadsheet_title,
                    "sheet_title": next_uv_date_data.sheet_title,
                    "cell": f"{next_uv_date_data.column_name}{next_uv_date_data.row_index}",
                    "status": next_uv_date.status,
                }
            )
    log.info("Finished writing next uv dates csv file")


###############################################################################
# Args


class Args(argparse.Namespace):
    def __init__(self):
        self.__parse()

    def __parse(self):
        # Set up parser
        p = argparse.ArgumentParser(
            prog="get_next_uv_dates",
            description="A script to get next update and verify dates.",
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
        get_next_uv_dates(
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

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
from typing import List, NamedTuple, Optional

import requests
from distributed import LocalCluster
from prefect import Flow, flatten, task, unmapped
from prefect.executors import DaskExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from siglatools import get_module_version

from ..institution_extracters.constants import MetaDataField
from ..institution_extracters.utils import SheetData, convert_rowcol_to_A1_name
from ..pipelines.exceptions import PrefectFlowFailure
from ..pipelines.utils import _extract, _get_spreadsheet_ids
from ..utils.exceptions import ErrorInfo

###############################################################################

logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)
log = logging.getLogger()

###############################################################################

URL_REGEX = r"(https?://\S+)"


class GoogleSheetCell(NamedTuple):
    """
    A GoogleSheet cell.

    Attributes:
        spreadsheet_title: str
            The title of the spreadsheet the contains the URL.
        sheet_title: str
            The title of the sheet that contains the URL.
        row_index: int
            The row index of the cell.
        col_index: int
            The col index of the cell.
        url: Optional[str] = None
            The url in the cell.
        msg: Optional[str] = None
            The status of url.
    """

    spreadsheet_title: str
    sheet_title: str
    row_index: int
    col_index: int
    url: Optional[str] = None
    msg: Optional[str] = None


class URLData(NamedTuple):
    """
    The URL and its context.

    Attributes:
        cells: List[GoogleSheetCell]
            The list of cells that has the URL.
        url: str
            The URL.
    """

    cells: List[GoogleSheetCell]
    url: str

    def get_key(self) -> str:
        "Get the key."
        return self.url

    def add_cell(self, cell: GoogleSheetCell):
        "Add a cell to the list of cells."
        self.cells.append(cell)


class CheckedURL(NamedTuple):
    """
    The status of an URL after checking.

    Attributes:
        has_error: bool
            Whether the URL has an error.
        url_data: URLData
            The URL and its context. See URLData class.
        msg: Optional[str] = None
            The status of the URL after checking. None if has_error is False.
    """

    has_error: bool
    url_data: URLData
    msg: Optional[str] = None


@task
def _extract_external_links(sheet_data: SheetData) -> List[URLData]:
    """
    Prefect Task to extract external links from a sheet.

    Parameters
    ----------
    sheet_data: SheetData
        The sheet's data.

    Returns
    -------
    urls_data: List[URLData]
        The list of URLs and their context. See URLData class.
    """
    urls_data = []
    for i, row in enumerate(sheet_data.data):
        for j, cell in enumerate(row):
            urls = re.findall(URL_REGEX, cell)
            row_index = int(sheet_data.meta_data.get(MetaDataField.start_row)) + i - 1
            # Assume bounding box always starts in the first column of a sheet
            col_index = j
            for url in urls:
                urls_data.append(
                    URLData(
                        cells=[
                            GoogleSheetCell(
                                spreadsheet_title=sheet_data.spreadsheet_title,
                                sheet_title=sheet_data.sheet_title,
                                row_index=row_index,
                                col_index=col_index,
                            )
                        ],
                        url=url,
                    )
                )
    return urls_data


@task
def _unique_external_links(urls_data: List[URLData]) -> List[URLData]:
    """
    Prefect Task to merge url data together by merging cells.

    Parameters
    ----------
    urls_data: List[URLData]
        The list of URLs and their contexts.

    Returns
    -------
    urls_data: List[URLData]
        The list of merged urls data.
    """
    external_links_group = {}
    for url_data in urls_data:
        if url_data.get_key() not in external_links_group:
            external_links_group.update({url_data.get_key(): url_data})
        else:
            external_links_group.get(url_data.get_key()).add_cell(url_data.cells[0])
    unique_urls_data = list(external_links_group.values())
    log.info(f"Found {len(unique_urls_data)} unique links of {len(urls_data)}.")
    return unique_urls_data


@task
def _check_external_link(url_data: URLData) -> CheckedURL:
    """
    Prefect Task to check the status of the URL.

    Parameters
    ----------
    url_data: URLData
        The URL and its context. See URLData class.

    Returns
    -------
        checked_url: CheckedURL
        The status of the URL after checking. See CheckedURL class.
    """
    has_error = False
    error_msg = None
    response = None
    try:
        http = requests.Session()
        retry_strategy = Retry(
            total=2,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http.mount("https://", adapter)
        http.mount("http://", adapter)
        http.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0",
            }
        )
        response = http.get(
            url_data.url,
            allow_redirects=True,
            timeout=5.0,
            verify=True,
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        has_error = True
        error_msg = f"{response.status_code} - {response.reason}"
    except requests.exceptions.SSLError:
        has_error = True
        error_msg = "Untrusted SSL Certificate"
    except requests.exceptions.Timeout as error:
        has_error = True
        error_msg = f"Request timed out: {error}"
    except requests.exceptions.ConnectionError as error:
        has_error = True
        error_msg = f"Error connecting: {error}"
    except requests.exceptions.RequestException as error:
        has_error = True
        error_msg = f"Unknown error: {error}"
    log.info(f"Finished checking {url_data.url}")
    return CheckedURL(has_error=has_error, url_data=url_data, msg=error_msg)


def run_external_link_checker(
    master_spreadsheet_id: str,
    google_api_credentials_path: str,
    spreadsheet_ids_str: Optional[str] = None,
):
    """
    Run the the external link checker.
    If a list of spreadsheet ids are provided, run the external link checker
    against the list of spreadsheet ids, instead of the spreadsheet ids gathered
    from the master spreadsheet.

    Parameters
    ----------
    master_spreadsheet_id: str
        The master spreadsheet id.
    google_api_credentials_path: str
        The path to Google API credentials file needed to read Google Sheets.
    spreadsheet_ids_str: Optional[str]
        The list spreadsheet ids, delimited by comma.
    """
    log.info("Finished external link checker set up, start checking external link.")
    log.info("=" * 80)
    # Spawn local dask cluster
    cluster = LocalCluster()
    # Log the dashboard link
    log.info(f"Dashboard available at: {cluster.dashboard_link}")
    # Setup workflow
    with Flow("Check external links") as flow:
        # Get spreadsheet ids
        spreadsheet_ids = _get_spreadsheet_ids(
            master_spreadsheet_id, google_api_credentials_path, spreadsheet_ids_str
        )

        # Extract sheets data.
        # Get back list of list of SheetData
        spreadsheets_data = _extract.map(
            spreadsheet_ids,
            unmapped(google_api_credentials_path),
        )
        # Extract links from list of SheetData
        # Get back list of list of URLData
        links_data = _extract_external_links.map(flatten(spreadsheets_data))
        # Unique the url data
        unique_links_data = _unique_external_links(flatten(links_data))
        # Check external links
        _check_external_link.map(unique_links_data)

    # Run the flow
    state = flow.run(executor=DaskExecutor(cluster.scheduler_address))
    if state.is_failed():
        raise PrefectFlowFailure(ErrorInfo({"flow_name": flow.name}))
    # Get the list of CheckedURL
    checked_links = state.result[flow.get_tasks(name="_check_external_link")[0]].result
    log.info("=" * 80)
    # Get error links
    error_links = [link for link in checked_links if link.has_error]
    gs_cells = []
    for error_link in error_links:
        for cell in error_link.url_data.cells:
            gs_cells.append(
                GoogleSheetCell(
                    spreadsheet_title=cell.spreadsheet_title,
                    sheet_title=cell.sheet_title,
                    row_index=cell.row_index,
                    col_index=cell.col_index,
                    url=error_link.url_data.url,
                    msg=error_link.msg,
                )
            )

    sorted_gs_cells = sorted(
        gs_cells,
        key=lambda x: (
            x.spreadsheet_title,
            x.sheet_title,
            x.row_index,
            x.col_index,
            x.url,
        ),
    )
    # Write error links to a csv file
    with open("external_links.csv", mode="w") as csv_file:
        fieldnames = ["spreadsheet_title", "sheet_title", "cell", "url", "reason"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for gs_cell in sorted_gs_cells:
            writer.writerow(
                {
                    "spreadsheet_title": gs_cell.spreadsheet_title,
                    "sheet_title": gs_cell.sheet_title,
                    "cell": convert_rowcol_to_A1_name(
                        gs_cell.row_index, gs_cell.col_index
                    ),
                    "url": gs_cell.url,
                    "reason": f"{gs_cell.msg}",
                }
            )
    log.info("Finished writing external links csv file")


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
            "-ssi",
            "--spreadsheet-ids",
            action="store",
            dest="spreadsheet_ids",
            type=str,
            help="The list of spreadsheet ids, delimited by comma",
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
        run_external_link_checker(
            args.master_spreadsheet_id,
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

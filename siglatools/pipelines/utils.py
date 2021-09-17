#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from datetime import timedelta
from typing import List, Optional

from prefect import task
from prefect.tasks.control_flow import FilterTask

from ..databases import MongoDBDatabase
from ..institution_extracters import GoogleSheetsInstitutionExtracter
from ..institution_extracters.constants import MetaDataField
from ..institution_extracters.utils import FormattedSheetData, SheetData

###############################################################################

logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)
log = logging.getLogger()

######################################################


@task
def _get_spreadsheet_ids(
    master_spreadsheet_id: str,
    google_api_credentials_path: str,
    spreadsheet_ids_str: Optional[str] = None,
) -> List[str]:
    """
    Prefect task to get spreadsheet ids from the master spreadsheet.
    If list of spreadsheet ids is given, return the list.

    Parameters
    ----------
    master_spreadsheet_id: str
        The master spreadsheet id.
    google_api_credentials_path: str
        The path to Google API credentials file needed to read Google Sheets.
    spreadsheet_ids_str: Optional[str] = None
        The list of spreadsheet ids str.

    Returns
    -------
    spreadsheet_ids: List[str]
        The list of spreadsheet ids.
    """
    # If spreadsheet ids are provided
    if spreadsheet_ids_str:
        return [
            spreadsheet_id.strip() for spreadsheet_id in spreadsheet_ids_str.split(",")
        ]

    # If spreadsheet ids are not provided
    # Create a connection to the google sheets reader
    google_sheets_institution_extracter = GoogleSheetsInstitutionExtracter(
        google_api_credentials_path
    )
    # Get the list of spreadsheets ids from the master spreadsheet
    spreadsheet_ids = google_sheets_institution_extracter.get_spreadsheet_ids(
        master_spreadsheet_id
    )
    return spreadsheet_ids


@task(max_retries=5, retry_delay=timedelta(seconds=100))
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


@task
def _load_institutions_data(
    formatted_sheet_data: FormattedSheetData,
    db_connection_url: str,
):
    """
    Prefect task to oad the institutional formatted sheet data into the database.

    Parameters
    ----------
    formatted_sheet_data: FormattedSheetData
        The sheet's formatted data.
    db_connection_url: str
        The DB's connection url str.
    """
    database = MongoDBDatabase(db_connection_url)
    database.load(formatted_sheet_data)
    database.close_connection()


@task
def _load_composites_data(
    formatted_sheet_data: FormattedSheetData,
    db_connection_url: str,
):
    """
    Prefect task to load the composite formatted sheet data into the database.

    Parameters
    ----------
    formatted_sheet_data: FormattedSheetData
        The sheet's formatted data.
    db_connection_url: str
        The DB's connection url str.
    """
    database = MongoDBDatabase(db_connection_url)
    database.load(formatted_sheet_data)
    database.close_connection()


@task
def _log_spreadsheets(spreadsheets_data: List[List[SheetData]]):
    """
    Prefect task to log the spreadsheet titles.

    Parameters
    ----------
    spreadsheets_data: List[List[SheetData]]
        The list of spreadsheet data.
    """
    spreadsheets_title = [
        spreadsheet_data[0].spreadsheet_title for spreadsheet_data in spreadsheets_data
    ]
    log.info("=" * 80)
    log.info(f"""Finished processing {", ".join(spreadsheets_title)}.""")


def _create_filter_task(gs_formats: List[str]) -> FilterTask:
    """
    Create a Prefect FilterTask that filters FormattedSheetData based on the given GoogleSheet formats.

    Parameters
    ----------
    gs_formats: List[str]
        The list of GoogleSheet formats.

    Returns
    -------
    task: FilterTask
        The filter task.
    """
    return FilterTask(
        filter_func=lambda x: x.meta_data.get(MetaDataField.format) in gs_formats
    )

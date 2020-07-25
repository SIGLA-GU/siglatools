#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from typing import Dict, List, NamedTuple, Optional, Union

from google.oauth2 import service_account
from googleapiclient.discovery import build

from . import constants as institution_extracters_constants
from . import exceptions

###############################################################################

log = logging.getLogger(__name__)

GOOGLE_API_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

###############################################################################


class SheetData(NamedTuple):
    sheet_title: str
    meta_data: Dict[str, str]
    data: List[List[str]]


class FormattedSheetData(NamedTuple):
    sheet_title: str
    meta_data: Dict[str, str]
    formatted_data: List


class SpreadsheetData(NamedTuple):
    # TODO make this appear in doc
    # The list of sheet titles of a spreadsheet.
    sheet_titles: List[str]
    # The meta data of spreadsheet, one datum (a dict) for each sheet.
    meta_data: List[Dict[str, str]]
    # The data of a spreadsheet, one datum (a list of list) for each sheet.
    data: List[List[List[str]]]


class A1Notation(NamedTuple):
    # This doesn't capture all possible A1 notations because start_row and end_row are required,
    # but they don't have to be.
    sheet_title: str
    start_row: int
    end_row: int
    start_column: Optional[str] = None
    end_column: Optional[str] = None

    def __str__(self) -> str:
        if self.start_column is not None and self.end_column is not None:
            return f"{self.sheet_title}!{self.start_column}{self.start_row}:{self.end_column}{self.end_row}"
        else:
            return f"{self.sheet_title}!{self.start_row}:{self.end_row}"


class GoogleSheetsInstitutionExtracter:
    def __init__(self, credentials_path: str):
        self._credentials_path = credentials_path
        # Creates a Credentials instance from a service account json file.
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=GOOGLE_API_SCOPES
        )
        # Construct a Resource for interacting with Google Sheets API
        service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
        # Store the spreadsheets service
        self.spreadsheets = service.spreadsheets()

    @staticmethod
    def _construct_a1_notation(a1_notation: A1Notation) -> str:
        """
        Construction an A1 Notation str
        https://developers.google.com/sheets/api/guides/concepts#a1_notation

        For a description of an A1 notation, please view the A1Notation class atttributes.

        Parameters
        ----------
        a1_notation: A1Notation
            The attributes of an A1 notation

        Returns
        ----------
        a1_notation: str
            The str representation of an A1 notation.

        Examples
        ----------
        ```
        # Get A1 notation for the all the cells in top two rows of Sheet1
        GoogleSheetsInstitutionExtracter.__construct_a1_notation(
            A1Notation(
                sheet_title="Sheet1"
                start_row=1
                end_row=2
            )
        )

        # Get A1 notation for the first two cells in the top two rows of Sheet1
        GoogleSheetsInstitutionExtracter.__construct_a1_notation(
            A1Notation(
                sheet_title="Sheet1"
                start_row=1
                end_row=2
                start_column="A"
                end_column="B"
            )
        )
        ```
        """

        if int(a1_notation.start_row) > int(a1_notation.end_row):
            # Start row is greater than end row
            raise exceptions.InvalidRangeInA1Notation(
                a1_notation.sheet_title, a1_notation.start_row, a1_notation.end_row
            )
        elif (
            a1_notation.start_column is not None and a1_notation.end_column is not None
        ):
            # Start and end column are both present
            if len(a1_notation.start_column) > len(a1_notation.end_column):
                # Length of start column is greater than length end column
                raise exceptions.InvalidRangeInA1Notation(
                    a1_notation.sheet_title,
                    a1_notation.start_column,
                    a1_notation.end_column,
                )
            elif (
                len(a1_notation.start_column) == len(a1_notation.end_column)
                and a1_notation.start_column > a1_notation.end_column
            ):
                # Start column is greater than end column
                raise exceptions.InvalidRangeInA1Notation(
                    a1_notation.sheet_title,
                    a1_notation.start_column,
                    a1_notation.end_column,
                )
            else:
                return str(a1_notation)
        elif any(
            field is not None
            for field in [a1_notation.start_column, a1_notation.end_column]
        ):
            # Either start column or end column is present
            raise exceptions.IncompleteColumnRangeInA1Notation(
                a1_notation.sheet_title,
                a1_notation.start_column,
                a1_notation.end_column,
            )
        else:
            # Neither start column nor end column is present
            return str(a1_notation)

    def _get_meta_data_a1_notations(self, spreadsheet_id: str) -> List[A1Notation]:
        """
        Construct an A1Notation for each sheet from its first two rows of meta data

        Parameters
        ----------
        spreadsheet_id: str
            The id of a spreadsheet

        Returns
        ----------
        a1_notations: List[A1Notatoin]
            The list of A1Notations, one for each sheet.
        """
        # Get the spreadsheet
        spreadsheet_response = self.spreadsheets.get(
            spreadsheetId=spreadsheet_id
        ).execute()
        # Create an A1Notation for each sheet's meta data
        return [
            A1Notation(
                sheet_title=sheet.get("properties").get("title"), start_row=1, end_row=2
            )
            for sheet in spreadsheet_response.get("sheets")
        ]

    def _get_constitutional_rights(self, sheet_data: SheetData) -> List:
        institution = {
            "name": sheet_data.meta_data.get("name"),
            "category": sheet_data.meta_data.get("category"),
            "country": sheet_data.meta_data.get("country"),
            "variables": [
                {
                    "heading": row[0],
                    "beneficiaries": row[1],
                    "name": row[2],
                    "orig_text": row[3],
                    "source": row[4],
                }
                for row in sheet_data.data[1:]
            ],
        }
        return [institution]

    def _get_constitutional_amendments(self, sheet_data: SheetData) -> List:
        institution = {
            "name": sheet_data.meta_data.get("name"),
            "category": sheet_data.meta_data.get("category"),
            "country": sheet_data.meta_data.get("country"),
            "variables": [
                {
                    "heading": row[0],
                    "name_and_number": row[1],
                    "date_in_force": row[2],
                    "portions_of_constitutional_text_modified": row[3],
                    "sigla_answer": row[4],
                    "orig_text": row[5],
                    "source": row[6],
                }
                for row in sheet_data.data[1:]
            ],
        }
        return [institution]

    def _get_institution_by_rows(
        self, sheet_data: SheetData
    ) -> List[Dict[str, Union[str, List[Dict[str, Union[int, str]]]]]]:
        """
        Get the list of institutions and their variables from a sheet.

        Parameters
        ----------
        sheet_data: SheetData
            The data of a sheet.

        Returns
        ----------
        institutions: List[Dict[str, Union[str, List[Dict[str, Union[int, str]]]]]]
            The list of institutions and their variables
        """
        # Get the variable names in the 2nd row of data,
        # exclulde 'Source' columns and the first column (the institution name in Spanish)
        variable_names = [name for name in sheet_data.data[1][1:] if name != "Source"]

        institutions = [
            {
                "name": institution_row[0],
                "category": sheet_data.meta_data.get("category"),
                "variables": [
                    {
                        # The headings are found in the 1st row of data
                        # The indices of institution_row are 1-more than the indices of variable_names
                        "heading": sheet_data.data[0][j * 2 + 1],
                        "name": variable_name,
                        "sigla_answer": institution_row[j * 2 + 1],
                        "source": institution_row[j * 2 + 2],
                    }
                    for j, variable_name in enumerate(variable_names)
                ],
            }
            # The institutions starts in the 3rd row of data
            for institution_row in sheet_data.data[2:]
        ]

        log.info(
            f"Found {len(institutions)} institutionss from sheet {sheet_data.sheet_title}"
        )
        return institutions

    def _get_institution_by_triples(
        self, sheet_data: SheetData
    ) -> List[Dict[str, Union[str, List[Dict[str, Union[int, str]]]]]]:
        """
        Get the list of institutions and their variables from a sheet.

        Parameters
        ----------
        sheet_data: SheetData
            The data of the sheet.

        Returns
        ----------
        institutions: List[Dict[str, Union[str, List[Dict[str, Union[int, str]]]]]]
            The list of institutions and their variables.
        """
        # print(sheet_title)

        # Get the institution names from the first row of data
        institution_names = [
            institution_name
            for institution_name in sheet_data.data[0]
            if institution_name
        ]
        # log.info(institution_names)
        institutions = [
            {
                "name": institution_name,
                "category": sheet_data.meta_data.get("category"),
                "country": sheet_data.meta_data.get("country"),
                "variables": [
                    {
                        "heading": variable_row[0],
                        "name": variable_row[1],
                        "index": j,
                        "sigla_answer": variable_row[2 + i * 3],
                        "orig_text": variable_row[2 + i * 3 + 1],
                        "source": variable_row[2 + i * 3 + 2],
                    }
                    # The variables starts in the 3rd row of data
                    for j, variable_row in enumerate(sheet_data.data[2:])
                ],
            }
            for i, institution_name in enumerate(institution_names)
        ]

        log.info(
            f"Found {len(institutions)} institutionss from sheet {sheet_data.sheet_title}"
        )
        return institutions

    def get_spreadsheet_data(self, spreadsheet_id: str) -> List[SheetData]:
        """
        Get the spreadsheet data given a spreadsheet id.

        Parameters
        ----------
        spreadsheet_id: str
            The id of the spreadsheet.

        Returns
        ----------
        spreadsheet_data: List[SheetData]
            The spreadsheet data. Please the SheetData class to view its attributes.
        """
        # Get an A1Notation for each sheet's meta data
        meta_data_a1_notations = self._get_meta_data_a1_notations(spreadsheet_id)
        # Get the meta data for each sheet
        meta_data_response = (
            self.spreadsheets.values()
            .batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=[
                    self._construct_a1_notation(a1_notation)
                    for a1_notation in meta_data_a1_notations
                ],
                majorDimension="COLUMNS",
            )
            .execute()
        )
        # Get data within a range (specified by an a1 notation) for each sheet
        meta_data_value_ranges = meta_data_response.get("valueRanges")
        # print(meta_data_value_ranges)
        # Create the meta datum for each sheet
        meta_data = [
            {value[0].strip(): value[1].strip() for value in value_range.get("values")}
            for value_range in meta_data_value_ranges
        ]
        # Use the meta datum to create an a1 notation to get the datum of each sheet
        bounding_box_a1_notations = [
            A1Notation(
                sheet_title=meta_data_a1_notations[i].sheet_title,
                start_row=int(meta_datum.get("start_row")),
                end_row=int(meta_datum.get("end_row")),
                start_column=meta_datum.get("start_column"),
                end_column=meta_datum.get("end_column"),
            )
            for i, meta_datum in enumerate(meta_data)
        ]
        # Get data within a range (specified by an a1 notation) for each sheet
        data_response = (
            self.spreadsheets.values()
            .batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=[
                    self._construct_a1_notation(a1_notation)
                    for a1_notation in bounding_box_a1_notations
                ],
                majorDimension="ROWS",
            )
            .execute()
        )
        data = [
            value_range.get("values")
            for value_range in data_response.get("valueRanges")
        ]

        log.info(f"Finished extracting spreadsheet {spreadsheet_id}")
        log.info(f"Found {len(meta_data)} sheets in spreadsheet {spreadsheet_id}")
        return [
            SheetData(
                sheet_title=a1_notation.sheet_title,
                meta_data=meta_data[i],
                data=data[i],
            )
            for i, a1_notation in enumerate(meta_data_a1_notations)
        ]

        """return SpreadsheetData(
            sheet_titles=[a1_notation.sheet_title for a1_notation in meta_data_a1_notations],
            meta_data=meta_data,
            data=data
        )"""

    def get_spreadsheets_id(self, master_spreadsheet_id: str) -> List[str]:
        """
        Get the list of spreadsheet ids from a master spreadsheet.

        Parameters
        ----------
        master_spreadsheet_id: str
            The id of the master spreadsheet.

        Returns
        ----------
        spreadsheet_ids: List[str]
            The list of spreadsheet ids.
        """
        spreadsheet_data = self.get_spreadsheet_data(master_spreadsheet_id)

        # There is only sheet in the master spreadsheet.
        # All spreadsheet ids are in the first column.
        spreadsheet_ids = [row[0] for row in spreadsheet_data[0].data]
        # spreadsheet_ids = [row[0] for row in spreadsheet_data.data[0]]
        log.info(
            f"Found {len(spreadsheet_ids)} spreadsheets from master spreadsheet {master_spreadsheet_id}"
        )
        return spreadsheet_ids

    def process_sheet_data(self, sheet_data: SheetData) -> FormattedSheetData:
        """ 
        TODO
        Process a sheet to get its data in a format ready to consumed by DB.

        Parameters
        ----------
        sheet_data: SheetData
            The data of the sheet.

        Returns
        ----------
        formatted_sheet_data: FormattedSheetData
            The data in reqired format. TODO
        """
        google_sheets_format = sheet_data.meta_data.get("format")
        if (
            google_sheets_format
            == institution_extracters_constants.GoogleSheetsFormat.institution_by_triples
        ):
            formatted_data = self._get_institution_by_triples(sheet_data)
            return FormattedSheetData(
                sheet_title=sheet_data.sheet_title,
                meta_data=sheet_data.meta_data,
                formatted_data=formatted_data,
            )
        elif (
            google_sheets_format
            == institution_extracters_constants.GoogleSheetsFormat.institution_by_rows
        ):
            formatted_data = self._get_institution_by_rows(sheet_data)
            return FormattedSheetData(
                sheet_title=sheet_data.sheet_title,
                meta_data=sheet_data.meta_data,
                formatted_data=formatted_data,
            )
        else:
            raise exceptions.UnrecognizedGoogleSheetsFormat(
                sheet_data.sheet_title, google_sheets_format
            )

    def __str__(self):
        return f"<GoogleSheetsInstitutionExtracter [{self._credentials_path}]>"

    def __repr__(self):
        return str(self)

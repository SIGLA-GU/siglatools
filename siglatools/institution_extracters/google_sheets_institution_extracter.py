#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from typing import Dict, List, NamedTuple, Optional, Union

from google.oauth2 import service_account
from googleapiclient.discovery import build

from ..databases import constants as database_constants
from . import constants as institution_extracters_constants
from . import exceptions
from .institution_extracter import InstitutionExtracter

###############################################################################

log = logging.getLogger(__name__)

GOOGLE_API_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets"
]

###############################################################################


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


class GoogleSheetsInstitutionExtracter(InstitutionExtracter):
    def __init__(
        self,
        credentials_path: str,
        **kwargs
    ):
        # Creates a Credentials instance from a service account json file.
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=GOOGLE_API_SCOPES
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
                a1_notation.sheet_title, a1_notation.start_row, a1_notation.end_row)
        elif a1_notation.start_column is not None and a1_notation.end_column is not None:
            # Start and end column are both present
            if len(a1_notation.start_column) > len(a1_notation.end_column):
                # Length of start column is greater than length end column
                raise exceptions.InvalidRangeInA1Notation(
                    a1_notation.sheet_title, a1_notation.start_column, a1_notation.end_column)
            elif (len(a1_notation.start_column) == len(a1_notation.end_column)
                    and a1_notation.start_column > a1_notation.end_column):
                # Start column is greater than end column
                raise exceptions.InvalidRangeInA1Notation(
                    a1_notation.sheet_title, a1_notation.start_column, a1_notation.end_column)
            else:
                return str(a1_notation)
        elif any(field is not None for field in [a1_notation.start_column, a1_notation.end_column]):
            # Either start column or end column is present
            raise exceptions.IncompleteColumnRangeInA1Notation(
                a1_notation.sheet_title, a1_notation.start_column, a1_notation.end_column)
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
        spreadsheet_response = self.spreadsheets.get(spreadsheetId=spreadsheet_id).execute()
        # Create an A1Notation for each sheet's meta datum
        return [
            A1Notation(sheet_title=sheet.get("properties").get("title"), start_row=1, end_row=2)
            for sheet in spreadsheet_response.get("sheets")
        ]

    """def _get_composite_variable(
        self,
        sheet_title: str,
        meta_datum: Dict[str, str],
        datum: List[List[str]]
    ) -> List:"""

    def _get_institution_by_rows(
        self,
        sheet_title: str,
        meta_datum: Dict[str, str],
        datum: List[List[str]]
    ) -> List[Dict[str, Union[str, List[Dict[str, Union[int, str]]]]]]:
        """
        Get the list of institutions and their variables from a sheet.

        Parameters
        ----------
        sheet_title: str
            The title of a sheet.
        meta_datum: Dict[str, str]
            The meta datum of a sheet.
        datum: List[List[str]]
            The datum of a sheet.

        Returns
        ----------
        institutions: List[Dict[str, Union[str, List[Dict[str, Union[int, str]]]]]]
            The list of institutions and their variables
        """
        # Get the variable names in the 2nd row of datum,
        # exclulde 'Source' columns and the first column (the institution name in Spanish)
        variable_names = [name for name in datum[1][1:] if name != "Source"]

        institutions = [
            {
                "collection": database_constants.DatabaseCollection.institutions,
                "name": institution_row[0],
                "category": meta_datum.get("category"),
                "variables": [
                    {
                        # The headings are found in the 1st row of datum
                        # The indices of institution_row are 1-more than the indices of variable_names
                        "heading": datum[0][j * 2 + 1],
                        "name": variable_name,
                        "sigla_answer": institution_row[j * 2 + 1],
                        "source": institution_row[j * 2 + 2]
                    }
                    for j, variable_name in enumerate(variable_names)
                ]
            }
            # The institutions starts in the 3rd row of datum
            for institution_row in datum[2:]
        ]

        return institutions

    def _get_institution_by_triples(
        self,
        sheet_title: str,
        meta_datum: Dict[str, str],
        datum: List[List[str]]
    ) -> List[Dict[str, Union[str, List[Dict[str, Union[int, str]]]]]]:
        """
        Get the list of institutions and their variables from a sheet.

        Parameters
        ----------
        sheet_title: str
            The title of a sheet.
        meta_datum: Dict[str, str]
            The meta datum of a sheet.
        datum: List[List[str]]
            The datum of a sheet.

        Returns
        ----------
        institutions: List[Dict[str, Union[str, List[Dict[str, Union[int, str]]]]]]
            The list of institutions and their variables.
        """
        # print(sheet_title)

        # Get the institution names from the first row of datum
        institution_names = [
            institution_name
            for institution_name in datum[0]
            if institution_name
        ]
        # log.info(institution_names)
        institutions = [
            {
                "collection": database_constants.DatabaseCollection.institutions,
                "name": institution_name,
                "category": meta_datum.get("category"),
                "country": meta_datum.get("country"),
                "variables": [
                    {
                        "heading": variable_row[0],
                        "name": variable_row[1],
                        "index": j,
                        "sigla_answer": variable_row[2 + i * 3],
                        "orig_text": variable_row[2 + i * 3 + 1],
                        "source": variable_row[2 + i * 3 + 2]
                    }
                    # The variables starts in the 3rd row of datum
                    for j, variable_row in enumerate(datum[2:])
                ]
            }
            for i, institution_name in enumerate(institution_names)
        ]

        return institutions

    def get_spreadsheet_data(self, spreadsheet_id: str) -> SpreadsheetData:
        """
        Get the spreadsheet data given a spreadsheet id.

        Parameters
        ----------
        spreadsheet_id: str
            The id of the spreadsheet.

        Returns
        ----------
        spreadsheet_data: SpreadSheetData
            The spreadsheet data. Please the SpreadsheetData class to view its attributes.
        """
        # Get an A1Notation for each sheet's meta data
        meta_data_a1_notations = self._get_meta_data_a1_notations(spreadsheet_id)
        # Get the meta data for each sheet
        meta_data_response = self.spreadsheets.values().batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=[
                self._construct_a1_notation(a1_notation) for a1_notation in meta_data_a1_notations
            ],
            majorDimension="COLUMNS"
        ).execute()
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
                end_column=meta_datum.get("end_column")
            )
            for i, meta_datum in enumerate(meta_data)
        ]
        # Get data within a range (specified by an a1 notation) for each sheet
        data_response = self.spreadsheets.values().batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=[
                self._construct_a1_notation(a1_notation) for a1_notation in bounding_box_a1_notations
            ],
            majorDimension="ROWS"
        ).execute()
        data = [value_range.get("values") for value_range in data_response.get("valueRanges")]

        return SpreadsheetData(
            sheet_titles=[a1_notation.sheet_title for a1_notation in meta_data_a1_notations],
            meta_data=meta_data,
            data=data
        )

    def get_spreadsheet_ids(self, master_spreadsheet_id: str) -> List[str]:
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
        return [row[0] for row in spreadsheet_data.data[0]]

    def process_sheet_data(
        self,
        sheet_title: str,
        meta_datum: Dict[str, str],
        datum: List[List[str]]
    ) -> List:
        """ 
        TODO
        Process a sheet to get its data in a format ready to consumed by DB.

        Parameters
        ----------
        sheet_title: str
            The title of a sheet.
        meta_datum: Dict[str, str]
            The meta datum of a sheet.
        datum: List[List[str]]
            The datum of a sheet.

        Returns
        ----------
        data: List
            The data in reqired format. TODO
        """
        google_sheets_format = meta_datum.get("format")
        if google_sheets_format == institution_extracters_constants.GoogleSheetsFormat.institution_by_triples:
            return self._get_institution_by_triples(sheet_title, meta_datum, datum)
        elif google_sheets_format == institution_extracters_constants.GoogleSheetsFormat.institution_by_rows:
            return self._get_institution_by_rows(sheet_title, meta_datum, datum)
        else:
            pass
            # raise exceptions.UnrecognizedGoogleSheetsFormat(sheet_title, google_sheets_format)

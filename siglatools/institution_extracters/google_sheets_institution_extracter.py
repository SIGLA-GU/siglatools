#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Union

from google.oauth2 import service_account
from googleapiclient.discovery import build

from ..databases.constants import DatabaseCollection, VariableType
from . import exceptions
from .constants import GoogleSheetsFormat as gs_format
from .utils import FormattedSheetData, SheetData

###############################################################################

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)4s: %(module)s:%(lineno)4s %(asctime)s] %(message)s",
)
log = logging.getLogger(__name__)

GOOGLE_API_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
COMPOSITE_VARIABLE_HEADINGS = {
    "Constitutional Rights": DatabaseCollection.rights,
    "Constitutional Amendments": DatabaseCollection.amendments,
    "Legal Framework": DatabaseCollection.legal_framework,
}

###############################################################################


def _get_composite_variable(
    sheet_data: SheetData,
) -> List[Dict[str, Union[int, List[Dict[str, str]]]]]:
    """
    Get the constitutional rights belonging to an Constitutions institution.

    Parameters
    ----------
    sheet_data: SheetData
        The sheet data containing the constitutional rights.

    Returns
    ----------
    rights: List[Dict[str, Union[int, str]]]
        A list of constitutional rights.

    """
    column_names = sheet_data.data[0]
    rights = [
        {
            "index": i,
            "sigla_answers": [
                {"name": column_name, "sigla_answer": row[j]}
                for j, column_name in enumerate(column_names)
            ],
        }
        for i, row in enumerate(sheet_data.data[1:])
    ]
    return rights


def _get_institution_by_rows(
    sheet_data: SheetData,
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
            "childs": [
                {
                    # The headings are found in the 1st row of data
                    # The indices of institution_row are 1-more than the indices of variable_names
                    "type": "standard",
                    "heading": sheet_data.data[0][j * 2 + 1],
                    "name": variable_name,
                    "sigla_answer": institution_row[j * 2 + 1],
                    "source": institution_row[j * 2 + 2],
                    "index": j,
                }
                for j, variable_name in enumerate(variable_names)
            ],
        }
        # The institutions starts in the 3rd row of data
        for institution_row in sheet_data.data[2:]
    ]

    log.info(
        f"Found {len(institutions)} institutions from sheet {sheet_data.sheet_title}"
    )
    return institutions


def _get_institution_by_triples(
    sheet_data: SheetData,
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

    # Get the institution names from the first row of data
    institution_names = [
        institution_name for institution_name in sheet_data.data[0] if institution_name
    ]
    institutions = [
        {
            "name": institution_name,
            "category": sheet_data.meta_data.get("category"),
            "country": sheet_data.meta_data.get("country"),
            "childs": [
                {
                    "heading": variable_row[0],
                    "name": variable_row[1],
                    "sigla_answer": variable_row[2 + i * 3],
                    "orig_text": variable_row[2 + i * 3 + 1],
                    "source": variable_row[2 + i * 3 + 2],
                    "index": j,
                }
                # The variables starts in the 3rd row of data
                for j, variable_row in enumerate(sheet_data.data[2:])
            ],
        }
        for i, institution_name in enumerate(institution_names)
    ]
    for institution in institutions:
        for variable in institution.get("childs"):
            if variable.get("heading").strip() in COMPOSITE_VARIABLE_HEADINGS:
                variable["type"] = VariableType.composite
                variable["hyperlink"] = COMPOSITE_VARIABLE_HEADINGS.get(
                    variable.get("heading").strip()
                )
            else:
                variable["type"] = VariableType.standard

    log.info(
        f"Found {len(institutions)} institutions from sheet {sheet_data.sheet_title}"
    )
    return institutions


class A1Notation(NamedTuple):
    """
    A1 notation refers to a group of cells within a bounding rectangle in a sheet.
    This doesn't capture all possible A1 notations because start_row and end_row are required,
    but they don't have to be.

    Attributes:
        sheet_title: str
            The title of the sheet that contains the a group of cells.
        start_row: int
            The top row boundary of a group of cells.
        end_row: int
            The bottom row boundary of a group of cells.
        start_column: Optional[str] = None
            The left column boundary of a group of cells.
        end_column: Optional[str] = None
            The right column boundary of a group of cells
    """

    sheet_title: str
    start_row: int
    end_row: int
    start_column: Optional[str] = None
    end_column: Optional[str] = None

    def __str__(self) -> str:
        """
        Returns str representation of the a1 notation.
        """
        if self.start_column is not None and self.end_column is not None:
            return f"{self.sheet_title}!{self.start_column}{self.start_row}:{self.end_column}{self.end_row}"
        else:
            return f"{self.sheet_title}!{self.start_row}:{self.end_row}"


class GoogleSheetsInstitutionExtracter:
    google_sheets_format_to_function_dict = {
        gs_format.institution_by_triples: _get_institution_by_triples,
        gs_format.institution_by_rows: _get_institution_by_rows,
        gs_format.institution_and_composite_variable: _get_composite_variable,
        gs_format.composite_variable: _get_composite_variable,
    }

    def __init__(self, credentials_path: str):
        credentials_path = Path(credentials_path).resolve(strict=True)
        self._credentials_path = str(credentials_path)
        # Creates a Credentials instance from a service account json file.
        credentials = service_account.Credentials.from_service_account_file(
            self._credentials_path, scopes=GOOGLE_API_SCOPES
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
            elif len(a1_notation.start_column) == len(a1_notation.end_column):
                if a1_notation.start_column > a1_notation.end_column:
                    # Start column is greater than end column
                    raise exceptions.InvalidRangeInA1Notation(
                        a1_notation.sheet_title,
                        a1_notation.start_column,
                        a1_notation.end_column,
                    )
                else:
                    # Start column is less than or equal to end column
                    return str(a1_notation)
            else:
                # Length of start column is less than end column
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
        # Get the spreadsheet title
        spreadsheet_response = self.spreadsheets.get(
            spreadsheetId=spreadsheet_id
        ).execute()
        spreadsheet_title = spreadsheet_response.get("properties").get("title")
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

        log.info(f"Finished extracting spreadsheet {spreadsheet_title}")
        log.info(f"Found {len(meta_data)} sheets in spreadsheet {spreadsheet_title}")
        return [
            SheetData(
                sheet_title=a1_notation.sheet_title,
                meta_data=meta_data[i],
                data=data[i],
            )
            for i, a1_notation in enumerate(meta_data_a1_notations)
        ]

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

    @staticmethod
    def process_sheet_data(sheet_data: SheetData) -> FormattedSheetData:
        """
        Process a sheet to get its data in a format ready to consumed by DB.

        Parameters
        ----------
        sheet_data: SheetData
            The data of the sheet.

        Returns
        ----------
        formatted_sheet_data: FormattedSheetData
            The data in reqired format.
        """
        formatted_data = None
        get_institution_key = sheet_data.meta_data.get("format")

        if (
            get_institution_key
            in GoogleSheetsInstitutionExtracter.google_sheets_format_to_function_dict
        ):
            get_institution_key_function = GoogleSheetsInstitutionExtracter.google_sheets_format_to_function_dict[
                get_institution_key
            ]
            formatted_data = get_institution_key_function(sheet_data)
        else:
            raise exceptions.UnrecognizedGoogleSheetsFormat(
                sheet_data.sheet_title,
                sheet_data.meta_data.get("format"),
                sheet_data.meta_data.get("data_type"),
            )

        return FormattedSheetData(
            sheet_title=sheet_data.sheet_title,
            meta_data=sheet_data.meta_data,
            formatted_data=formatted_data,
        )

    def __str__(self):
        return f"<GoogleSheetsInstitutionExtracter [{self._credentials_path}]>"

    def __repr__(self):
        return str(self)

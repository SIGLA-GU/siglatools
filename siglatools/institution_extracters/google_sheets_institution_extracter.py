#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Union, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ..databases.constants import (
    CompositeVariableField,
    InstitutionField,
    SiglaAnswerField,
    VariableField,
    VariableType,
)
from ..utils.exceptions import ErrorInfo
from . import exceptions
from .constants import GoogleSheetsFormat as gs_format
from .constants import GoogleSheetsInfoField, MetaDataField
from .utils import (
    FormattedSheetData,
    SheetData,
    create_institution_sub_category,
)

###############################################################################

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)4s: %(module)s:%(lineno)4s %(asctime)s] %(message)s",
)
log = logging.getLogger(__name__)

GOOGLE_API_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

###############################################################################


def _get_composite_variable(
    sheet_data: SheetData,
) -> List[Dict[str, Union[int, List[Dict[str, str]]]]]:
    """
    Get the composite variable from a SheetData.

    Parameters
    ----------
    sheet_data: SheetData
        The sheet data containing the composite variable.

    Returns
    -------
    composite_variable: List[Dict[str, Union[int, List[Dict[str, str]]]]]
        A list of individual variables of a composite varible.
    """
    column_names = sheet_data.data[0]
    composite_variable = [
        {
            CompositeVariableField.index: i,
            CompositeVariableField.sigla_answers: [
                {SiglaAnswerField.name: column_name, SiglaAnswerField.answer: row[j]}
                for j, column_name in enumerate(column_names)
            ],
        }
        for i, row in enumerate(sheet_data.data[1:])
    ]

    log.info(
        f"Found composite variable: {sheet_data.meta_data.get('variable_heading')} of length {len(composite_variable)} "
        f"from sheet: {sheet_data.sheet_title}"
    )
    return composite_variable


def _get_multilple_sigla_answer_variable(
    sheet_data: SheetData,
) -> List[Dict[str, Union[str, List[Dict[str, Union[int, str]]]]]]:
    """
    Get the list of institutions and their variables from a sheet.

    Parameters
    ----------
    sheet_data: SheetData
        The data of a sheet.

    Returns
    -------
    institutions: List[Dict[str, Union[str, List[Dict[str, Union[int, str]]]]]]
        The list of institutions and their variables
    """
    try:
        institution = {
            InstitutionField.spreadsheet_id: sheet_data.spreadsheet_id,
            InstitutionField.sheet_id: sheet_data.sheet_id,
            InstitutionField.name: sheet_data.meta_data.get(InstitutionField.name),
            InstitutionField.country: sheet_data.meta_data.get(
                InstitutionField.country
            ),
            InstitutionField.category: sheet_data.meta_data.get(
                InstitutionField.category
            ),
            InstitutionField.sub_category: create_institution_sub_category(
                sheet_data.meta_data.get(InstitutionField.sub_category)
            ),
            "childs": [
                {
                    VariableField.heading: variable_row[0],
                    VariableField.name: variable_row[1],
                    VariableField.sigla_answer: variable_row[2],
                    VariableField.orig_text: variable_row[3],
                    VariableField.source: variable_row[4],
                    VariableField.variable_index: i,
                    VariableField.type: VariableType.standard,
                }
                for i, variable_row in enumerate(sheet_data.data[1:])
            ],
        }
        log.info(f"Found 1 institution from {sheet_data.sheet_title}")
        return [institution]
    except IndexError:
        log.info("=*80")
        log.error(sheet_data.sheet_title)


def _get_standard_institution(
    sheet_data: SheetData,
) -> List[Dict[str, Union[str, List[Dict[str, Union[int, str]]]]]]:
    """
    Get the list of institutions and their variables from a sheet.

    Parameters
    ----------
    sheet_data: SheetData
        The data of the sheet.

    Returns
    -------
    institutions: List[Dict[str, Union[str, List[Dict[str, Union[int, str]]]]]]
        The list of institutions and their variables.
    """

    # Get the institution names from the first row of data
    institution_names = [
        institution_name for institution_name in sheet_data.data[0] if institution_name
    ]
    institutions = [
        {
            InstitutionField.spreadsheet_id: sheet_data.spreadsheet_id,
            InstitutionField.sheet_id: sheet_data.sheet_id,
            InstitutionField.name: institution_name,
            InstitutionField.category: sheet_data.meta_data.get(
                InstitutionField.category
            ),
            InstitutionField.sub_category: create_institution_sub_category(
                sheet_data.meta_data.get(InstitutionField.sub_category)
            ),
            "childs": [
                {
                    VariableField.heading: variable_row[0],
                    VariableField.name: variable_row[1],
                    VariableField.sigla_answer: variable_row[2 + i * 3],
                    VariableField.orig_text: variable_row[2 + i * 3 + 1],
                    VariableField.source: variable_row[2 + i * 3 + 2],
                    VariableField.variable_index: j,
                    VariableField.type: VariableType.standard,
                }
                # The variables starts in the 3rd row of data
                for j, variable_row in enumerate(sheet_data.data[2:])
            ],
        }
        for i, institution_name in enumerate(institution_names)
    ]

    has_country = InstitutionField.country in sheet_data.meta_data
    for institution in institutions:
        if has_country:
            institution[InstitutionField.country] = sheet_data.meta_data.get(
                InstitutionField.country
            )

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
        sheet_id: str
            The id of the sheet that contains a group cells
        sheet_title: str
            The title of the sheet that contains a group of cells.
        start_row: int
            The top row boundary of a group of cells.
        end_row: int
            The bottom row boundary of a group of cells.
        start_column: Optional[str] = None
            The left column boundary of a group of cells.
        end_column: Optional[str] = None
            The right column boundary of a group of cells
    """

    sheet_id: str
    sheet_title: str
    start_row: int
    end_row: int
    start_column: Optional[str] = None
    end_column: Optional[str] = None

    def raise_for_validity(self) -> None:
        """
        Raise an error if the a1 notation is invalid.
        https://developers.google.com/sheets/api/guides/concepts#a1_notation

        For a description of an A1 notation, please view the A1Notation class atttributes.
        """

        if int(self.start_row) > int(self.end_row):
            # Start row is greater than end row
            raise exceptions.InvalidRangeInA1Notation(
                ErrorInfo(
                    {
                        GoogleSheetsInfoField.sheet_title: self.sheet_title,
                        MetaDataField.start_row: self.start_row,
                        MetaDataField.end_row: self.end_row,
                    }
                )
            )
        elif self.start_column is not None and self.end_column is not None:
            # Start and end column are both present
            if len(self.start_column) > len(self.end_column):
                # Length of start column is greater than length end column
                raise exceptions.InvalidRangeInA1Notation(
                    ErrorInfo(
                        {
                            GoogleSheetsInfoField.sheet_title: self.sheet_title,
                            MetaDataField.start_column: self.start_column,
                            MetaDataField.end_column: self.end_column,
                        }
                    )
                )
            elif len(self.start_column) == len(self.end_column):
                if self.start_column > self.end_column:
                    # Start column is greater than end column
                    raise exceptions.InvalidRangeInA1Notation(
                        ErrorInfo(
                            {
                                GoogleSheetsInfoField.sheet_title: self.sheet_title,
                                MetaDataField.start_column: self.start_column,
                                MetaDataField.end_column: self.end_column,
                            }
                        )
                    )
        elif any(field is not None for field in [self.start_column, self.end_column]):
            # Either start column or end column is present
            raise exceptions.IncompleteColumnRangeInA1Notation(
                ErrorInfo(
                    {
                        GoogleSheetsInfoField.sheet_title: self.sheet_title,
                        MetaDataField.start_column: self.start_column,
                        MetaDataField.end_column: self.end_column,
                    }
                )
            )

    def __str__(self) -> str:
        """
        Returns str representation of the a1 notation.
        """
        if self.start_column is not None and self.end_column is not None:
            return f"'{self.sheet_title}'!{self.start_column}{self.start_row}:{self.end_column}{self.end_row}"
        else:
            return f"'{self.sheet_title}'!{self.start_row}:{self.end_row}"


class GoogleSheetsInstitutionExtracter:
    google_sheets_format_to_function_dict = {
        gs_format.standard_institution: _get_standard_institution,
        gs_format.institution_and_composite_variable: _get_composite_variable,
        gs_format.composite_variable: _get_composite_variable,
        gs_format.multiple_sigla_answer_variable: _get_multilple_sigla_answer_variable,
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

    def _get_spreadsheet(self, spreadsheet_id: str) -> Any:
        """
        Get the spreadsheet from a spreadsheet_id

        Parameters
        ----------
        spreadsheet_id: str
            The id of the spreadsheet

        Returns
        -------
        spreadsheet: The spreadsheet.
        See https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets#Spreadsheet.

        """
        # Get the spreadsheet
        return self.spreadsheets.get(spreadsheetId=spreadsheet_id).execute()

    def _get_spreadsheet_tile(self, spreadsheet: Any) -> str:
        """Get the title of a spreadsheet"""
        return spreadsheet.get("properties").get("title")

    def _get_meta_data_a1_notations(
        self,
        spreadsheet: Any,
    ) -> List[A1Notation]:
        """
        Construct an A1Notation for each sheet from its first two rows of meta data

        Parameters
        ----------
        spreadsheet: str
            The spreadsheet object.
            See https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets#Spreadsheet

        Returns
        -------
        a1_notations: List[A1Notatoin]
            The list of A1Notations, one for each sheet.
        """
        # Create an A1Notation for each sheet's meta data
        return [
            A1Notation(
                sheet_id=sheet.get("properties").get("sheetId"),
                sheet_title=sheet.get("properties").get("title"),
                start_row=1,
                end_row=2,
            )
            for sheet in spreadsheet.get("sheets")
        ]

    def _get_meta_data(
        self, spreadsheet_id: str, a1_notations: List[A1Notation]
    ) -> List[Dict[str, str]]:
        "Get the rows specified by the a1 notations"
        # Get the meta data for each sheet
        meta_data_response = (
            self.spreadsheets.values()
            .batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=[str(a1_notation) for a1_notation in a1_notations],
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

        return meta_data

    def _get_data_a1_notations(
        self, a1_notations: List[A1Notation], meta_data: List[Dict[str, str]]
    ) -> List[A1Notation]:
        # Use the meta datum to create an a1 notation to get the datum of each sheet
        bounding_box_a1_notations = [
            A1Notation(
                sheet_id=a1_notations[i].sheet_id,
                sheet_title=a1_notations[i].sheet_title,
                start_row=int(meta_datum.get(MetaDataField.start_row)),
                end_row=int(meta_datum.get(MetaDataField.end_row)),
                start_column=meta_datum.get(MetaDataField.start_column),
                end_column=meta_datum.get(MetaDataField.end_column),
            )
            for i, meta_datum in enumerate(meta_data)
        ]

        for a1_anotation in bounding_box_a1_notations:
            a1_anotation.raise_for_validity()

        return bounding_box_a1_notations

    def _get_data(
        self, spreadsheet_id: str, a1_notations: List[A1Notation]
    ) -> List[List[List[Any]]]:
        data_response = (
            self.spreadsheets.values()
            .batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=[str(a1_notation) for a1_notation in a1_notations],
                majorDimension="ROWS",
            )
            .execute()
        )
        data = [
            value_range.get("values")
            for value_range in data_response.get("valueRanges")
        ]

        return data

    def _get_next_uv_dates_a1_annotations(
        self, a1_notations: List[A1Notation], meta_data: List[Dict[str, str]]
    ) -> List[A1Notation]:
        # Create a1 notations to get next uv dates
        next_uv_date_a1_notations = [
            A1Notation(
                sheet_id=a1_notations[i].sheet_id,
                sheet_title=a1_notations[i].sheet_title,
                start_row=int(meta_datum.get(MetaDataField.start_row)),
                end_row=int(meta_datum.get(MetaDataField.end_row)),
                start_column=meta_datum.get(MetaDataField.date_of_next_uv_column),
                end_column=meta_datum.get(MetaDataField.date_of_next_uv_column),
            )
            for i, meta_datum in enumerate(meta_data)
            if meta_datum.get(MetaDataField.date_of_next_uv_column) is not None
        ]
        for a1_notation in next_uv_date_a1_notations:
            a1_notation.raise_for_validity()
        return next_uv_date_a1_notations

    def _get_next_uv_dates_data(
        self, spreadsheet_id: str, a1_notations: List[A1Notation]
    ) -> List[List[Any]]:
        next_uv_date_response = (
            self.spreadsheets.values()
            .batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=[str(a1_notation) for a1_notation in a1_notations],
                majorDimension="COLUMNS",
            )
            .execute()
        )
        next_uv_date_data = [
            value_range.get("values")[0]
            for value_range in next_uv_date_response.get("valueRanges") or []
        ]
        return next_uv_date_data

    def get_spreadsheet_data(self, spreadsheet_id: str) -> List[SheetData]:
        """
        Get the spreadsheet data given a spreadsheet id.

        Parameters
        ----------
        spreadsheet_id: str
            The id of the spreadsheet.

        Returns
        -------
        spreadsheet_data: List[SheetData]
            The spreadsheet data. Please the SheetData class to view its attributes.
        """
        try:
            spreadsheet = self._get_spreadsheet(spreadsheet_id=spreadsheet_id)
            # Get an A1Notation for each sheet's meta data
            meta_data_a1_notations = self._get_meta_data_a1_notations(
                spreadsheet=spreadsheet
            )
            # Get the spreadsheet title
            spreadsheet_title = self._get_spreadsheet_tile(spreadsheet=spreadsheet)
            # Get the meta data for each sheet
            meta_data = self._get_meta_data(
                spreadsheet_id=spreadsheet_id,
                a1_notations=meta_data_a1_notations,
            )
            # Use the meta datum to create an a1 notation to get the datum of each sheet
            bounding_box_a1_notations = self._get_data_a1_notations(
                a1_notations=meta_data_a1_notations,
                meta_data=meta_data,
            )
            # Get data within a range (specified by an a1 notation) for each sheet
            data = self._get_data(
                spreadsheet_id=spreadsheet_id,
                a1_notations=bounding_box_a1_notations,
            )

            # Create a1 notations to get next uv dates
            next_uv_date_a1_notations = self._get_next_uv_dates_a1_annotations(
                a1_notations=meta_data_a1_notations,
                meta_data=meta_data,
            )
            # Get the next uv dates
            next_uv_date_data = self._get_next_uv_dates_data(
                spreadsheet_id=spreadsheet_id, a1_notations=next_uv_date_a1_notations
            )
        except HttpError as http_error:
            raise exceptions.UnableToAccessSpreadsheet(
                ErrorInfo(
                    {
                        GoogleSheetsInfoField.spreadsheet_title: spreadsheet_title,
                        "reason": f"{http_error}",
                    }
                )
            )

        next_uv_date_data_iter = iter(next_uv_date_data)

        log.info(f"Finished extracting spreadsheet {spreadsheet_title}")
        log.info(f"Found {len(meta_data)} sheets in spreadsheet {spreadsheet_title}")
        return [
            SheetData(
                spreadsheet_id=spreadsheet_id,
                spreadsheet_title=spreadsheet_title,
                sheet_id=a1_notation.sheet_id,
                sheet_title=a1_notation.sheet_title,
                meta_data=meta_data[i],
                data=data[i],
                next_uv_dates=(
                    next(next_uv_date_data_iter)
                    if meta_data[i].get(MetaDataField.date_of_next_uv_column)
                    is not None
                    else None
                ),
            )
            for i, a1_notation in enumerate(meta_data_a1_notations)
        ]

    def get_spreadsheet_ids(self, master_spreadsheet_id: str) -> List[str]:
        """
        Get the list of spreadsheet ids from a master spreadsheet.

        Parameters
        ----------
        master_spreadsheet_id: str
            The id of the master spreadsheet.

        Returns
        -------
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
        -------
        formatted_sheet_data: FormattedSheetData
            The data in reqired format.
        """
        formatted_data = None
        get_data_key = sheet_data.meta_data.get(MetaDataField.format)

        if (
            get_data_key
            in GoogleSheetsInstitutionExtracter.google_sheets_format_to_function_dict
        ):
            get_data_function = (
                GoogleSheetsInstitutionExtracter.google_sheets_format_to_function_dict[
                    get_data_key
                ]
            )
            try:
                formatted_data = get_data_function(sheet_data)
            except Exception:
                raise exceptions.UnableToCreateFormattedSheetData(
                    ErrorInfo(
                        {
                            GoogleSheetsInfoField.spreadsheet_title: sheet_data.spreadsheet_title,
                            GoogleSheetsInfoField.sheet_title: sheet_data.sheet_title,
                        }
                    )
                )
        else:
            raise exceptions.UnrecognizedGoogleSheetsFormat(
                ErrorInfo(
                    {
                        GoogleSheetsInfoField.spreadsheet_title: sheet_data.spreadsheet_title,
                        GoogleSheetsInfoField.sheet_title: sheet_data.sheet_title,
                        MetaDataField.format: sheet_data.meta_data.get(
                            MetaDataField.format
                        ),
                        MetaDataField.data_type: sheet_data.meta_data.get(
                            MetaDataField.data_type
                        ),
                    }
                )
            )

        return FormattedSheetData(
            spreadsheet_id=sheet_data.spreadsheet_id,
            spreadsheet_title=sheet_data.spreadsheet_title,
            sheet_id=sheet_data.sheet_id,
            sheet_title=sheet_data.sheet_title,
            meta_data=sheet_data.meta_data,
            formatted_data=formatted_data,
        )

    def __str__(self):
        return f"<GoogleSheetsInstitutionExtracter [{self._credentials_path}]>"

    def __repr__(self):
        return str(self)

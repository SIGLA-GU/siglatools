#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Dict, List, NamedTuple


class SheetData(NamedTuple):
    """
    The extracted data from a Google Sheet.

    Attributes:
        spreadsheet_title: str
            The title of spreadsheet that contains the sheet.
        sheet_title: str
            The title of the sheet.
        meta_data: Dict[str, str]
            The meta data of the sheet, found in the first two rows.
        data: List[List[str]]
            The data of the sheet.
    """

    spreadsheet_title: str
    sheet_title: str
    meta_data: Dict[str, str]
    data: List[List[str]]


class FormattedSheetData(NamedTuple):
    """
    The formatted data from a Google Sheet.

    Attributes:
        spreadsheet_title: str
            The title of the spreadsheet that contains the sheet.
        sheet_title: str
            The title of the sheet.
        meta_data: Dict[str, str]
            The meta data of the sheet, found in the first two rows.
        formatted_data: List
            The formatted data of the sheet.
    """

    spreadsheet_title: str
    sheet_title: str
    meta_data: Dict[str, str]
    formatted_data: List


def convert_rowcol_to_A1_name(row: int, col: int) -> str:
    """
    Converts row and col to an A1 name.

    Parameters
    ----------
    row: int
        The row number.
    col: int
        The column number.

    Returns
    -------
    A1_cell: str
        The A1 cell str.

    """
    col_str = convert_col_to_name(col)

    return col_str + str(row + 1)


def convert_col_to_name(col: int) -> str:
    """
    Convert a zero indexed column cell reference to a string.

    Parameters
    ----------
    col: int
        The cell column.
    Returns
    -------
    col_str: str
        The column style string.
    """
    col_num = col

    col_num += 1  # Change to 1-index.
    col_str = ""

    while col_num:
        # Set remainder from 1 .. 26
        remainder = col_num % 26

        if remainder == 0:
            remainder = 26

        # Convert the remainder to a character.
        col_letter = chr(ord("A") + remainder - 1)

        # Accumulate the column letters, right to left.
        col_str = col_letter + col_str

        # Get the next order of magnitude.
        col_num = int((col_num - 1) / 26)

    return col_str

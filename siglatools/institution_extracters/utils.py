#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Dict, List, NamedTuple


class SheetData(NamedTuple):
    """
    The extracted data from a Google Sheet.

    Attributes:
        sheet_title: str
            The title of the sheet.
        meta_data: Dict[str, str]
            The meta data of the sheet, found in the first two rows.
        data: List[List[str]]
            The data of the sheet.
    """

    sheet_title: str
    meta_data: Dict[str, str]
    data: List[List[str]]


class FormattedSheetData(NamedTuple):
    """
    The formatted data from a Google Sheet.

    Attributes:
        sheet_title: str
            The title of the sheet.
        meta_data: Dict[str, str]
            The meta data of the sheet, found in the first two rows.
        formatted_data: List
            The formatted data of the sheet.
    """

    sheet_title: str
    meta_data: Dict[str, str]
    formatted_data: List

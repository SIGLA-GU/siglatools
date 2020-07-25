#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Union


class IncompleteColumnRangeInA1Notation(Exception):
    def __init__(self, sheet_title: str, start_column: str, end_column: int, **kwargs):
        super().__init__(**kwargs)
        self.sheet_title = sheet_title
        self.start_column = start_column
        self.end_column = end_column

    def __str__(self):
        return (
            f"In {self.sheet_title}, mising either the start_column value or end_column value. \
                Both values must be present or not present together. "
            f"Received start_row: {self.start_column}, end_row: {self.end_column}"
        )


class InvalidRangeInA1Notation(Exception):
    def __init__(
        self, sheet_title: str, start: Union[int, str], end: Union[int, str], **kwargs
    ):
        super().__init__(**kwargs)
        self.sheet_title = sheet_title
        self.start = start
        self.end = end

    def __str__(self):
        return (
            f"In {self.sheet_title}, the start value is greater than the end value. "
            f"Received start: {self.start}, end: {self.end}"
        )


class UnrecognizedGoogleSheetsFormat(Exception):
    def __init__(self, sheet_title: str, format: str, **kwargs):
        super().__init__(**kwargs)
        self.sheet_title = sheet_title
        self.format = format

    def __str__(self):
        return (
            f"In {self.sheet_title}, the specified format is not a valid Google Sheets format. "
            f"Received format: {self.format}."
        )

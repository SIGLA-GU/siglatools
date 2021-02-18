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
            f"Received start_column: {self.start_column}, end_column: {self.end_column}"
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
    def __init__(
        self, sheet_title: str, google_sheets_format: str, data_type: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.sheet_title = sheet_title
        self.google_sheets_format = google_sheets_format
        self.data_type = data_type

    def __str__(self):
        return (
            f"In {self.sheet_title}, the specified format is not a valid Google Sheets format. "
            f"Received format: {self.google_sheets_format}, data_type: {self.data_type}."
        )


class InvalidDateRange(Exception):
    def __init__(self, start_date: str, end_date: str, **kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date

    def __str__(self):
        return f"InvalidDateRange: The start date {self.start_date} is greater than the end date {self.end_date}."

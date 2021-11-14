#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ..utils.exceptions import BaseError, ErrorInfo


class UnableToAccessSpreadsheet(BaseError):
    def __init__(self, info: ErrorInfo):
        super().__init__("Unable to access spreadsheet.", info)


class IncompleteColumnRangeInA1Notation(BaseError):
    def __init__(self, info: ErrorInfo):
        super().__init__("A1 Notation's start_column or end_column is missing.", info)


class InvalidRangeInA1Notation(BaseError):
    def __init__(self, info: ErrorInfo):
        super().__init__(
            "A1 Notation's start value is greater than its end value.", info
        )


class UnrecognizedGoogleSheetsFormat(BaseError):
    def __init__(self, info: ErrorInfo):
        super().__init__("Invalid Googlesheet format.", info)


class InvalidDateRange(BaseError):
    def __init__(self, info: ErrorInfo):
        super().__init__("Invalid date range.", info)

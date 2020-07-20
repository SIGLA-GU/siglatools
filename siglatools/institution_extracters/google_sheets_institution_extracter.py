#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from typing import NamedTuple, Optional

from . import exceptions
from .institution_extracter import InstitutionExtracter

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


class A1Notation(NamedTuple):
    sheet_id: str
    first_row: Optional[int] = None
    last_row: Optional[int] = None
    first_column: Optional[str] = None
    last_column: Optional[str] = None

    def __str__(self) -> str:
        return f"{self.sheet_id}!{self.first_column}{self.first_row}:{self.last_column}{self.last_row}"


class GoogleSheetsInstitutionExtracter(InstitutionExtracter):
    @staticmethod
    def _construct_a1_notation(a1_notation: A1Notation) -> str:
        """
        Construction an A1 Notation string
        https://developers.google.com/sheets/api/guides/concepts#a1_notation

        For a description of an A1 notation, please view the A1Notation class atttributes.

        Parameters
        ----------
        a1_notation: A1Notation

        Returns
        ----------
        a1_notation: string
        """
        if bool(a1_notation.first_row) != bool(a1_notation.last_row):
            raise exceptions.IncompleteRowRangeInA1Notation(a1_notation.first_row, a1_notation.last_row)
        elif bool(a1_notation.first_column) != bool(a1_notation.last_column):
            raise exceptions.IncompleteColumnRangeInA1Notation(a1_notation.first_column, a1_notation.last_column)
        else:
            return str(a1_notation)

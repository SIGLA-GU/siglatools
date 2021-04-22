#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pytest

from siglatools.institution_extracters import (
    GoogleSheetsInstitutionExtracter,
    exceptions,
)
from siglatools.institution_extracters.google_sheets_institution_extracter import (
    A1Notation,
)


@pytest.mark.parametrize(
    "a1_notation, expected",
    [
        pytest.param(
            A1Notation(sheet_id="0", sheet_title="Sheet1", start_row=2, end_row=1),
            None,
            marks=pytest.mark.raises(exception=exceptions.InvalidRangeInA1Notation),
        ),
        pytest.param(
            A1Notation(
                sheet_id="0",
                sheet_title="Sheet1",
                start_row=1,
                end_row=2,
                start_column="AB",
                end_column="C",
            ),
            None,
            marks=pytest.mark.raises(exception=exceptions.InvalidRangeInA1Notation),
        ),
        pytest.param(
            A1Notation(
                sheet_id="0",
                sheet_title="Sheet1",
                start_row=1,
                end_row=2,
                start_column="B",
                end_column="A",
            ),
            None,
            marks=pytest.mark.raises(exception=exceptions.InvalidRangeInA1Notation),
        ),
        (
            A1Notation(
                sheet_id="0",
                sheet_title="Sheet1",
                start_row=1,
                end_row=2,
                start_column="A",
                end_column="B",
            ),
            "'Sheet1'!A1:B2",
        ),
        (
            A1Notation(
                sheet_id="0",
                sheet_title="Sheet1",
                start_row=1,
                end_row=2,
                start_column="A",
                end_column="AA",
            ),
            "'Sheet1'!A1:AA2",
        ),
        pytest.param(
            A1Notation(
                sheet_id="0",
                sheet_title="Sheet1",
                start_row=1,
                end_row=2,
                start_column="A",
            ),
            None,
            marks=pytest.mark.raises(
                exception=exceptions.IncompleteColumnRangeInA1Notation
            ),
        ),
        (
            A1Notation(sheet_id="0", sheet_title="Sheet1", start_row=1, end_row=2),
            "'Sheet1'!1:2",
        ),
    ],
)
def test_construct_a1_notation(a1_notation, expected):
    assert (
        GoogleSheetsInstitutionExtracter._construct_a1_notation(a1_notation) == expected
    )

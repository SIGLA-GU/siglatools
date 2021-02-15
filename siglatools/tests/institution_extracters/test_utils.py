#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pytest

from siglatools.institution_extracters.utils import (
    convert_col_to_name,
    convert_rowcol_to_A1_name,
)


@pytest.mark.parametrize(
    "col, expected",
    [
        (0, "A"),
        (1, "B"),
        (2, "C"),
        (9, "J"),
        (24, "Y"),
        (25, "Z"),
        (26, "AA"),
        (27, "AB"),
        (254, "IU"),
        (255, "IV"),
        (256, "IW"),
        (16383, "XFD"),
        (16384, "XFE"),
    ],
)
def test_convert_col_to_name(col, expected):
    assert convert_col_to_name(col) == expected


@pytest.mark.parametrize(
    "row, col, expected",
    [
        (0, 0, "A1"),
        (0, 1, "B1"),
        (0, 2, "C1"),
        (0, 9, "J1"),
        (1, 0, "A2"),
        (2, 0, "A3"),
        (9, 0, "A10"),
        (1, 24, "Y2"),
        (7, 25, "Z8"),
        (9, 26, "AA10"),
        (1, 254, "IU2"),
        (1, 255, "IV2"),
        (1, 256, "IW2"),
        (0, 16383, "XFD1"),
        (1048576, 16384, "XFE1048577"),
    ],
)
def test_convert_rowcol_to_A1_name(row, col, expected):
    assert convert_rowcol_to_A1_name(row, col) == expected

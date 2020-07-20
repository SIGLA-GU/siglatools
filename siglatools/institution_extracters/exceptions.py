#!/usr/bin/env python
# -*- coding: utf-8 -*-


class IncompleteRowRangeInA1Notation(Exception):
    def __init__(self, first_row: int, last_row: int, **kwargs):
        super().__init__(**kwargs)
        self.first_row = first_row
        self.last_row = last_row

    def __str__(self):
        return (
            "Mising either the first_row value or last_row value. Both values must be present or not present together.",
            f"Recevied first_row:{self.first_row}, last_row:{self.last_row}"
        )


class IncompleteColumnRangeInA1Notation(Exception):
    def __init__(self, first_column: str, last_column: int, **kwargs):
        super().__init__(**kwargs)
        self.first_column = first_column
        self.last_column = last_column

    def __str__(self):
        return (
            "Mising either the first_column value or last_column value. Both values must be present or not present \
                together.",
            f"Recevied first_row:{self.first_column}, last_row:{self.last_column}"
        )

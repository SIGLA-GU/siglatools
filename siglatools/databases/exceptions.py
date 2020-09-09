#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import List, Dict, Union


class UnableToFindDocument(Exception):
    def __init__(
        self,
        sheet_title: str,
        collection: str,
        primary_keys: List[Dict[str, Union[int, str]]],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sheet_title = sheet_title
        self.collection = collection
        self.primary_keys = primary_keys

    def __str__(self):
        return (
            f"In {self.sheet_title}, unable to find correct documents of {self.collection} "
            f"with primary keys {str(self.primary_keys)}."
        )

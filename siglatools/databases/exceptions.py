#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ..utils.exceptions import BaseError, ErrorInfo


class UnableToFindDocument(BaseError):
    def __init__(self, info: ErrorInfo):
        super().__init__("Unable to find document in database.", info)

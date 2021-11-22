#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ..utils.exceptions import BaseError, ErrorInfo


class PrefectFlowFailure(BaseError):
    def __init__(self, info: ErrorInfo):
        super().__init__("Prefect flow failed.", info)

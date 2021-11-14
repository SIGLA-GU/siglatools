#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Union, Dict


class ErrorInfo:
    def __init__(self, info: Dict[str, Union[str, int]]):
        self.info = info

    def __str__(self):
        items = [f"{k}: {v}" for k, v in self.info.items()]
        return f"""{", ".join(items)}"""


class BaseError(Exception):
    def __init__(self, message: str, info: ErrorInfo):
        super().__init__(message)
        self.message = message
        self.info = info

    def __str__(self):
        return f"{self.message} {str(self.info)}"

    def __reduce__(self):
        return (self.__class__, self.message, self.info)

#!/usr/bin/env python
# -*- coding: utf-8 -*-

from enum import Enum


class DatabaseCollection(Enum):
    institutions = "institutions"
    variables = "variables"
    rights = "rights"
    amendments = "amendments"
    body_of_law = "body_of_law"


class VariableType(Enum):
    standard = "standard"
    composite = "composite"
    aggregate = "aggregate"


class Environment(Enum):
    staging = "staging"
    production = "production"

#!/usr/bin/env python
# -*- coding: utf-8 -*-


class DatabaseCollection:
    institutions = "institutions"
    variables = "variables"
    rights = "rights"
    amendments = "amendments"
    body_of_law = "body_of_law"


class VariableType:
    standard = "standard"
    composite = "composite"
    aggregate = "aggregate"


class Environment:
    staging = "staging"
    production = "production"

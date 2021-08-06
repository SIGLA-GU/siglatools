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


class InstitutionField:
    _id = "_id"
    name = "name"
    country = "country"
    category = "category"
    sub_category = "sub_category"
    sheet_id = "sheet_id"
    spreadsheet_id = "spreadsheet_id"


class VariableField:
    _id = "_id"
    institution = "institution"
    heading = "heading"
    name = "name"
    variable_index = "variable_index"
    sigla_answer ="sigla_answer"
    orig_text = "orig_text"
    source = "source"
    type = "type"
    hyperlink = "hyperlink"

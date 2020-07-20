#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from .database import Database

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


class MongoDBDatabase(Database):
    pass

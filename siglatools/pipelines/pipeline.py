#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from abc import ABC, abstractmethod

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


class Pipeline(ABC):
    @abstractmethod
    def run(self):
        """
        Run the pipeline.
        """
        pass

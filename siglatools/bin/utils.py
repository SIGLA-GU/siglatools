#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import List

from prefect import task


@task
def _flatten_list(outer_list: List[List]) -> List:
    """
    Prefect Task to flatten a 2D list.

    Parameters
    ----------
    outer_list: List[List]
        The list of list.

    Returns
    -------
    flattened_list: List
        The flattened list.
    """
    # Flatten a list of list.
    return [element for inner_list in outer_list for element in inner_list]

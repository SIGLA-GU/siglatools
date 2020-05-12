# -*- coding: utf-8 -*-

"""Top-level package for SIGLA Tools."""

__author__ = "To Huynh"
__email__ = "huynh.nto@gmail.com"
# Do not edit this string manually, always use bumpversion
# Details in CONTRIBUTING.md
__version__ = "0.1.0"


def get_module_version():
    return __version__


from .example import Example  # noqa: F401

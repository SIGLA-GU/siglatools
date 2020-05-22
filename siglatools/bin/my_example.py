#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This sample script will get deployed in the bin directory of the
users' virtualenv when the parent module is installed using pip.
"""

import argparse
import logging
import sys
import traceback

from siglatools import get_module_version

###############################################################################

log = logging.getLogger()
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)

###############################################################################


class Args(argparse.Namespace):
    def __init__(self):
        # Arguments that could be passed in through the command line
        self.debug = False
        #
        self.__parse()

    def __parse(self):
        p = argparse.ArgumentParser(
            prog="run_exmaple", description="A simple example of a bin script"
        )
        p.add_argument(
            "-v",
            "--version",
            action="version",
            version="%(prog)s " + get_module_version(),
        )
        p.add_argument(
            "-ce",
            "--client_email",
            action="store",
            dest="client_email",
            type=str,
            help="The Google API client email",
        )
        p.add_argument(
            "-ci",
            "--client_id",
            action="store",
            dest="client_id",
            type=str,
            help="The Google API client id",
        )
        p.add_argument(
            "-ccu",
            "--client_cert_url",
            action="store",
            dest="client_cert_url",
            type=str,
            help="The Google API client x509 cert url",
        )
        p.add_argument(
            "-pk",
            "--private_key",
            action="store",
            dest="private_key",
            type=str,
            help="The Google API private key",
        )
        p.add_argument(
            "-pki",
            "--private_key_id",
            action="store",
            dest="private_key_id",
            type=str,
            help="The Google API private key id",
        )
        p.add_argument(
            "-pi",
            "--project_id",
            action="store",
            dest="project_id",
            type=str,
            help="The Google API project id",
        )
        p.add_argument(
            "-dbcu",
            "--db_connection_url",
            action="store",
            dest="db_connection_url",
            type=str,
            help="The Database Connection URL",
        )
        p.add_argument(
            "--debug", action="store_true", dest="debug", help=argparse.SUPPRESS
        )
        p.parse_args(namespace=self)


###############################################################################


def main():
    try:
        args = Args()
        dbg = args.debug
        print("running pipeline")
    except Exception as e:
        log.error("=============================================")
        if dbg:
            log.error("\n\n" + traceback.format_exc())
            log.error("=============================================")
        log.error("\n\n" + str(e) + "\n")
        log.error("=============================================")
        sys.exit(1)


###############################################################################
# Allow caller to directly run this module (usually in development scenarios)

if __name__ == "__main__":
    main()

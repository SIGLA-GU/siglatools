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
from collections import namedtuple

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from siglatools import Example, get_module_version

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
            "--debug", action="store_true", dest="debug", help=argparse.SUPPRESS
        )
        p.parse_args(namespace=self)


###############################################################################


def main():
    try:
        args = Args()
        dbg = args.debug
        #print(args.private_key)
        #print("-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCiLEP98PaV67hn\nNGNIMX3uJAnh35JqeX8JWBFb+kTRVk3Q1UVftLbJT7HPdcjLcA5rIpKSqRFAWtcg\nK1K7XtP8b6wF+TDgD/XUaP2ooBdOun+1wHRswXaN/1vDF1IrCPxAklp57O0m2NuE\nqHw5vjmDFLeMYZIKUgbchqQXpFEW+Q5V8EA5vLmIjbeEn9TQ5jAO6kyndVuNrth5\nWCbyFJSyPh6Cbq2JPRvSsFGjF1IQRYQQ5zHlZoctlhbwE3OAcOuuGNej7jw00sxe\n0w3oXW9wxWV+gLASaV/01rowSgDhfJ4oosMYNguJaCVd8HR0t7Jk+AKf/q6EJYYf\nKHWOhXIvAgMBAAECggEAEPowWKvQAgw4yaPG3FPDZVmkJ99yDQUmGEd+Hb0P/5hD\nh4q5e1ZIUmqDk1PhzvEEBBGM+vSJvU0lO0ATJlFm64TnTdgeAT7lKIpLOnV0AIZu\nqzYixLs7cDcc7J9gNz12vEMgsftB18YZCQS4aIpyVozQgE47Qyr+KTpL1bvFJMcy\n9XbC1n+rXGDNeIssLuXLtgEaACj5dQ8LS10a0UiG/e+xV8C7Vgrb2MmvZMKP+PRg\nVFCkvSFdgRJ/RtISP/Xv7LlTGYtlcoKzQ2lfvIi2ijCS3RbHHnU4HiMbEaSVXPo9\n9zs9OPcwQbG+agAS24jBhtHKYpwBz8cOTR1QFTsRyQKBgQDNakfb6qW0rLk0ghCp\nP0Uvxv2zbgx7Mnm+skQJasUK5uE8mryEhus/N4YPlKtqp4eXg3kjufxNVZB82R7R\nuRjeKV3KU2517GKVy/wWjaCPeR+wb7c+mljOaVLXDOpA49xL2GdMdHgWF0qiwmbM\n86SSpxcw5IRgdEE2TlBd4V6JKQKBgQDKG+uk2E8jGwUuGmwE1DC4slxtGQj+4JVm\nNHD2ba6qEltZMPuLus05Nk5gcAWs9RH67fkFNEXguTUR6/hd6sjZTH2EP8U+84bK\nN8vltwWFYXHdXmCCt7PJS9zlhFbZvA6FqeUEPfLV9sa78u0Bkwlrlywqbf6dTHAC\neLS5fwCTlwKBgHoV68WaDYh3i8/YadydfRprU4fcJVDnbBJZ0zQhoCDdngquENNX\neOPmtSf3fXXzQhRcEJiaRokUDL8XMEkHkO8heNvygFlX+DP9u8MPw9jh7WKo0ylD\nBPsRACpOQ7/zbZAqeyKmqmS+zR41GnI/cJW094SYnNDS55tGKl/RvaUZAoGBAMWA\ncU8enn70yaQa59H0NURX7+Ag5dyERRqiRn1aA6Ro2eGX70jFnAS7n+23qqQwvIhV\nAtLmGR8YfdbsnEHSzMEkcUfKNYtl2SNFUThDGN8VsXqc3nz+3W7pdozHPUP69MoD\nkywJCYOzatOB+b9fG4aLLPqtkHHQn2ia4iifBkYzAoGAFXchKUTdp7ITiI211n5C\nA+/t1D3LaH3a0HuKZx5BkpezHvv+NJOCYilJH5BUAVEzgItf1IvFUvkzx9abgm9U\nCOLgiDZJkCDUD6f5yS9i80HZoy81IW276PPwnp47EuJdbr7PsMqh12zyjBYL32tK\njnyfLbMRZdsixsjA/EWNw5I=\n-----END PRIVATE KEY-----\n")
        scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            {
                "type": "service_account",
                "project_id": args.project_id,
                "private_key_id": args.private_key_id,
                "private_key": 	args.private_key,
                "client_email": args.client_email,
                "client_id": args.client_id,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": args.client_cert_url
            }, 
            scope
        )
        #print("wat")
        client = gspread.authorize(creds)
        sh = client.open('Production Templates')
        first = sh.get_worksheet(0)
        data = first.get_all_records()
        print(data)

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

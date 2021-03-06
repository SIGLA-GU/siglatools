# SIGLA Tools

[![Build Status](https://github.com/sigla-gu/siglatools/workflows/Build%20Master/badge.svg)](https://github.com/sigla-gu/siglatools/actions)
[![Documentation](https://github.com/sigla-gu/siglatools/workflows/Documentation/badge.svg)](https://sigla-gu.github.io/siglatools)

Tools to extract SIGLA data from Google Sheets and load into MongoDB. Please see the SIGLA_Data_PROD folder on Google Drive to find documentations on acceptable Google Sheets formats.

---

## Features
* Bin script to run SIGLA ETL pipeline.
* Bin script to run external link checker.
* Bin script to get the next update and verify dates to determine what data needs updating and verifying.
* Bin script to run QA test to compare GoogleSheet data and data in the database.
* GitHub workflows to run bin script semi/automatically on GitHub Actions.

## Installation
**Stable Release:** `pip install siglatools`<br>
**Development Head:** `pip install git+https://github.com/sigla-gu/siglatools.git`

## Documentation
For full package documentation please visit [sigla-gu.github.io/siglatools](https://sigla-gu.github.io/siglatools).

## Development
See [CONTRIBUTING.md](CONTRIBUTING.md) for information related to developing the code.

## The Four Commands You Need To Know
1. `pip install -e .[dev]`

    This will install your package in editable mode with all the required development dependencies (i.e. `tox`).

2. `make build`

    This will run `tox` which will run all your tests in Python 3.8 as well as linting
    your code.

3. `make clean`

    This will clean up various Python and build generated files so that you can ensure that you are working in a clean
    environment.

4. `make docs`

    This will generate and launch a web browser to view the most up-to-date documentation for your Python package.

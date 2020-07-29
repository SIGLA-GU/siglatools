# SIGLA Tools

[![Build Status](https://github.com/tohuynh/siglatools/workflows/Build%20Master/badge.svg)](https://github.com/tohuynh/siglatools/actions)
[![Documentation](https://github.com/tohuynh/siglatools/workflows/Documentation/badge.svg)](https://tohuynh.github.io/siglatools)

Tools to extract SIGLA data from Google Sheets and load into MongoDB.

---

## Features
* Bin script to run SIGLA ETL pipeline
* GitHub workflows to run pipeline semi/automatically on GitHub Actions.

## Quick Start
```
run_sigla_pipeline -msi <master_spreadsheet_id> -dbcu <db_connection_url> -gacp /path/to/google-api-credentials.json
```

## Installation
**Stable Release:** `pip install siglatools`<br>
**Development Head:** `pip install git+https://github.com/tohuynh/siglatools.git`

## Documentation
For full package documentation please visit [tohuynh.github.io/siglatools](https://tohuynh.github.io/siglatools).

## Development
See [CONTRIBUTING.md](CONTRIBUTING.md) for information related to developing the code.

## The Four Commands You Need To Know
1. `pip install -e .[dev]`

    This will install your package in editable mode with all the required development dependencies (i.e. `tox`).

2. `make build`

    This will run `tox` which will run all your tests in both Python 3.6, Python 3.7, and Python 3.8 as well as linting
    your code.

3. `make clean`

    This will clean up various Python and build generated files so that you can ensure that you are working in a clean
    environment.

4. `make docs`

    This will generate and launch a web browser to view the most up-to-date documentation for your Python package.

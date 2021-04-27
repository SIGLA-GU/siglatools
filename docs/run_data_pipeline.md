# Run Data Pipeline

## Command line

1. Install the package with:

    ```bash
    pip install siglatools
    ```

2. Run the following command with the correct configurations.

    ```bash
    run_sigla_pipeline -msi <master_spreadsheet_id> -gacp /path/to/google-api-credentials.json -dbe <db_env> -sdbcu <staging_db_connection_url> -pdbcu <prod_db_connection_url>
    ```

## GitHub Actions (for collaborators+ only) 
1. Visit https://github.com/SIGLA-GU/siglatools/actions.
2. From the list of workflows, select `Manual Run Data Pipeline`.
3. Click on the `Run workflow` dropdown button.
4. Enter the `spreadsheetId` of the main spreadsheet.
5. Enter a database environment, `staging` or `production`. Choose `staging` if you want to load the spreadsheet(s) found in the main spreadsheet of step 4 to the `staging` database. Choose `production` if you want to load the spreadsheet(s) found in the main spreadsheet of step 4 to the `production` database. <em>Only the data in these spreadsheets will be in the database, and nothing else.</em>
6. Click on the green `Run workflow` button to run the workflow.
7. Wait until a new workflow run result shows up or refresh the page, and click on it to see its details.
8. After the script is done loading the spreadsheet(s) to the database, the script runs QA Test and produces a `qa-test-artifact` that contains the result of the QA Test. Please see [Run QA Test](run_qa_test.html) to see documentation on the produced zip file.

## Debugging Data Pipeline Logs
Access the pipeline's logs by opening a specific workflow instance. Then select `run_sigla_pipeline` job (on the left) from the list of jobs. Click the gear icon (on the right) and select `View raw logs`. A successful run of the pipeline is indicated by `Flow run SUCCESS: all reference tasks succeeded` near the end of the logs.

In cases where the pipeline failed, here are potential errors and how to change the PROD Google Sheets to fix them. Search `ERROR` in the logs to find them.

- TypeError (missing 3 required positional arguments: sheet_title, start_row, and end_row): In the metadata section of a Google Sheet, make sure start_row and end_row have values.
- IncompleteColumnRangeInA1Notation: In the metadata section of a Google Sheet, make sure start_column and end_column have values.
- InvalidRangeInA1Notation: In the metadata section of a Google Sheet, make sure start_column is less than end_column and start_row is less than end_row.
- UnrecognizedGoogleSheetsFormat: In the metadata section of a Google Sheet, make sure the format is one of the accepted Google Sheets Format.
    - standard-institution
    - institution-and-composite-variable
    - composite-variable
    - multiple-sigla-answer-variable
- UnableToFindDocument: In the metadata section of a Google Sheet, make sure the variable_heading and variable_name exactly matches the variable heading and variable name of the corresponding Google Sheet's variable row. For example, in the Constitutional Rights sheet's metadata section, the variable_heading and variable_name must match the variable heading and variable name of the accompanying Google Sheet's rights variable row.
- IndexError: There's probably an illegal non-empty cell in the data section of a Google Sheet.
- HttpError 429: The number of Google Sheets read operations has been exhausted. Maybe wait 15-30 minutes and re-run the pipeline by using the re-run workflow button.

# Run Data Pipeline

## Command line

1. Install the package with:

    ```bash
    pip install siglatools
    ```

2. Run the following command with the correct configurations.

    ```bash
    run_sigla_pipeline -msi <master_spreadsheet_id> -dbcu <db_connection_url> -gacp /path/to/google-api-credentials.json
    ```

## GitHub Actions (for collaborators+ only) 

1. Visit https://github.com/SIGLA-GU/siglatools/issues/new/choose
2. Get started with Run Staging Pipeline to run the staging pipeline, or with Run Production Pipeline to run the production pipeline
3. Add an appropriate title and in the body of the issue write about the key changes to the data
4. Click Submit New Issue
5. Go to the [Actions](https://github.com/SIGLA-GU/siglatools/actions) tab and see the pipeline in progress. You can narrow down the workflows by choosing the type of workflows on the left side, either Manual Run Staging Pipepline or Manual Run Production Pipeline. Then find the specific workflow by finding the GitHub issue title in step 3. Click on the workflow to view its details. On the left side, you should see a check beside `authorization_and_correct_label`. And if the pipeline ran succesfully then you should also see a check next to `pipeline`.

## Debugging Data Pipeline Logs
Access the pipeline's logs by opening a specific workflow (follow similar instructions in the above step 5) and then on the right side click `...` and select `View raw logs`. A successful run of the pipeline is indicated by `Flow run SUCCESS: all reference tasks succeeded` near the end of the logs.

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

# Run Data(subset) Pipeline

## Command line

1. Install the package with:

    ```bash
    pip install siglatools
    ```

2. Run the following command with the correct configurations.

    ```bash
    load_spreadsheets -gacp /path/to/google-api-credentials.json -ssi <spreadsheet_ids> -dbe <db_env> -sdbcu <staging_db_connection_url> -pdbcu <prod_db_connection_url>
    ```

## GitHub Actions (for collaborators+ only) 

1. Visit https://github.com/SIGLA-GU/siglatools/actions.
2. From the list of workflows, select `Manual Run Data(subset) Pipeline`.
3. Click on the `Run workflow` dropdown button.
4. Enter a list of `spreadsheetId`s, delimited by `,`. For example `spreadsheetId-1,spreadsheetId-2,spreadsheetId-3`.
5. Enter a database environment, `staging` or `production`. Choose `staging` if you want to load the spreadsheet(s) in step 4 to the `staging` database. Choose `production` if you want to load the spreadsheet(s) in step 4 to the `production` database.
6. Click on the green `Run workflow` button to run the workflow.
7. Wait until a new workflow run result shows up or refresh the page, and click on it to see its details.
8. After the script is done loading the spreadsheet(s) to the database, the script runs QA Test and produces a `qa-test-artifact` that contains the result of the QA Test. Please see [Run QA Test](run_qa_test.html) to see documentation on the produced zip file.

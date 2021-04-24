# Run QA Test

## Command line

1. Install the package with:

    ```bash
    pip install siglatools
    ```

2. Run the following command with the correct configurations.

    ```bash
    run_qa_test -gacp /path/to/google-api-credentials.json -ssi <spreadsheet_ids> -dbe <db_env> -sdbcu <staging_db_connection_url> -pdbcu <prod_db_connection_url>
    ```

## GitHub Actions (for collaborators+ only) 

1. Visit https://github.com/SIGLA-GU/siglatools/actions.
2. From the list of workflows, select `Manual Run QA Test`.
3. Click on the `Run workflow` dropdown button.
4. Enter a list of `spreadsheetId`s, delimited by `,`. For example `spreadsheetId-1,spreadsheetId-2,spreadsheetId-3`.
5. Enter a database environment, `staging` or `production`. Choose `staging` if you want to compare the spreadsheet(s) in step 4 with the `staging` database. Choose `production` if you want to compare the spreadsheet(s) in step 4 with the `production` database.
6. Click on the green `Run workflow` button to run the workflow.
7. Wait until a new workflow run result shows up or refresh the page, and click on it to see its details.
8. On the workflow details page, once the run is finished click on the `qa-test-artifact` to download a zip file.

## QA Test Artifact Zip File
The zip file contains any mismatches between the data in the GoogleSheet and the data in the database. One file for each institution, if there are mismatches between the institution in the GoogleSheet and the institution in the database. The files are organized by the spreadsheet they belong to. There is an additional file `extra-institutions.csv`, which contains institutions in the database, but for some reason, doesn't have a counterpart institution in the GoogleSheet. If there are no files in the zip file, then the QA script found no mismatches for the given spreadsheets.

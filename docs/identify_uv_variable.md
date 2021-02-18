# Identify Update and Verify Variables

## Command line

1. Install the package with:

    ```bash
    pip install siglatools
    ```

2. Run the following command with the correct configurations.

    ```bash
    identify_uv_variable -msi <master_spreadsheet_id> -gacp /path/to/google-api-credentials.json -sd <start_date> -ed <end_date>
    ```

## GitHub Actions (for collaborators+ only) 

1. Visit https://github.com/SIGLA-GU/siglatools/actions.
2. From the list of workflows, select `Identify Update and Verify Variables`.
3. Click on the `Run workflow` dropdown button.
4. Enter a start date and an end date. Make sure they are in YYYY-MM-DD format and start date is less than or equal to end date.
5. Click on the green `Run workflow` button to run the workflow.
6. Wait until a new workflow run result shows up and click on it see its details.
7. On workflow details page, once the run is finished click on the `identify-uv-variable-artifact` to download a csv file of variables that need updating and verifying.
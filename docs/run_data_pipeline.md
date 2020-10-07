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
5. Go to the Actions tab and see the pipeline in progress

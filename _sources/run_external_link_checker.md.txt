# Run External Link Checker

## Command line

1. Install the package with:

    ```bash
    pip install siglatools
    ```

2. Run the following command with the correct configurations.

    ```bash
    run_external_link_checker -msi <master_spreadsheet_id> -gacp /path/to/google-api-credentials.json
    ```

## GitHub Actions (for collaborators+ only) 

1. Visit https://github.com/SIGLA-GU/siglatools/actions.
2. From the list of workflows, select `Manual Run External Link Checker`.
3. Click on the `Run workflow` dropdown button.
4. Click on the green `Run workflow` button to run the external link checker.
5. Wait until a new workflow run result shows up and click on it see its details.
6. On workflow details page, once the run is finished click on the `external-link-artifact` to download a csv file of bad external URLs.

## External Links Reasons

The tab delimited csv file has a `reason` fieldname as to why the URL is included for further inspection. Here are the relevant reasons:
- `404 - Not found` means the webpage doesn't exist.
- `403 - Forbidden` means the webpage required authentication or the webpage doesn't allow a script to visit the webpage. Either way, this webpage requires further inspection.
- `Untrusted SSL Certificate` means the webpage doesn't have an acceptable SSL Certificate and the browser may warn users of potential security risks when they visit the webpage.
- `Request timed out` means it took too long to get a response from the webpage and requires further inspection.
- `Error connecting` means the script was unable to visit the webpage and requires further inspection.
- `Unknown error` means the script encountered an unknown error and requires further inspection.

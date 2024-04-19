import argparse
import sys

import requests
import shipyard_bp_utils.files as shipyard
from requests.auth import HTTPBasicAuth
from shipyard_templates import ShipyardLogger

from shipyard_mode.cli import exit_codes

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account-name", dest="account_name", required=True)
    parser.add_argument("--report-id", dest="report_id", required=True)
    parser.add_argument("--token-id", dest="token_id", required=True)
    parser.add_argument("--token-password", dest="token_password", required=True)
    parser.add_argument("--run-id", dest="run_id", required=False)
    parser.add_argument("--dest-file-name", dest="dest_file_name", required=True)
    parser.add_argument(
        "--dest-folder-name", dest="dest_folder_name", default="", required=False
    )
    parser.add_argument(
        "--file-type",
        dest="file_type",
        choices=["json", "pdf", "csv"],
        type=str.lower,
        required=True,
    )
    return parser.parse_args()


def assess_request_status(request):
    """
    Look at the request to determine if an error should be raised.
    """
    status_code = request.status_code
    if status_code == 200:
        pass

    elif status_code == 401:  # Invalid credentials
        logger.error(
            "Mode API returned an Unauthorized response,",
            "check if credentials are correct and try again",
        )
        sys.exit(exit_codes.EXIT_CODE_INVALID_CREDENTIALS)

    elif status_code == 404:  # Invalid run
        if "account not found" in request.text:
            logger.error("Mode reports: account not found")
            sys.exit(exit_codes.EXIT_CODE_INVALID_ACCOUNT)
        if "report not found" in request.text:
            logger.error("Mode reports: report not found")
            sys.exit(exit_codes.EXIT_CODE_INVALID_REPORT_ID)

    else:  # some other error
        logger.error(
            f"Mode run report returned an unknown status {status_code}/n",
            f"returned data: {request.text}",
        )
        sys.exit(exit_codes.EXIT_CODE_UNKNOWN_ERROR)


def get_report_latest_run_id(account_name, report_id, token_id, token_password):
    """
    Get the latest successful run ID from a given report_id.
    """
    mode_api_base = f"https://app.mode.com/api/{account_name}"
    results_api = f"{mode_api_base}/reports/{report_id}/runs/"
    logger.info(f"Finding the latest successful run_id for report {report_id}")
    report_request = run_mode_request(token_id, token_password, results_api)

    result = report_request.json()
    assess_request_status(report_request)

    # Find the latest successful run_id and return it.
    # Will only look at the last 20 runs... but that should be good enough.
    for report_run in result["_embedded"]["report_runs"]:
        if report_run["is_latest_successful_report_run"]:
            most_recent_report_run_id = report_run["token"]
            break

    logger.info(f"The latest successful run_id is {most_recent_report_run_id}.")
    return most_recent_report_run_id


def generate_report_url(account_name, report_id, run_id, file_type):
    mode_api_base = f"https://app.mode.com/api/{account_name}"

    """
    Download report as file
    see:https://mode.com/developer/api-reference/analytics/report-runs/#getReportRun
    or: https://mode.com/developer/api-cookbook/distribution/export-pdf/
    """
    return (
        f"{mode_api_base}/reports/{report_id}/exports/runs/{run_id}/pdf/download"
        if file_type == "pdf"
        else f"{mode_api_base}/reports/{report_id}/runs/{run_id}/results/content.{file_type}"
    )


def run_mode_request(token_id, token_password, report_url):
    report_request = requests.get(
        report_url,
        headers={"Content-Type": "application/json", "Accept": "application/hal+json"},
        auth=HTTPBasicAuth(token_id, token_password),
        stream=True,
    )

    assess_request_status(report_request)
    return report_request


def main():
    args = get_args()
    token_id = args.token_id
    token_password = args.token_password
    account_name = args.account_name
    report_id = args.report_id
    file_type = args.file_type

    # get latest successful run_id if not specified
    if args.run_id:
        run_id = args.run_id
    else:
        run_id = get_report_latest_run_id(
            account_name, report_id, token_id, token_password
        )
    dest_file_name = args.dest_file_name
    dest_folder_name = args.dest_folder_name
    if dest_folder_name:
        shipyard.create_folder_if_dne(dest_folder_name)

    destination_file_path = shipyard.combine_folder_and_file_name(
        dest_folder_name, dest_file_name
    )

    report_url = generate_report_url(account_name, report_id, run_id, file_type)
    logger.info(f"Downloading the contents of the report {report_id}.")
    result = run_mode_request(token_id, token_password, report_url)
    with open(destination_file_path, "wb+") as f:
        f.write(result.content)
    logger.info(
        f"The contents of report {report_id} were successfully written to {destination_file_path}"
    )


if __name__ == "__main__":
    main()

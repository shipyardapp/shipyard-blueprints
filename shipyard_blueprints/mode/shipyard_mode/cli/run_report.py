import argparse
import sys
import time

import requests
from requests.auth import HTTPBasicAuth
from shipyard_bp_utils.artifacts import Artifact
from shipyard_templates import ShipyardLogger

from shipyard_mode.cli import exit_codes

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account-name", dest="account_name", required=True)
    parser.add_argument("--report-id", dest="report_id", required=True)
    parser.add_argument("--token-id", dest="token_id", required=True)
    parser.add_argument("--token-password", dest="token_password", required=True)
    parser.add_argument(
        "--wait-for-completion", dest="wait_for_completion", required=False
    )
    return parser.parse_args()


def execute_run_report(account_name, report_id, token_id, token_password):
    """Executes a mode report run
    see: https://mode.com/developer/api-reference/analytics/report-runs/#runReport
    """
    mode_api_base = f"https://app.mode.com/api/{account_name}"
    run_report_endpoint = f"{mode_api_base}/reports/{report_id}/runs"

    report_request = requests.post(
        run_report_endpoint,
        data={},
        headers={"Content-Type": "application/json", "Accept": "application/hal+json"},
        auth=HTTPBasicAuth(token_id, token_password),
    )

    status_code = report_request.status_code
    # save report data
    run_report_data = report_request.json()

    # handle response codes
    if status_code == 202:
        logger.info(f"Run report for ID: {report_id} was successfully triggered.")
        return run_report_data

    elif status_code == 400:
        logger.error("Bad request sent to Mode. Response data: {report_request.text}")
        sys.exit(exit_codes.EXIT_CODE_BAD_REQUEST)

    elif status_code == 401:
        logger.error(
            "Mode API returned an Unauthorized response,",
            "check if credentials are correct and try again",
        )
        sys.exit(exit_codes.EXIT_CODE_INVALID_CREDENTIALS)
    elif status_code == 403:
        logger.error(
            "Mode Account provided is not accessible,"
            "Check if account is correct and try again"
        )
        sys.exit(exit_codes.EXIT_CODE_INVALID_CREDENTIALS)
    elif status_code == 404:
        if "account not found" in report_request.text:
            logger.error("Mode reports: account not found")
            sys.exit(exit_codes.EXIT_CODE_INVALID_ACCOUNT)
        elif "report not found" in report_request.text:
            logger.error("Mode reports: report not found")
            sys.exit(exit_codes.EXIT_CODE_INVALID_REPORT_ID)
        sys.exit(exit_codes.EXIT_CODE_UNKNOWN_ERROR)
    elif status_code == 500:
        logger.error(
            "Mode encountered an Error trying your request.",
            f"Check if Report ID: {report_id} is correct",
        )
        sys.exit(exit_codes.EXIT_CODE_BAD_REQUEST)
    else:
        logger.error(
            f"Mode run report returned an unknown status {status_code}/n",
            f"returned data: {report_request.text}",
        )
        sys.exit(exit_codes.EXIT_CODE_UNKNOWN_ERROR)


def handle_run_data(run_report_data):
    run_id = run_report_data["token"]
    state = run_report_data["state"]
    completed_at = run_report_data["completed_at"]
    # handle the various run states
    if state == "cancelled":
        logger.info(f"Report run {run_id} was cancelled.")
        return exit_codes.EXIT_CODE_FINAL_STATUS_CANCELLED
    elif state == "completed":
        logger.info(
            f"Report run {run_id} was completed. completed time: {completed_at}"
        )
        return exit_codes.EXIT_CODE_FINAL_STATUS_SUCCESS
    elif state == "enqueued":
        logger.info(f"Report run {run_id} is enqueued to be run.")
        return exit_codes.EXIT_CODE_FINAL_STATUS_NOT_STARTED
    elif state == "failed":
        logger.info(f"Report run {run_id} failed.")
        return exit_codes.EXIT_CODE_FINAL_STATUS_FAILED
    elif state == "pending":
        logger.info(f"Report run {run_id} is currently pending.")
        return exit_codes.EXIT_CODE_FINAL_STATUS_PENDING
    elif state == "running_notebook":
        logger.info(f"Report run {run_id} is currently running a notebook process.")
        return exit_codes.EXIT_CODE_FINAL_STATUS_PENDING
    elif state == "succeeded":
        logger.info(f"Report run: {run_id} completed successfully at {completed_at}")
        return exit_codes.EXIT_CODE_FINAL_STATUS_SUCCESS
    else:
        logger.warning(f"Unknown status: {state}. check response data for details")
        return exit_codes.EXIT_CODE_UNKNOWN_ERROR


def main():
    artifact = Artifact("mode")

    args = get_args()
    token_id = args.token_id
    token_password = args.token_password
    account_name = args.account_name
    report_id = args.report_id

    # execute run report
    report_data = execute_run_report(account_name, report_id, token_id, token_password)
    artifact.responses.write_json(report_data, f"sync_run_{report_id}_response")

    # get run report id and save as pickle
    report_run_id = report_data["token"]
    logger.info(f"Report run id is: {report_run_id}")
    artifact.variables.create_variable("report_run_id", report_run_id)

    if args.wait_for_completion == "TRUE":
        exit_code_status = handle_run_data(report_data)
        while exit_code_status in {
            exit_codes.EXIT_CODE_FINAL_STATUS_PENDING,
            exit_codes.EXIT_CODE_FINAL_STATUS_NOT_STARTED,
        }:
            logger.info("Waiting 60 seconds to check status again...")
            time.sleep(60)
            exit_code_status = handle_run_data(report_data)
        sys.exit(exit_code_status)


if __name__ == "__main__":
    main()

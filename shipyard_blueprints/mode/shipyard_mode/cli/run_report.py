import argparse
import sys
import time

from shipyard_bp_utils.artifacts import Artifact
from shipyard_templates import ShipyardLogger

from shipyard_mode import ModeClient

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


def main():
    artifact = Artifact("mode")
    args = get_args()
    token_id = args.token_id
    token_password = args.token_password
    account_name = args.account_name
    report_id = args.report_id

    mode = ModeClient(token_id, token_password, account_name)

    report_data = mode.execute_run_report(report_id)
    artifact.responses.write_json(f"sync_run_{report_id}_response.json", report_data)

    report_run_id = report_data["token"]
    logger.info(f"Report run id is: {report_run_id}")

    artifact.variables.write_pickle("report_run_id", report_run_id)

    if args.wait_for_completion == "TRUE":
        exit_code_status = mode.handle_run_data(report_data)
        while exit_code_status in {
            mode.EXIT_CODE_FINAL_STATUS_PENDING,
            mode.EXIT_CODE_FINAL_STATUS_NOT_STARTED,
        }:
            logger.info("Waiting 60 seconds to check status again...")
            time.sleep(60)
            exit_code_status = mode.handle_run_data(report_data)
        sys.exit(exit_code_status)


if __name__ == "__main__":
    main()

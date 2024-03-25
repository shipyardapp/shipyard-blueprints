import argparse
import sys

from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger, ExitCodeException, DataVisualization

from shipyard_mode import ModeClient

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
        required=True,
    )
    return parser.parse_args()


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


def main():
    try:
        args = get_args()
        token_id = args.token_id
        token_password = args.token_password
        account_name = args.account_name
        report_id = args.report_id
        file_type = args.file_type.lower()
        mode = ModeClient(token_id, token_password, account_name)

        # get latest successful run_id if not specified
        run_id = args.run_id or mode.get_report_latest_run_id(report_id)

        dest_folder_name = args.dest_folder_name

        shipyard.create_folder_if_dne(dest_folder_name)
        destination_file_path = shipyard.combine_folder_and_file_name(
            dest_folder_name, args.dest_file_name
        )

        report_url = generate_report_url(account_name, report_id, run_id, file_type)
        logger.info(f"Downloading the contents of the report {report_id}.")
        result = mode.run_mode_request(report_url)
        with open(destination_file_path, "wb+") as f:
            f.write(result.content)
        logger.info(
            f"The contents of report {report_id} were successfully written to {destination_file_path}"
        )
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(e)
        sys.exit(DataVisualization.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()

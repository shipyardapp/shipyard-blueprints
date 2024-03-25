import argparse
import sys

from shipyard_bp_utils.artifacts import Artifact
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
    return parser.parse_args()


def main():
    try:
        artifact = Artifact("mode")

        args = get_args()

        mode = ModeClient(args.token_id, args.token_password, args.account_name)

        report_run_id = args.run_id or artifact.variables.read_pickle("report_run_id")
        run_data = mode.get_report_run_data(args.report_id, report_run_id)
        run_report_file_name = artifact.responses.write_json(
            f"verify_run_{report_run_id}_response.json", run_data
        )

        logger.info(f"Run report data saved to {run_report_file_name}")

        exit_code = mode.handle_run_data(run_data)
        sys.exit(exit_code)
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(e)
        sys.exit(DataVisualization.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()

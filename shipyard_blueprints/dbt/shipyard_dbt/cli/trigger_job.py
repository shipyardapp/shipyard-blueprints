import argparse
import sys
import time

import shipyard_bp_utils as shipyard
from shipyard_bp_utils.artifacts import Artifact
from shipyard_templates import ShipyardLogger, ExitCodeException, Etl

from shipyard_dbt import DbtClient

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", dest="api_key", required=True)
    parser.add_argument("--account-id", dest="account_id", required=True)
    parser.add_argument("--job-id", dest="job_id", required=True)
    parser.add_argument(
        "--wait-for-completion",
        dest="wait_for_completion",
        default="TRUE",
        required=False,
    )
    parser.add_argument(
        "--download-log",
        default="False",
        required=False,
        help="Legacy flag, not used in this blueprint.",
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()
        artifact = Artifact("dbtcloud")
        job_id = args.job_id
        wait_for_completion = shipyard.args.convert_to_boolean(args.wait_for_completion)

        client = DbtClient(args.api_key, args.account_id)
        job_run_response = client.trigger_sync(job_id)

        artifact.responses.write_json(f"job_{job_id}_response", job_run_response)

        run_id = job_run_response["data"]["id"]
        artifact.variables.create_pickle("run_id", run_id)

        if wait_for_completion:
            is_complete = False
            exit_code = Etl.EXIT_CODE_FINAL_STATUS_INCOMPLETE
            while not is_complete:
                run_details = client.get_run_details(run_id)
                artifact.responses.write_json(f"run_{run_id}_response", run_details)
                is_complete = run_details["data"]["is_complete"]
                if not is_complete:
                    logger.info(
                        f"Run {run_id} is not complete. Waiting 60 seconds and trying again."
                    )
                    time.sleep(60)
                else:
                    exit_code = client.determine_sync_status(run_id)
            sys.exit(exit_code)
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An unknown error occurred: {e}")
        sys.exit(Etl.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()

import argparse
import sys
import time

import shipyard_bp_utils as shipyard
from shipyard_bp_utils.artifacts import Artifact
from shipyard_templates import ShipyardLogger, Etl, ExitCodeException

from shipyard_dbt import DbtClient

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", dest="api_key", required=True)
    parser.add_argument("--account-id", dest="account_id", required=True)
    parser.add_argument("--job-id", dest="job_id", required=True)
    parser.add_argument(
        "--download-artifacts",
        dest="download_artifacts",
        default="TRUE",
        choices={"TRUE", "FALSE"},
        required=False,
    )
    parser.add_argument(
        "--download-logs",
        dest="download_logs",
        default="TRUE",
        choices={"TRUE", "FALSE"},
        required=False,
    )

    return parser.parse_args()


def main():
    exit_code = 0
    try:
        artifact = Artifact(
            "dbtcloud"
        )  # Note: dbt has a concept of artifact as well. This is a different artifact.

        args = get_args()
        download_artifacts = shipyard.args.convert_to_boolean(args.download_artifacts)
        download_logs = shipyard.args.convert_to_boolean(args.download_logs)

        job_id = args.job_id

        client = DbtClient(args.api_key, args.account_id)

        job_run_response = client.trigger_sync(job_id)
        artifact.responses.write_json(f"job_{job_id}_response", job_run_response)

        run_id = job_run_response["data"]["id"]
        artifact.variables.create_pickle("run_id", run_id)

        is_complete = False
        while not is_complete:
            run_details_response = client.get_run_details(run_id)
            is_complete = run_details_response["data"]["is_complete"]
            if not is_complete:
                logger.info(
                    f"Run {run_id} is not complete. Waiting 60 seconds and trying again."
                )
                time.sleep(60)
            else:
                exit_code = client.determine_sync_status(run_id)

        if download_logs:
            number_of_steps = len(run_details_response["data"]["run_steps"])
            if number_of_steps > 0:
                debug_log_name = shipyard.files.combine_folder_and_file_name(
                    artifact.logs.path, "dbt.log"
                )
                output_log_name = shipyard.files.combine_folder_and_file_name(
                    artifact.logs.path, "dbt_console_output.txt"
                )

                logger.info(f"Downloading logs for {number_of_steps} steps")
                for index, step in enumerate(
                    run_details_response["data"]["run_steps"], start=1
                ):
                    step_id = step["id"]
                    logger.info(
                        f"Grabbing step details for step {step_id} ({index} of {number_of_steps})"
                    )
                    artifact.responses.write_json(f"step_{step_id}_response", step)

                    with open(debug_log_name, "a") as debug_file:
                        debug_file.write(step["debug_logs"])

                    with open(output_log_name, "a") as log_file:
                        log_file.write(step["logs"])

                logger.info(f"Successfully wrote logs to {output_log_name}")
                logger.info(f"Successfully wrote debug_logs to {debug_log_name}")
            elif number_of_steps == 0:
                logger.info(f"No logs to download for run {run_id}")

        if download_artifacts:
            dbt_artifact_response = client.get_artifact_details(run_id)
            artifact.responses.write_json(
                f"artifacts_{run_id}_response", dbt_artifact_response
            )
            dbt_artifacts = dbt_artifact_response["data"]

            if dbt_artifacts:
                artifact_path = artifact.create_folder("artifacts")
                number_of_artifacts = len(dbt_artifacts)
                logger.info(f"Downloading {number_of_artifacts} artifacts")
                for index, dbt_artifact in enumerate(dbt_artifacts, start=1):
                    logger.info(f"Downloading file {index} of {number_of_artifacts}")
                    client.download_artifact(run_id, dbt_artifact, artifact_path)
            else:
                logger.info("No artifacts exist for this run.")

        sys.exit(exit_code)
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An unknown error occurred: {e}")
        sys.exit(Etl.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()

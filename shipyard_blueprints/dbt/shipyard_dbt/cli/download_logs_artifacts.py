import argparse
import sys

import shipyard_bp_utils as shipyard
from shipyard_bp_utils.artifacts import Artifact
from shipyard_templates import ShipyardLogger, ExitCodeException, Etl

from shipyard_dbt import DbtClient

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", dest="api_key", required=True)
    parser.add_argument("--account-id", dest="account_id", required=True)
    parser.add_argument("--run-id", dest="run_id", required=False)

    return parser.parse_args()


def download_steps(run_details_response, artifact):
    logger.info("Downloading logs for run steps")
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


def download_dbt_artifacts(dbt_client, run_id, artifact):
    logger.info("Downloading dbt artifacts")
    dbt_artifact_response = dbt_client.get_artifact_details(run_id)
    artifact.responses.write_json(f"artifacts_{run_id}_response", dbt_artifact_response)
    if dbt_artifacts := dbt_artifact_response["data"]:
        artifact_path = artifact.create_folder("artifacts")
        number_of_artifacts = len(dbt_artifacts)
        logger.info(f"Downloading {number_of_artifacts} artifacts")
        for index, dbt_artifact in enumerate(dbt_artifacts, start=1):
            logger.info(f"Downloading file {index} of {number_of_artifacts}")
            dbt_client.download_artifact(run_id, dbt_artifact, artifact_path)
    else:
        logger.info("No artifacts exist for this run.")


def main():
    try:
        artifact = Artifact(
            "dbtcloud"
        )  # Note: dbt has a concept of artifact as well. This is a different artifact.
        args = get_args()

        run_id = args.run_id or artifact.variables("run_id")

        client = DbtClient(args.api_key, args.account_id)
        run_details_response = client.get_run_details(run_id)
        artifact.responses.write_json(f"run_{run_id}_response", run_details_response)

        download_steps(run_details_response, artifact)

        download_dbt_artifacts(client, run_id, artifact)
    except ExitCodeException as e:
        logger.error(e.message)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An unknown error occurred.{e}")
        sys.exit(Etl.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()

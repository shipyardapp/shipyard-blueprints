import sys
import time
import argparse
import shipyard_bp_utils as shipyard
from shipyard_domo.utils import exceptions as errs
from shipyard_domo import DomoClient
from shipyard_templates import ShipyardLogger, ExitCodeException
from shipyard_bp_utils.artifacts import Artifact

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-id", dest="client_id", required=True)
    parser.add_argument("--secret-key", dest="secret_key", required=True)
    parser.add_argument("--dataset-id", dest="dataset_id", required=True)
    parser.add_argument(
        "--wait-for-completion",
        dest="wait_for_completion",
        required=False,
        default="FALSE",
    )
    args = parser.parse_args()

    return args


def determine_execution_status(execution_data):
    # check if execution has finished first
    status = execution_data["currentState"]
    if status == "ABORTED":
        print("Domo Refresh has been aborted")
        return errs.EXIT_CODE_FINAL_STATUS_CANCELLED
    elif status == "ACTIVE":
        print(f"Domo Refresh is still currently ongoing with status {status}")
        return errs.EXIT_CODE_STATUS_INCOMPLETE
    elif status == "INVALID":
        print("Domo Refresh is invalid either due to system conflict or error")
        return errs.EXIT_CODE_FINAL_STATUS_INVALID
    elif status == "SUCCESS":
        print("Domo has refreshed successfully ")
        return errs.EXIT_CODE_FINAL_STATUS_SUCCESS
    else:
        print(f"Unknown Domo Refresh status: {status}")
        return errs.EXIT_CODE_UNKNOWN_STATUS


def main():
    try:
        args = get_args()
        wait = shipyard.args.convert_to_boolean(args.wait_for_completion)
        # initialize domo with auth credentials
        client = DomoClient(client_id=args.client_id, secret_key=args.secret_key)

        # execute dataset refresh
        results = client.refresh_dataset(args.dataset_id)
        execution_id = results["id"]
        logger.info("Sucessfully refreshed dataset")

        # create the artifacts directory and save the execution id (for legacy purposes)
        domo_artifact = Artifact("domo")
        domo_artifact.variables.write(
            filename="execution_id", file_type="pickle", data=execution_id
        )

        if wait:
            execution_data = client.get_execution_details(args.dataset_id, execution_id)
            exit_code_status = determine_execution_status(execution_data)
            logger.info("Waiting for Domo Refresh to complete")
            while exit_code_status == errs.EXIT_CODE_STATUS_INCOMPLETE:
                print("Waiting for 60 seconds before checking again")
                time.sleep(60)
                execution_data = client.get_execution_details(
                    args.dataset_id, execution_id
                )
                exit_code_status = determine_execution_status(execution_data)
            logger.info("Dataset refresh complete")
            sys.exit(exit_code_status)
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(f"Error in refreshing dataset. Response from the API: {str(e)}")
        sys.exit(errs.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()

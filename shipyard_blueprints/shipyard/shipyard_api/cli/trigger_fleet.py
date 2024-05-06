import argparse
import sys
import shipyard_bp_utils as utils
from shipyard_api import ShipyardClient
from shipyard_templates import ShipyardLogger, ExitCodeException
from shipyard_api.errors import EXIT_CODE_ARTIFACTS_ERROR, EXIT_CODE_UNKNOWN_ERROR
from shipyard_bp_utils.artifacts import Artifact

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--organization-id", dest="org_id", required=True)
    parser.add_argument("--api-key", dest="api_key", required=True)
    parser.add_argument("--project-id", dest="project_id", required=True)
    parser.add_argument("--fleet-id", dest="fleet_id", required=True)
    return parser.parse_args()


def main():
    try:
        args = get_args()
        org_id = args.org_id
        api_key = args.api_key
        project_id = args.project_id
        fleet_id = args.fleet_id
        shipyard = ShipyardClient(org_id=org_id, api_key=api_key, project_id=project_id)
        response = shipyard.trigger_fleet(fleet_id)
        logger.info("Successfully triggered fleet")
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)

    try:
        logger.debug(
            "Response is being stored in the artifacts directory for downstream use"
        )
        log_url = response.get("log")
        logger.info(f"Fleet run log URL: {log_url}")

        artifact = Artifact("shipyard-api")
        artifact.responses.write_json("trigger_fleet_response", response)
        org_id = response.get("data").get("org_id")
        project_id = response.get("data").get("project_id")
        fleet_id = response.get("data").get("fleet_id")
        fleet_run_id = response.get("data").get("fleet_run_id")
        artifact.variables.write("org_id", "pickle", org_id)
        artifact.variables.write("project_id", "pickle", project_id)
        artifact.variables.write("fleet_id", "pickle", fleet_id)
        artifact.variables.write("fleet_run_id", "pickle", fleet_run_id)
    except Exception as e:
        logger.error(
            f"An error occurred while writing the response and variables to the artifacts directory: {e}"
        )
        sys.exit(EXIT_CODE_ARTIFACTS_ERROR)


if __name__ == "__main__":
    main()

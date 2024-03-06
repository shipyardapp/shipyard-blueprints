import re
import sys
import time
import argparse
import requests
import shipyard_bp_utils as shipyard
from shipyard_hex import HexClient
from shipyard_templates import ExitCodeException, ShipyardLogger
from shipyard_bp_utils.artifacts import Artifact
import shipyard_hex.hex_exceptions as ec


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-id", dest="project_id", required=True)
    parser.add_argument("--api-token", dest="api_token", required=True)
    parser.add_argument(
        "--wait-for-completion", dest="wait_for_completion", default="FALSE"
    )
    return parser.parse_args()


def main():
    args = get_args()
    project_id = args.project_id.strip()
    api_token = args.api_token.strip()
    wait = shipyard.args.convert_to_boolean(args.wait_for_completion)
    client = HexClient(api_token=api_token, project_id=project_id)

    ## run the project
    run_response = client.run_project()
    run_id = run_response.json()["runId"]

    if wait:
        run_status = client.determine_status(run_response.json())
        while run_status in (ec.EXIT_CODE_RUNNING, ec.EXIT_CODE_PENDING):
            time.sleep(60)
            run_response = client.get_run_status(run_id)
            run_status = client.determine_status(run_response)
        sys.exit(run_status)


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()

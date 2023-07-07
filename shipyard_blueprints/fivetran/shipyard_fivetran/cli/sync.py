import argparse
import sys
import os
import shipyard_utils as shipyard

from shipyard_fivetran import FivetranClient
from shipyard_templates import ExitCodeException


def create_pickle(connector_id):
    base_folder_name = shipyard.logs.determine_base_artifact_folder("fivetran")
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name
    )
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # save sync run id as variable
    shipyard.logs.create_pickle_file(
        artifact_subfolder_paths, "connector_id", connector_id
    )


def get_args():
    parser = argparse.ArgumentParser(description="Trigger a sync using FivetranClient")
    parser.add_argument("--api-key", type=str, help="Fivetran API access token")
    parser.add_argument("--api-secret", type=str, help="Fivetran API secret")
    parser.add_argument("--connector-id", type=str, help="ID of the connector")
    parser.add_argument(
        "--force",
        default="TRUE",
        help="Force the sync (optional, default: True)",
    )
    parser.add_argument(
        "--wait",
        default="FALSE",
        help="Wait for the sync to complete (optional, default: False)",
    )
    parser.add_argument(
        "--interval",
        default=1,
        help="Interval in minutes to check for sync completion (optional, default: 1)",
    )

    return parser.parse_args()


def main():
    args = get_args()

    force_sync = args.force
    wait_for_completion = args.wait.upper() == "TRUE"
    force_sync = force_sync.upper() == "TRUE"
    poke_interval = args.interval

    fivetran_client = FivetranClient(
        access_token=args.api_key, api_secret=args.api_secret
    )
    if (
        not wait_for_completion
        and int(os.environ.get("SHIPYARD_FLEET_DOWNSTREAM_COUNT")) > 0
    ):
        # This function is to support the legacy check status functionality
        create_pickle(args.connector_id)
    poke_interval = int(poke_interval) if poke_interval else 0
    if wait_for_completion:
        if 0 < poke_interval <= 60:
            fivetran_client.logger.info(f"Setting poke interval to {poke_interval} minute(s)")
            poke_interval = poke_interval
        else:
            fivetran_client.logger.error("Poke interval must be between 1 and 60 minutes")
            sys.exit(fivetran_client.EXIT_CODE_INVALID_POKE_INTERVAL)
    try:
        fivetran_client.trigger_sync(
        args.connector_id,
        force=force_sync,
        wait_for_completion=wait_for_completion,
        poke_interval=poke_interval * 60,
    )
    except ExitCodeException as e:
        fivetran_client.logger.error(e)
        sys.exit(e.exit_code)


if __name__ == "__main__":
    main()

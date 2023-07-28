import argparse
import time
import sys
import os

import shipyard_utils as shipyard

from shipyard_rudderstack import RudderStackClient
from shipyard_templates import ExitCodeException


def create_artifacts(source_id):
    """This function is to support the legacy check status functionality"""
    # create artifacts folder to save run id
    base_folder_name = shipyard.logs.determine_base_artifact_folder("rudderstack")
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name
    )
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # save sync run id as variable
    shipyard.logs.create_pickle_file(artifact_subfolder_paths, "source_id", source_id)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--source-id", dest="source_id", required=True)
    parser.add_argument("--wait-for-completion", default="FALSE")
    parser.add_argument("--poke-interval", default=1)
    return parser.parse_args()


def main():
    args = get_args()
    access_token = args.access_token
    source_id = args.source_id

    rudderstack = RudderStackClient(access_token=access_token)
    # execute trigger sync
    try:
        rudderstack.trigger_sync(source_id)
    except ExitCodeException as e:
        rudderstack.logger.error(e.message)
        sys.exit(e.exit_code)

    wait = args.wait_for_completion.upper() == "TRUE"
    poke_interval = args.poke_interval

    if (
        not wait
        and int(os.environ.get("SHIPYARD_FLEET_DOWNSTREAM_COUNT")) > 0
    ):
        # This handles the situation where there are downstream vessels and the poke interval is not set since this is a good indicator that the fleet is running on the original blueprint
        create_artifacts(source_id)
    elif wait:
        # wait for sync to complete if wait is set to true
        if 0 < int(poke_interval) <= 60:
            rudderstack.logger.info(f"Setting poke interval to {poke_interval} minute(s)")
            poke_interval = int(poke_interval)
        else:
            rudderstack.logger.error("Poke interval must be between 1 and 60 minutes")
            sys.exit(rudderstack.EXIT_CODE_INVALID_POKE_INTERVAL)

        status = rudderstack.determine_sync_status(source_id)
        try:
            while status == "processing":
                rudderstack.logger.info(
                    f"Waiting {poke_interval} {'minutes' if poke_interval > 1 else 'minute'} to check sync status..."
                )
                time.sleep(poke_interval * 60)
                status = rudderstack.determine_sync_status(source_id)
                rudderstack.logger.info(f"Sync status: {status}")
        except ExitCodeException as e:
            sys.exit(e.exit_code)


if __name__ == "__main__":
    main()

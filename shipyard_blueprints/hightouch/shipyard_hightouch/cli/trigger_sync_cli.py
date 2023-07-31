import argparse
import sys
import requests
import time
from shipyard_templates import ExitCodeException

import shipyard_utils as shipyard
from shipyard_hightouch import HightouchClient


def generate_pickle_file(sync_run_id):
    # create artifacts folder to save run id
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'hightouch')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # save sync run id as variable
    shipyard.logs.create_pickle_file(artifact_subfolder_paths,
                                  'sync_run_id', sync_run_id)

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--sync-id', dest='sync_id', required=True)
    parser.add_argument('--full-resync', dest='full_resync',
                        default=False, required=False)
    parser.add_argument('--wait-for-completion', dest='wait_for_completion')
    parser.add_argument('--poke-interval', dest='poke_interval')

    return parser.parse_args()


def main():
    args = get_args()
    access_token = args.access_token
    sync_id = args.sync_id
    full_resync = shipyard.args.convert_to_boolean(args.full_resync)

    # execute trigger sync
    hightouch = HightouchClient(access_token)
    try:
        trigger_sync_response = hightouch.trigger_sync(
            sync_id=sync_id, full_resync=full_resync)
        sync_run_id = trigger_sync_response['id']
    except ExitCodeException as error:
        hightouch.logger.error(error.message)
        sys.exit(error.exit_code)

    if args.wait_for_completion == 'TRUE' and (0 < int(args.poke_interval) <= 60):
        sync_detail = hightouch.get_sync_status(sync_id, sync_run_id)
        status = hightouch.determine_sync_status(sync_detail)

        while status not in (hightouch.EXIT_CODE_FINAL_STATUS_COMPLETED,
                             hightouch.EXIT_CODE_FINAL_STATUS_ERRORED):
            hightouch.logger.info(f"Waiting {args.poke_interval} minute(s)...")
            time.sleep(int(args.poke_interval) * 60)
            sync_detail = hightouch.get_sync_status(sync_id, sync_run_id)
            status = hightouch.determine_sync_status(sync_detail)

        sys.exit(status)
    elif args.wait_for_completion == 'TRUE':
        hightouch.logger.error(
            "Poke interval must be between 1 and 60 minutes")
        sys.exit(hightouch.EXIT_CODE_SYNC_INVALID_POKE_INTERVAL)
    else:
        generate_pickle_file(sync_run_id) # Backwards Compatibility: Ensures this code works with older versions of Check Sync Blueprint.


if __name__ == "__main__":
    main()

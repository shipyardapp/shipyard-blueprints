import argparse
import sys
import requests
from shipyard_blueprints import utils
from shipyard_blueprints import HightouchClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--sync-id', dest='sync_id', required=True)
    parser.add_argument('--full-resync', dest='full_resync',
                        default=False, required=False)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    access_token = args.access_token
    sync_id = args.sync_id
    full_resync = utils.args.convert_to_boolean(args.full_resync)

    # execute trigger sync
    hightouch = HightouchClient(access_token)
    trigger_sync = hightouch.trigger_sync(
        sync_id=sync_id, full_resync=full_resync)
    sync_run_id = trigger_sync['id']

    # create artifcats folder to save run id
    base_folder_name = utils.logs.determine_base_artifact_folder(
        'hightouch')
    artifact_subfolder_paths = utils.logs.determine_artifact_subfolders(
        base_folder_name)
    utils.logs.create_artifacts_folders(artifact_subfolder_paths)

    # save sync run id as variable
    utils.logs.create_pickle_file(artifact_subfolder_paths,
                                  'sync_run_id', sync_run_id)


if __name__ == "__main__":
    main()

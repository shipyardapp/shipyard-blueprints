import argparse
import sys
import requests
from shipyard_blueprints import shipyard_utils
from shipyard_blueprints import HightouchClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--sync-id', dest='sync_id', required=True)
    parser.add_argument('--sync-run-id', dest='sync_run_id', required=False)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    access_token = args.access_token
    sync_id = args.sync_id
    # create artifacts folder to save run id
    base_folder_name = shipyard_utils.logs.determine_base_artifact_folder(
        'hightouch')
    artifact_subfolder_paths = shipyard_utils.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard_utils.logs.create_artifacts_folders(artifact_subfolder_paths)

    hightouch = HightouchClient(access_token)
    if args.sync_run_id:
        sync_run_id = args.sync_run_id
    else:
        sync_run_id = shipyard_utils.logs.read_pickle_file(
            artifact_subfolder_paths, 'sync_run_id')

    sync_run_data = hightouch.get_sync_status(
        sync_id, sync_run_id)
    # save sync run data as json file
    sync_run_data_file_name = shipyard_utils.files.combine_folder_and_file_name(
        artifact_subfolder_paths['responses'],
        f'sync_run_{sync_run_id}_response.json')
    shipyard_utils.files.write_json_to_file(sync_run_data, sync_run_data_file_name)
    # return final status
    exit_code_status = hightouch.determine_sync_status(sync_run_data)
    sys.exit(exit_code_status)


if __name__ == '__main__':
    main()

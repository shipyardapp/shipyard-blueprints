import argparse
import requests
import shipyard_utils as shipyard
from shipyard_blueprints import CensusClient
import sys


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--sync-run-id', dest='sync_run_id', required=False)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    access_token = args.access_token
    # create artifacts folder to save run id
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'census')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # get sync run id variable from user or pickle file if not inputted
    if args.sync_run_id:
        sync_run_id = args.sync_run_id
    else:
        sync_run_id = shipyard.logs.read_pickle_file(
            artifact_subfolder_paths, 'sync_run_id')
    census = CensusClient(access_token)
    # run check sync status
    sync_run_data = census.get_sync_status(sync_run_id)
    # save sync run data as json file
    sync_run_data_file_name = shipyard.files.combine_folder_and_file_name(
        artifact_subfolder_paths['responses'],
        f'sync_run_{sync_run_id}_response.json')
    shipyard.files.write_json_to_file(sync_run_data, sync_run_data_file_name)
    # return status code to sys.out
    exit_code_status = census.determine_sync_status(sync_run_data)
    sys.exit(exit_code_status)


if __name__ == "__main__":
    main()

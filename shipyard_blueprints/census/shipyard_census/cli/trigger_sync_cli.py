import argparse
from shipyard_blueprints import CensusClient
import shipyard_utils as shipyard


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--sync-id', dest='sync_id', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    access_token = args.access_token
    sync_id = args.sync_id
    census = CensusClient(access_token)
    trigger_sync = census.trigger_sync(sync_id)
    sync_run_id = trigger_sync['data']['sync_run_id']

    # create artifacts folder to save run id
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'census')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # save sync run id as variable
    shipyard.logs.create_pickle_file(artifact_subfolder_paths,
                                     'sync_run_id', sync_run_id)


if __name__ == "__main__":
    main()

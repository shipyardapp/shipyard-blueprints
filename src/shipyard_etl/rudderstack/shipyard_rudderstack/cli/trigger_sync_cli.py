import argparse
import sys
import requests
import shipyard_utils as shipyard
from shipyard_rudderstack import RudderStack


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--source-id', dest='source_id', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    access_token = args.access_token
    source_id = args.source_id

    rudderstack = RudderStack(access_token= access_token)
    # execute trigger sync
    rudderstack.trigger_sync(source_id)
    # create artifacts folder to save run id
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'rudderstack')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
    
    # save sync run id as variable
    shipyard.logs.create_pickle_file(artifact_subfolder_paths, 
                                'source_id', source_id)


if __name__ == "__main__":
    main()

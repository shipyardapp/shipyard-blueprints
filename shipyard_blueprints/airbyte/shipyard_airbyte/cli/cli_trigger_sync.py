import argparse
from shipyard_airbyte import AirbyteClient
import shipyard_bp_utils as shipyard


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection-id", dest='connection_id', required=True)
    parser.add_argument("--api-token", dest='api_token', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    connection_id = args.connection_id
    api_token = args.api_token
    client = AirbyteClient(access_token=api_token)
    resp = client.trigger_sync(
        connection_id=connection_id)
    # create artifacts folder to save response
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'airbyte')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # save sync response as variable
    shipyard.logs.create_pickle_file(artifact_subfolder_paths,
                                     'sync_response', resp['jobId'])


if __name__ == "__main__":
    main()

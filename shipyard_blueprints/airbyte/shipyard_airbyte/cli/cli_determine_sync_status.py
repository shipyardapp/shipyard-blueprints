from shipyard_airbyte import AirbyteClient
import shipyard_bp_utils as shipyard
import argparse
import sys


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-token', dest='api_token', required=True)
    parser.add_argument('--job-id', dest='job_id',
                        default=None, required=False)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    api_token = args.api_token
    job_id = args.job_id
    client = AirbyteClient(api_token)
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'airbyte')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
    # check if a job id was provided, otherwise look for it in the artifact folders
    if job_id is None or job_id == '':
        response = shipyard.logs.read_pickle_file(
            artifact_subfolder_paths, 'sync_response')
        job_id = response
    job_status = client.get_sync_status(job_id=job_id)
    sync_status = client.determine_sync_status(job_status)
    sys.exit(sync_status)


if __name__ == "__main__":
    main()

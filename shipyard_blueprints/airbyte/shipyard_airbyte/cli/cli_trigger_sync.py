import argparse
import sys
import time
from shipyard_airbyte import AirbyteClient
import shipyard_bp_utils as shipyard


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection-id", dest='connection_id', required=True)
    parser.add_argument("--api-token", dest='api_token', required=True)
    parser.add_argument("--wait-for-completion", dest='wait_for_completion')
    parser.add_argument("--poke-interval", dest='poke_interval')
    return parser.parse_args()


def generate_pickle_file(job_id):
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'airbyte')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # save sync response as variable
    shipyard.logs.create_pickle_file(artifact_subfolder_paths,
                                     'sync_response', job_id)


def main():
    args = get_args()
    connection_id = args.connection_id
    api_token = args.api_token

    client = AirbyteClient(access_token=api_token)
    try:
        resp = client.trigger_sync(connection_id=connection_id)
    except Exception as e:
        client.logger.error(
            f"Error occurred when attempting to trigger sync due to the following error: {e}")
        sys.exit(1)

    job_id = resp['jobId']

    generate_pickle_file(job_id)  # This is for backwards compatibility the Check Sync Blueprint needs in order to run

    if args.wait_for_completion == 'TRUE' and (0 < args.poke_interval <= 60):
        status = client.determine_sync_status(job_id=job_id)
        while status not in (client.EXIT_CODE_FINAL_STATUS_COMPLETED,
                             client.EXIT_CODE_FINAL_STATUS_INCOMPLETE,
                             client.EXIT_CODE_FINAL_STATUS_CANCELLED):
            status = client.determine_sync_status(job_id=job_id)
            time.sleep(args.poke_interval)

        sys.exit(status)
    elif args.wait_for_completion == 'TRUE':
        client.logger.error(
            "Poke interval must be between 1 and 60 seconds")
        sys.exit(client.EXIT_CODE_SYNC_INVALID_POKE_INTERVAL)



if __name__ == "__main__":
    main()

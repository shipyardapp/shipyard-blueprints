import argparse
import shipyard_bp_utils as shipyard
import sys
from shipyard_coalesce import CoalesceClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--run-id", dest="run_id", required=False, default=None)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    access_token = args.access_token
    run_id = args.run_id
    client = CoalesceClient(access_token)
    base_folder_name = shipyard.logs.determine_base_artifact_folder("coalesce")
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name
    )
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # check if the job id was provided, if not then read it from the artifacts folder
    if run_id is None or run_id == "":
        response = shipyard.logs.read_pickle_file(
            artifact_subfolder_paths, "coalesce_response"
        )
        run_id = response["runCounter"]
    # fetch the status and return the exit code
    status = client.determine_sync_status(run_id)
    sys.exit(status)


if __name__ == "__main__":
    main()

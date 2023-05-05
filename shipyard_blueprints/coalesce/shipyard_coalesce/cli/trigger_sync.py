import argparse

from requests import get
from shipyard_coalesce import CoalesceClient
import shipyard_bp_utils as shipyard


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--environment-id", dest="environment_id", required=True)
    parser.add_argument("--job-id", dest="job_id", required=False, default=None)
    parser.add_argument(
        "--snowflake-username", dest="snowflake_username", required=True
    )
    parser.add_argument(
        "--snowflake-password", dest="snowflake_password", required=True
    )
    parser.add_argument("--snowflake-role", dest="snowflake_role", required=True)
    parser.add_argument(
        "--snowflake-warehouse", dest="snowflake_warehouse", required=True
    )
    parser.add_argument("--parallelism", dest="parallelism", required=False, default=16)
    parser.add_argument(
        "--include-nodes-selector", dest="include_nodes", required=False, default=None
    )
    parser.add_argument(
        "--exclude-nodes-selector", dest="exclude_nodes", required=False, default=None
    )

    args = parser.parse_args()
    return args


def main():
    args = get_args()
    access_token = args.access_token
    sync_args = {
        "environment_id": args.environment_id,
        "job_id": None if args.job_id == '' else args.job_id,
        "snowflake_username": args.snowflake_username,
        "snowflake_password": args.snowflake_password,
        "snowflake_role": None if args.snowflake_role == '' else args.snowflake_role,
        "snowflake_warehouse": None if args.snowflake_role == '' else args.snowflake_warehouse,
        "parallelism": args.parallelism,
        "include_nodes_selector": None if args.include_nodes == '' else args.include_nodes,
        "exclude_nodes_selector": None if args.exclude_nodes == '' else args.exclude_nodes
    }
    client = CoalesceClient(access_token)
    response = client.trigger_sync(**sync_args)

    # create artifacts folder to save response
    base_folder_name = shipyard.logs.determine_base_artifact_folder("coalesce")
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name
    )
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
    # save sync response as variable
    shipyard.logs.create_pickle_file(
        artifact_subfolder_paths, "coalesce_response", response
    )


if __name__ == "__main__":
    main()

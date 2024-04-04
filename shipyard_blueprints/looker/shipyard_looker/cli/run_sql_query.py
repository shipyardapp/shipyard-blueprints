import argparse
import sys
import os
import shipyard_bp_utils as shipyard

from shipyard_templates import ShipyardLogger, ExitCodeException, DataVisualization
from shipyard_looker import LookerClient
from shipyard_bp_utils.artifacts import Artifact

from shipyard_looker.exceptions import EXIT_CODE_SLUG_NOT_FOUND

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", dest="base_url", required=True)
    parser.add_argument("--client-id", dest="client_id", required=True)
    parser.add_argument("--client-secret", dest="client_secret", required=True)
    parser.add_argument("--slug", dest="slug", required=False)
    parser.add_argument("--destination-file-name", dest="dest_file_name", required=True)
    parser.add_argument(
        "--destination-folder-name", dest="dest_folder_name", required=False, default=""
    )
    parser.add_argument(
        "--file-type",
        dest="file_type",
        choices=[
            "inline_json",
            "json",
            "json_detail",
            "json_fe",
            "csv",
            "html",
            "md",
            "txt",
            "xlsx",
            "gsxml",
            "json_label",
        ],
        type=str.lower,
        required=True,
    )
    args = parser.parse_args()
    return args


def run_sql_query_and_download(sdk, slug, file_format):
    try:
        response = sdk.run_sql_query(slug=slug, result_format=file_format)
        print(f"SQL Query {slug} created successfully")
    except Exception as e:
        print(
            f"Error running create query, please ensure that slug {slug} is valid in the explore tab of looker"
        )
        sys.exit(ec.EXIT_CODE_INVALID_SLUG)
    return response


def main():
    try:
        args = get_args()
        base_url = args.base_url
        client_id = args.client_id
        client_secret = args.client_secret
        file_type = args.file_type
        output_file = args.dest_file_name
        arg_slug = args.slug if args.slug != "" else None
        target_dir = args.dest_folder_name
        # get cwd if no folder name is specified
        looker = LookerClient(
            base_url=base_url, client_id=client_id, client_secret=client_secret
        )
        if target_dir:
            shipyard.files.create_folder_if_dne(target_dir)

        target_path = shipyard.files.combine_folder_and_file_name(
            target_dir, output_file
        )
        if arg_slug:
            slug = arg_slug
        else:
            artifacts = Artifact("looker")
            slug = artifacts.variables.read_pickle("slug")
        looker.download_sql_query(slug, target_path, file_format=file_type)
        logger.info(f"SQL Query {slug} successfully downloaded to {target_path}")

    except FileNotFoundError as e:
        logger.error("Slug not found in artifacts directory")
        sys.exit(EXIT_CODE_SLUG_NOT_FOUND)
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to download the SQL query: {e}"
        )
        sys.exit(DataVisualization.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()

import sys
import argparse
import pandas as pd
import os
import ast
import re
import shipyard_bp_utils as shipyard
from pydomo import Domo
from pydomo.streams import CreateStreamRequest, UpdateMethod
from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType
from shipyard_domo.domo import DomoClient
from shipyard_domo.utils import utils
from shipyard_bp_utils.artifacts import Artifact
from shipyard_templates import ShipyardLogger, ExitCodeException

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-id", dest="client_id", required=True)
    parser.add_argument("--secret-key", dest="secret_key", required=True)
    parser.add_argument("--file-name", dest="file_name", required=True)
    parser.add_argument("--dataset-name", dest="dataset_name", required=True)
    parser.add_argument(
        "--dataset-description", dest="dataset_description", required=False
    )
    parser.add_argument("--folder-name", dest="folder_name", required=False)
    parser.add_argument("--domo-schema", dest="domo_schema", required=False, default="")
    parser.add_argument(
        "--insert-method",
        dest="insert_method",
        default="REPLACE",
        choices={"REPLACE", "APPEND"},
        required=True,
    )
    parser.add_argument("--dataset-id", required=False, default="", dest="dataset_id")
    parser.add_argument(
        "--source-file-match-type",
        dest="source_file_match_type",
        choices={"regex_match", "exact_match"},
        default="exact_match",
        required=False,
    )
    args = parser.parse_args()

    return args


def main():
    args = get_args()
    client_id = args.client_id
    secret = args.secret_key
    file_to_load = args.file_name
    dataset_name = args.dataset_name
    dataset_description = args.dataset_description
    folder_name = args.folder_name
    insert_method = args.insert_method
    dataset_id = args.dataset_id
    match_type = args.source_file_match_type
    if args.domo_schema != "":
        domo_schema = args.domo_schema
        domo_schema = ast.literal_eval(domo_schema)

    try:
        client = DomoClient(client_id=client_id, secret_key=secret)
        logger.info("Successfully connected to Domo")
        artifacts = Artifact("domo")

        if match_type == "regex_match":
            file_names = shipyard.files.find_all_local_file_names(folder_name)

            matching_file_names = shipyard.files.find_all_file_matches(
                file_names, re.compile(file_to_load)
            )
            logger.info(
                f"{len(matching_file_names)} files found. Preparing to upload..."
            )
            # if the schema is provided, then use that otherwise infer the schema using sampling
            if args.domo_schema != "":
                dataset_schema = utils.make_schema(
                    domo_schema, matching_file_names, folder_name
                )
            else:
                logger.debug("schema is being inferred")
                dataset_schema = client.infer_schema(
                    matching_file_names, folder_name, k=10000
                )
            # stream_id, execution_id = utils.upload_stream(
            #     domo,
            #     matching_file_names,
            #     dataset_name,
            #     insert_method,
            #     dataset_id,
            #     folder_name,
            #     dataset_description,
            #     dataset_schema,
            # )
            stream_id, execution_id = client.upload_stream(
                file_name=file_to_load,
                dataset_name=dataset_name,
                insert_method=insert_method,
                dataset_id=dataset_id,
                dataset_description=dataset_description,
                domo_schema=dataset_schema,
            )

            logger.debug(f"Stream id is {stream_id}")
            logger.debug(f"Execution id is {execution_id}")
            logger.info("Successfully loaded all files to Domo")

            # base_folder_name = shipyard.logs.determine_base_artifact_folder("domo")
            # artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
            #     base_folder_name
            # )
            # shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
            # shipyard.logs.create_pickle_file(
            #     artifact_subfolder_paths, "stream_id", stream_id
            # )
            # shipyard.logs.create_pickle_file(
            #     artifact_subfolder_paths, "execution_id", execution_id
            # )

        else:
            # if the schema is provided, then use that otherwise infer the schema using sampling
            if folder_name:
                file_path = shipyard.files.combine_folder_and_file_name(
                    folder_name=folder_name, file_name=file_to_load
                )
            else:
                file_path = file_to_load

            if args.domo_schema != "":
                dataset_schema = utils.make_schema(domo_schema, file_path, folder_name)
            else:
                logger.info("About to infer schema")
                lines = utils.count_lines(file_to_load)
                k = lines if lines < 10000 else 10000
                logger.debug(f"number of lines is {k}")
                dataset_schema = client.infer_schema(file_path, folder_name, k=k)

            logger.info("About to upload data")
            stream_id, execution_id = client.upload_stream(
                file_name=file_to_load,
                dataset_name=dataset_name,
                insert_method=insert_method,
                dataset_id=dataset_id,
                dataset_description=dataset_description,
                domo_schema=dataset_schema,
            )

            logger.debug(f"Stream id is {stream_id}")
            logger.debug(f"Execution id is {execution_id}")
            logger.info(f"Successfully loaded {file_path} to Domo")

            # base_folder_name = shipyard.logs.determine_base_artifact_folder("domo")
            # artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
            #     base_folder_name
            # )
            # shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
            # shipyard.logs.create_pickle_file(
            #     artifact_subfolder_paths, "stream_id", stream_id
            # )
            # shipyard.logs.create_pickle_file(
            #     artifact_subfolder_paths, "execution_id", execution_id
            # )
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()

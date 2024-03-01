import sys
import argparse
import ast
import re
import shipyard_bp_utils as shipyard
from shipyard_domo.domo import DomoClient
from shipyard_domo.utils import utils
from shipyard_bp_utils.artifacts import Artifact
from shipyard_templates import ShipyardLogger, ExitCodeException
from shipyard_domo.utils import exceptions as errs

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-id", dest="client_id", required=True)
    parser.add_argument("--secret-key", dest="secret_key", required=True)
    parser.add_argument("--file-name", dest="file_name", required=True)
    parser.add_argument("--dataset-name", dest="dataset_name", required=True)
    parser.add_argument(
        "--dataset-description", dest="dataset_description", required=False, default=""
    )
    parser.add_argument("--folder-name", dest="folder_name", required=False, default="")
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
    try:
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
        artifacts = Artifact("domo")

        client = DomoClient(client_id=client_id, secret_key=secret)
        logger.info("Successfully connected to Domo")
        if args.domo_schema != "":
            domo_schema = args.domo_schema
            domo_schema = ast.literal_eval(domo_schema)

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
            stream_id, execution_id = client.upload_stream(
                file_name=file_to_load,
                dataset_name=dataset_name,
                insert_method=insert_method,
                dataset_id=dataset_id,
                dataset_description=dataset_description,
                domo_schema=dataset_schema,
            )
            logger.info("Successfully loaded all files to Domo")
            logger.debug("Storing stream_id and execution_id in artifacts")
            artifacts.variables.write("stream_id", "pickle", stream_id)
            artifacts.variables.write("execution_id", "pickle", execution_id)

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
                dataset_schema = client.infer_schema(file_path, folder_name, k=10000)

            stream_id, execution_id = client.upload_stream(
                file_name=file_to_load,
                dataset_name=dataset_name,
                insert_method=insert_method,
                dataset_id=dataset_id,
                dataset_description=dataset_description,
                domo_schema=dataset_schema,
            )

            logger.info(f"Successfully loaded {file_path} to Domo")
            logger.debug("Storing stream_id and execution_id in artifacts")
            artifacts.variables.write("stream_id", "pickle", stream_id)
            artifacts.variables.write("execution_id", "pickle", execution_id)

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(f"Error in uploading file to Domo: {str(e)}")
        sys.exit(errs.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()

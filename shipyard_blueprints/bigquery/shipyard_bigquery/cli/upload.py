import argparse
import ast
import os
import re
import sys

import shipyard_bp_utils as shipyard
from shipyard_templates import ShipyardLogger, ExitCodeException

from shipyard_bigquery import BigQueryClient
from shipyard_bigquery.utils.exceptions import InvalidSchema, SchemaFormatError
from shipyard_bigquery.utils.creds import get_credentials

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", dest="dataset", required=True)
    parser.add_argument("--table", dest="table", required=True)
    parser.add_argument("--service-account", dest="service_account", required=False)
    parser.add_argument(
        "--upload-type",
        dest="upload_type",
        default="append",
        choices={"append", "overwrite"},
        required=False,
    )
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        default="exact_match",
        choices={"exact_match", "regex_match"},
        required=False,
    )
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument("--schema", dest="schema", default="", required=False)
    parser.add_argument(
        "--skip-header-rows", dest="skip_header_rows", default="", required=False
    )
    parser.add_argument(
        "--quoted-newline", dest="quoted_newline", default="FALSE", required=False
    )
    return parser.parse_args()


def main():
    schema = None
    try:
        args = get_args()
        dataset = args.dataset
        table = args.table
        upload_type = args.upload_type
        file_name = args.source_file_name
        folder_name = args.source_folder_name
        full_path = shipyard.files.combine_folder_and_file_name(
            folder_name=f"{os.getcwd()}/{folder_name}", file_name=file_name
        )
        match_type = args.source_file_name_match_type
        schema = None if args.schema == "" else ast.literal_eval(args.schema)
        quoted_newline = args.quoted_newline.strip().upper() == "TRUE"

        skip_header_rows = (
            None if args.skip_header_rows == "" else args.skip_header_rows
        )

        creds = get_credentials()

        if skip_header_rows:
            skip_header_rows = int(args.skip_header_rows)

        if schema and not skip_header_rows:
            logger.warning(
                "Skip Header Rows was not provided but a schema was defined. Setting the Skip Header Rows to 1 to "
                "properly load"
            )
            skip_header_rows = 1

        client = BigQueryClient(**creds)
        client.connect()
        logger.info("Successfully connected to BigQuery")

        if match_type == "regex_match":
            file_names = shipyard.files.find_all_local_file_names(folder_name)
            matching_file_names = shipyard.files.find_all_file_matches(
                file_names, re.compile(file_name)
            )
            if not matching_file_names:
                raise FileNotFoundError(f"No files found matching {file_name}")
            logger.info(
                f"{len(matching_file_names)} files found. Preparing to upload..."
            )

            for index, file_name in enumerate(matching_file_names, start=1):
                logger.info(f"Uploading file {index} of {len(matching_file_names)}")
                client.upload(
                    file=file_name,
                    dataset=dataset,
                    table=table,
                    upload_type=upload_type,
                    skip_header_rows=skip_header_rows,
                    schema=schema,
                    quoted_newline=quoted_newline,
                )
                upload_type = "append"
        else:
            client.upload(
                file=full_path,
                dataset=dataset,
                table=table,
                upload_type=upload_type,
                skip_header_rows=skip_header_rows,
                schema=schema,
                quoted_newline=quoted_newline,
            )
            logger.info(f"Successfully loaded {full_path} to {dataset}.{table}")
    except FileNotFoundError as fe:
        logger.error(str(fe))
        sys.exit(BigQueryClient.EXIT_CODE_FILE_NOT_FOUND)
    except (InvalidSchema, SchemaFormatError, ExitCodeException) as ec:
        logger.error(ec.message)
        if schema:
            logger.warning(
                "Ensure that the data types provided in the schema input are correct. Visit "
                "https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#data_type_properties for "
                "more details"
            )
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(f"Error in uploading file to BigQuery: {str(e)}")
        sys.exit(BigQueryClient.EXIT_CODE_INVALID_UPLOAD_VALUE)


if __name__ == "__main__":
    main()

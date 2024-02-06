import sys
import pandas
import argparse

from shipyard_bp_utils import files
from shipyard_bp_utils.artifacts import Artifact
from shipyard_airtable import AirtableClient
from shipyard_bp_utils.args import convert_to_boolean
from shipyard_templates import ExitCodeException, ShipyardLogger

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", dest="api_key", required=True)
    parser.add_argument("--base-id", dest="base_id", required=True)
    parser.add_argument("--table-id", dest="table_id", required=True)
    parser.add_argument(
        "--filename-or-pattern", dest="filename_or_pattern", required=True
    )
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", required=False, default=""
    )
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        required=True,
        default="exact_match",
    )
    parser.add_argument("--key-fields", dest="key_fields", required=False)
    parser.add_argument("--typecast", dest="typecast", required=False, default="FALSE")
    parser.add_argument("--insert-method", dest="insert_method", required=True)
    return parser.parse_args()


def prepare_data_from_csv(file):
    logger.debug(f"Preparing data from file: {file}")
    records = []
    df = pandas.read_csv(file)

    for index, row in df.iterrows():
        record = {"fields": row.to_dict()}
        if "id" in record["fields"]:
            del record["fields"]["id"]
        records.append(record)
    logger.debug(f"Following records are prepared: \n {records}")
    return records


def main():
    try:
        artifact = Artifact("airtable")
        args = get_args()

        typecast = convert_to_boolean(args.typecast) if args.typecast else False
        search_for = args.filename_or_pattern
        match_type = args.source_file_name_match_type
        source_folder = args.source_folder_name or "."
        key_fields = args.key_fields = args.key_fields.split(",")
        upload_method = args.insert_method.lower()

        files_found = files.find_matching_files(
            args.filename_or_pattern, source_folder, match_type
        )

        client = AirtableClient(api_key=args.api_key)
        if upload_method == "replace":
            client.clear_table(args.base_id, args.table_id)
            upload_method = "append"

        if len(files_found) == 0:
            logger.error(
                f"No files found matching search criteria: {search_for} in {source_folder} "
                f"with match type: {match_type}"
            )
            sys.exit(client.EXIT_CODE_FILE_NOT_FOUND)

        failed, success = [], []
        logger.info(f"Uploading {len(files_found)} file(s) to Airtable...")

        for file in files_found:

            try:
                upload_args = {
                    "base": args.base_id,
                    "table": args.table_id,
                    "data": prepare_data_from_csv(file),
                    "typecast": typecast,
                    "upload_method": upload_method,
                }
                if key_fields:
                    upload_args["key_fields"] = key_fields
                client.upload(**upload_args)

            except Exception as e:
                logger.error(f"Error uploading file: {file} to Airtable: {e}")
                failed.append(
                    {
                        "file": file,
                        "error": f"Error uploading file: {file} to Airtable: {e}",
                    }
                )

            else:
                success.append(file)
                logger.info(f"Uploaded {file} to Airtable successfully.")

    except ExitCodeException as err:
        logger.error(err.message)
        sys.exit(err.exit_code)
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        sys.exit(1)

    else:
        artifact.logs.write_json(
            "airtable_upload_status", {"success": success, "failed": failed}
        )
        if success:
            logger.info(f"Uploaded {len(success)} file(s) to Airtable successfully.")
        if failed:
            logger.error(f"Failed to upload {len(failed)} file(s) to Airtable.")
            sys.exit(client.EXIT_CODE_UPLOAD_ERROR)


if __name__ == "__main__":
    main()

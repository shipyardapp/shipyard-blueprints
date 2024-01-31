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
    parser.add_argument("--table-name", dest="table_name", required=True)
    parser.add_argument("--view-name", dest="view_name", required=False)
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
        choices={"exact_match", "regex_match"},
    )
    parser.add_argument("--key-fields", dest="key_fields", required=True)
    parser.add_argument("--typecast", dest="typecast", required=False, default="FALSE")
    return parser.parse_args()


def main():
    try:
        artifact = Artifact("airtable")
        args = get_args()

        typecast = convert_to_boolean(args.typecast) if args.typecast else False
        search_for = args.filename_or_pattern
        match_type = args.source_file_name_match_type
        if args.source_folder_name:
            source_path = files.combine_folder_and_file_name(
                args.source_folder_name, args.source_file_name
            )
        else:
            source_path = args.source_file_name
        client = AirtableClient(api_key=args.api_key)
        file_matches = files.find_matching_files(
            args.filename_or_pattern, source_path, match_type
        )
        if len(file_matches) == 0:
            logger.error(
                f"No files found matching search criteria: {search_for} in {source_path} with match type: {match_type}"
            )
            sys.exit(client.EXIT_CODE_FILE_NOT_FOUND)
        failed, success = [], []
        logger.info(f"Uploading {len(file_matches)} file(s) to Airtable...")
        for file in file_matches:
            try:
                records = []
                df = pandas.read_csv(file)

                for index, row in df.iterrows():
                    record = {"fields": row.to_dict()}
                    if "id" in record["fields"]:
                        del record["fields"]["id"]
                    records.append(record)

                client.upload(
                    args.base_id, args.table_name, records, args.key_fields, typecast
                )
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

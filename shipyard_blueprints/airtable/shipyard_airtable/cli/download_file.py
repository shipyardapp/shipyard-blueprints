import sys
import argparse
import pandas as pd

from shipyard_airtable import AirtableClient
from shipyard_bp_utils.artifacts import Artifact
from shipyard_bp_utils.args import convert_to_boolean
from shipyard_templates import ShipyardLogger, ExitCodeException
from shipyard_bp_utils.files import (
    combine_folder_and_file_name,
    create_folder_if_dne,
    clean_folder_name,
)

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-id", dest="base_id", default="", required=True)
    parser.add_argument("--table-name", dest="table_name", default=None, required=True)
    parser.add_argument("--view-name", dest="view_name", default=None, required=False)
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default=None,
        required=True,
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    parser.add_argument("--api-key", dest="api_key", default=None, required=True)
    parser.add_argument(
        "--include-record-id",
        dest="include_record_id",
        default="TRUE",
        required=False,
    )
    return parser.parse_args()


def main():
    try:
        artifact = Artifact("airtable")

        args = get_args()
        api_key = args.api_key
        table_name = args.table_name
        base_id = args.base_id
        view_name = args.view_name
        include_record_id = convert_to_boolean(args.include_record_id)

        destination_file_name = clean_folder_name(args.destination_file_name)
        destination_folder_name = clean_folder_name(args.destination_folder_name)

        destination_full_path = combine_folder_and_file_name(
            folder_name=destination_folder_name, file_name=destination_file_name
        )

        if destination_folder_name:
            create_folder_if_dne(destination_folder_name)
        airtable_client = AirtableClient(api_key)
        records = airtable_client.fetch(base_id, table_name, view_name)

        df = pd.DataFrame.from_records(row["fields"] for row in records)
        if include_record_id:
            for index, row in enumerate(records):
                df.at[index, "airtable_record_id"] = row["id"]
        df.to_csv(destination_full_path, index=False)

        # Redundant but used for consistency so that future blueprints can easily access the data
        artifact.variables.write_csv(destination_file_name, df)
    except ExitCodeException as err:
        logger.error(err)
        sys.exit(err.exit_code)
    except Exception as error:
        logger.error(f"An unexpected error occurred: {error}")
        sys.exit(1)
    else:

        logger.info(
            f"Successfully stored the contents of Base:{base_id} "
            f"Table:{table_name} View:{view_name} as {destination_full_path}"
        )


if __name__ == "__main__":
    main()

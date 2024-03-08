import sys
import argparse
import shipyard_bp_utils as shipyard
import requests
import pandas as pd
import io
from shipyard_domo import DomoClient
from shipyard_domo.utils import exceptions as errs
from shipyard_templates import ShipyardLogger, ExitCodeException

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--domo-instance", dest="domo_instance", required=True)
    parser.add_argument("--card-id", dest="card_id", required=True)
    parser.add_argument(
        "--destination-file-name", dest="destination_file_name", required=True
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    parser.add_argument(
        "--file-type", dest="file_type", choices={"ppt", "csv", "excel"}, required=True
    )
    parser.add_argument("--developer-token", dest="developer_token", required=True)
    args = parser.parse_args()

    return args


def write_file(file_type: str, data: requests.Response, target: str):
    if file_type == "csv":
        pd.read_csv(io.StringIO(data.content.decode("utf-8"))).to_csv(
            target, index=False
        )
    else:
        with open(target, "wb") as fd:
            # iterate through the blob 1MB at a time
            for chunk in data.iter_content(1024 * 1024):
                fd.write(chunk)


def main():
    try:
        args = get_args()
        card_id = args.card_id
        folder_path = args.destination_folder_name
        file_type = args.file_type
        domo_instance = args.domo_instance
        if folder_path:
            shipyard.files.create_folder_if_dne(folder_path)
            file_path = shipyard.files.combine_folder_and_file_name(
                folder_path, args.destination_file_name
            )
        else:
            file_path = args.destination_file_name

        client = DomoClient(
            access_token=args.developer_token, domo_instance=domo_instance
        )
        client.connect_with_access_token()

        card = client.fetch_card_data(card_id=card_id)[0]
        logger.info("Successfully fetched card data, beginning the export")
        logger.debug(f"Contents of card are: {card}")

        # export if card type is 'graph'
        if (card_type := card["type"]) == "kpi":
            response = client.export_card(
                card_id, file_path=file_path, file_type=file_type
            )
            logger.debug(f"Contents of data are {response.text}")
        else:
            logger.error(
                f"Card type {card_type} not supported by system, only type `kpi` can be exported"
            )
            sys.exit(errs.EXIT_CODE_INCORRECT_CARD_TYPE)

        if response.ok:
            write_file(file_type, response, file_path)
            logger.info(f"Successfully downloaded card contents to {file_path}")
        else:
            logger.error(
                f"Error in downloading card. Response from API reads: {response.text}"
            )

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(
            f"An error occured when attemtping to download card from Domo. Message from the API: {str(e)}"
        )
        sys.exit(errs.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()

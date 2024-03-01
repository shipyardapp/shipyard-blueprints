import sys
import argparse
import shipyard_bp_utils as shipyard
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
        client.connect()

        card = client.fetch_card_data(card_id=card_id)
        logger.info("Successfully fetched card data, beginning the export")

        # export if card type is 'graph'
        if (card_type := card["type"]) == "kpi":
            client.export_card(card_id, file_path=file_path, file_type=file_type)
        else:
            logger.error(
                f"Card type {card_type} not supported by system, only type `kpi` can be exported"
            )
            sys.exit(errs.EXIT_CODE_INCORRECT_CARD_TYPE)

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

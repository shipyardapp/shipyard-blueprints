import argparse
import sys

from shipyard_fivetran import FivetranClient
from shipyard_templates import ExitCodeException


def get_args():
    parser = argparse.ArgumentParser(
        description="Update a connector using FivetranClient"
    )
    parser.add_argument("access_token", type=str, help="Fivetran API access token")
    parser.add_argument("api_secret", type=str, help="Fivetran API secret")
    parser.add_argument("connector_id", type=str, help="ID of the connector")
    parser.add_argument(
        "--schedule-type",
        choices=["manual", "auto"],
        help="Schedule type for the connector (optional)",
    )
    parser.add_argument(
        "--pause",
        action="store_true",
        help="Pause the connector (optional)",
    )
    parser.add_argument(
        "--historical-sync",
        action="store_true",
        help="Perform a historical sync (optional)",
    )

    return parser.parse_args()


def main():
    args = get_args()

    args_dict = vars(args)
    # Remove arguments with empty string values
    args_dict = {k: v for k, v in args_dict.items() if v is not None}
    if args_dict.get("pause"):
        args_dict["pause"] = args_dict["pause"].upper() == "TRUE"
    if args_dict.get("historical_sync"):
        args_dict["historical_sync"] = args_dict["historical_sync"].upper() == "TRUE"
    access_token = args_dict.pop("access_token")
    api_secret = args_dict.pop("api_secret")
    connector_id = args_dict.pop("connector_id")

    fivetran_client = FivetranClient(access_token, api_secret)

    try:
        fivetran_client.update_connector(
            connector_id,
            **args_dict,
        )
    except ExitCodeException as e:
        fivetran_client.logger.error(e)
        sys.exit(e.exit_code)


if __name__ == "__main__":
    main()
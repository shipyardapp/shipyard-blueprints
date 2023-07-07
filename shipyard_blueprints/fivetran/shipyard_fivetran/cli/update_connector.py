import argparse
import sys
import json

from shipyard_fivetran import FivetranClient
from shipyard_templates import ExitCodeException


def get_args():
    parser = argparse.ArgumentParser(
        description="Update a connector using FivetranClient"
    )
    parser.add_argument("--api-key", type=str, help="Fivetran API access token")
    parser.add_argument("--api-secret", type=str, help="Fivetran API secret")
    parser.add_argument("--connector-id", type=str, help="ID of the connector")
    parser.add_argument(
        "--schedule-type",
        help="Schedule type for the connector (optional)",
    )
    parser.add_argument(
        "--paused",
        help="Pause the connector (optional)",
    )
    parser.add_argument(
        "--historical-sync",
        help="Perform a historical sync (optional)",
    )
    parser.add_argument(
        "--custom-update", dest="custom_update", default="None", required=False
    )

    return parser.parse_args()


def main():
    args = get_args()

    args_dict = vars(args)
    if args_dict["schedule_type"]=='None':
        args_dict.pop("schedule_type")
    # Remove arguments with empty string values
    args_dict = {k: v for k, v in args_dict.items() if v is not None and v != ""}
    if args_dict.get("paused"):
        args_dict["paused"] = args_dict["paused"].upper() == "TRUE"
    if args_dict.get("historical_sync"):
        args_dict["historical_sync"] = args_dict["historical_sync"].upper() == "TRUE"
    if args_dict.get("custom_update"):
        additional_update = args_dict.pop("custom_update")
        additional_update= json.loads(additional_update)
        args_dict["additional_details"]=additional_update

    access_token = args_dict.pop("api_key")
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
import argparse
import sys

from shipyard_fivetran import FivetranClient
from shipyard_templates import ExitCodeException


def get_args():
    parser = argparse.ArgumentParser(description="Trigger a sync using FivetranClient")
    parser.add_argument("access_token", type=str, help="Fivetran API access token")
    parser.add_argument("api_secret", type=str, help="Fivetran API secret")
    parser.add_argument("connector_id", type=str, help="ID of the connector")
    parser.add_argument(
        "--force",
        default="TRUE",
        help="Force the sync (optional, default: True)",
    )
    parser.add_argument(
        "--wait",
        default="FALSE",
        help="Wait for the sync to complete (optional, default: False)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Interval in seconds to check for sync completion (optional, default: 30)",
    )

    return parser.parse_args()


def main():
    args = get_args()

    force_sync = args.force
    wait_for_completion = args.wait.upper() == "TRUE"
    force_sync = force_sync.upper() == "TRUE"
    poke_interval = args.interval

    fivetran_client = FivetranClient(args.access_token, args.api_secret)

    try:
        fivetran_client.trigger_sync(
            args.connector_id,
            force=force_sync,
            wait_for_completion=wait_for_completion,
            poke_interval=poke_interval,
        )
    except ExitCodeException as e:
        fivetran_client.logger.error(e)
        sys.exit(e.exit_code)


if __name__ == "__main__":
    main()

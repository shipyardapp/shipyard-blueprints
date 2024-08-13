import argparse
import sys

from shipyard_templates import ShipyardLogger
from shipyard_templates.exit_code_exception import ExitCodeException

from shipyard_microsoft_power_bi import MicrosoftPowerBiClient, utils

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-id", dest="client_id", required=False)
    parser.add_argument("--client-secret", dest="client_secret", required=False)
    parser.add_argument("--tenant-id", dest="tenant_id", required=False)
    parser.add_argument("--group-id", dest="group_id", required=True)
    parser.add_argument("--object-type", dest="object_type", required=True)
    parser.add_argument("--object-id", dest="object_id", required=True)
    parser.add_argument(
        "--wait-for-completion", dest="wait_for_completion", required=True
    )
    parser.add_argument("--poke-interval", dest="poke_interval", required=False)

    return parser.parse_args()


def main():
    try:
        args = get_args()
        wait_for_completion, wait_time = utils.validate_args(args)

        credentials = utils.get_credential_group(args)
        client = MicrosoftPowerBiClient(**credentials)

        client.refresh(
            object_type=args.object_type,
            object_id=args.object_id,
            group_id=args.group_id,
            wait_for_completion=wait_for_completion,
            wait_time=wait_time,
        )
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(MicrosoftPowerBiClient.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()

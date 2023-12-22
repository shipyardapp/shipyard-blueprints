import sys
import argparse
from shipyard_microsoft_power_bi import MicrosoftPowerBiClient
from shipyard_templates.exit_code_exception import ExitCodeException


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-id", dest="client_id", required=True)
    parser.add_argument("--client-secret", dest="client_secret", required=True)
    parser.add_argument("--tenant-id", dest="tenant_id", required=True)
    parser.add_argument("--group-id", dest="group_id", required=True)
    parser.add_argument("--object-type", dest="object_type", required=True)
    parser.add_argument("--object-id", dest="object_id", required=True)
    parser.add_argument(
        "--wait-for-completion", dest="wait_for_completion", required=True
    )
    parser.add_argument("--poke-interval", dest="poke_interval", required=False)

    return parser.parse_args()


def validate_args(args):
    """
    Handle datatype conversions and validations for arguments

    @param args: Namespace object containing arguments
    @return: Tuple containing wait_for_completion and wait_time
    """

    if args.poke_interval is None or args.poke_interval == "":
        wait_time = 1
    else:
        wait_time = int(args.poke_interval) * 60

    if wait_time < 0:
        raise ExitCodeException(
            "Error: poke-interval must be greater than 0",
            MicrosoftPowerBiClient.EXIT_CODE_INVALID_INPUT,
        )
    elif wait_time > 60:
        raise ExitCodeException(
            "Error: poke-interval must be less than or equal to 60",
            MicrosoftPowerBiClient.EXIT_CODE_INVALID_INPUT,
        )

    wait_for_completion = None
    if type(args.wait_for_completion) is str:
        if args.wait_for_completion.upper() == "TRUE":
            wait_for_completion = True
        elif args.wait_for_completion.upper() == "FALSE":
            wait_for_completion = False

    elif type(args.wait_for_completion) is bool:
        wait_for_completion = args.wait_for_completion

    return wait_for_completion, wait_time


def main():
    try:  # Initialize client to ensure client.logger is available
        args = get_args()

        client = MicrosoftPowerBiClient(
            client_id=args.client_id,
            client_secret=args.client_secret,
            tenant_id=args.tenant_id,
        )
    except ExitCodeException as e:
        print(e)
        sys.exit(e.exit_code)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(MicrosoftPowerBiClient.EXIT_CODE_UNKNOWN_ERROR)

    try:
        wait_for_completion, wait_time = validate_args(args)

        client.refresh(
            object_type=args.object_type,
            object_id=args.object_id,
            group_id=args.group_id,
            wait_for_completion=wait_for_completion,
            wait_time=wait_time,
        )
    except ExitCodeException as e:
        client.logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        client.logger.error(f"Unexpected error: {e}")
        sys.exit(client.EXIT_CODE_UNKNOWN_ERROR)

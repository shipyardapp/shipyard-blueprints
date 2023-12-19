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


def main():
    args = get_args()

    wait_for_completion = None
    if type(args.wait_for_completion) is str:
        if args.wait_for_completion.upper() == "TRUE":
            wait_for_completion = True
        elif args.wait_for_completion.upper() == "FALSE":
            wait_for_completion = False
    elif type(args.wait_for_completion) is bool:
        wait_for_completion = args.wait_for_completion

    try:
        client = MicrosoftPowerBiClient(
            client_id=args.client_id,
            client_secret=args.client_secret,
            tenant_id=args.tenant_id,
        )

        client.refresh(
            object_type=args.object_type,
            object_id=args.object_id,
            group_id=args.group_id,
            wait_for_completion=wait_for_completion,
            wait_time=int(args.poke_interval),
        )
    except ExitCodeException as e:
        print(e)
        sys.exit(e.exit_code)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

import argparse
import time
import sys

from shipyard_rudderstack import RudderStackClient
from shipyard_templates import ExitCodeException


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--source-id", dest="source_id", required=True)
    parser.add_argument("--wait-for-completion", default="FALSE")
    parser.add_argument("--poke-interval", default=1)
    return parser.parse_args()


def main():
    args = get_args()
    access_token = args.access_token
    source_id = args.source_id

    rudderstack = RudderStackClient(access_token=access_token)
    # execute trigger sync
    try:
        rudderstack.trigger_sync(source_id)
    except ExitCodeException as e:
        rudderstack.logger.error(e.message)
        sys.exit(e.exit_code)

    # create artifacts folder to save run id
    if args.wait_for_completion == "TRUE":
        poke = args.poke_interval
        if 0 < int(poke) <= 60:
            rudderstack.logger.info(f"Setting poke interval to {poke} minutes")
        else:
            rudderstack.logger.error("Poke interval must be between 1 and 60 minutes")
            sys.exit(rudderstack.EXIT_CODE_INVALID_POKE_INTERVAL)

        status = "processing"
        try:
            while status == "processing":
                rudderstack.logger.info(
                    f"Waiting {args.poke_interval} {'minutes' if args.poke_interval > 1 else 'minute'} to check sync status..."
                )
                time.sleep(args.poke_interval * 60)
                status = rudderstack.determine_sync_status(source_id)
                rudderstack.logger.info(f"Sync status: {status}")
        except ExitCodeException as e:
            sys.exit(e.exit_code)


if __name__ == "__main__":
    main()

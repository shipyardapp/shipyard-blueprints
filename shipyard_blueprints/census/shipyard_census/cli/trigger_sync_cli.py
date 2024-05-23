import argparse
import os
import sys
import time

from shipyard_bp_utils.artifacts import Artifact
from shipyard_templates import ExitCodeException, ShipyardLogger

from shipyard_census import CensusClient

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--sync-id", dest="sync_id", required=True)
    parser.add_argument("--wait-for-completion", default="FALSE")
    parser.add_argument("--poke-interval", default=1)
    return parser.parse_args()


def main():
    args = get_args()
    census = CensusClient(args.access_token)
    try:
        trigger_sync = census.trigger_sync(args.sync_id)
    except ExitCodeException as error:
        logger.error(error)
        sys.exit(error.exit_code)

    sync_run_id = trigger_sync["data"]["sync_run_id"]
    wait = args.wait_for_completion.upper() == "TRUE"
    if wait:
        poke_interval = args.poke_interval
        if poke_interval == "" or poke_interval is None:
            logger.error(
                "Poke interval must be specified when waiting for sync completion"
            )
            sys.exit(census.EXIT_CODE_SYNC_INVALID_POKE_INTERVAL)

        if 0 < int(poke_interval) <= 60:
            logger.info(f"Setting poke interval to {poke_interval} minute(s)")
            poke_interval = int(poke_interval)
        else:
            logger.error("Poke interval must be between 1 and 60 minutes")
            sys.exit(census.EXIT_CODE_SYNC_INVALID_POKE_INTERVAL)

        status = None
        while status != "completed":
            try:
                sync_run_data = census.get_sync_status(sync_run_id)
                status = census.determine_sync_status(sync_run_data)
                if status != "completed":
                    logger.info(
                        f"Waiting {poke_interval} minute(s) to check sync status..."
                    )
                    time.sleep(poke_interval * 60)
            except ExitCodeException as error:
                logger.error(error)
                sys.exit(error.exit_code)

    elif not wait and int(os.environ.get("SHIPYARD_FLEET_DOWNSTREAM_COUNT")) > 0:
        artifact = Artifact("census")
        # Create the artifact for legacy blueprints to continue to work
        artifact.variables.create_pickle("sync_run_id", sync_run_id)


if __name__ == "__main__":
    main()

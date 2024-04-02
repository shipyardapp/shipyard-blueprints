import argparse
import sys
import time
import shipyard_bp_utils as shipyard
from shipyard_templates import ShipyardLogger, ExitCodeException, Etl
from shipyard_portable import PortableClient

logger = ShipyardLogger.get_logger()
INTERVAL = 60


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--flow-id", dest="flow_id", required=True)
    parser.add_argument(
        "--wait-for-completion",
        dest="wait_for_completion",
        required=False,
        default="TRUE",
    )

    return parser.parse_args()


def main():
    try:
        args = get_args()
        flow_id = args.flow_id
        portable = PortableClient(args.access_token)
        portable.connect()
        flow_response = portable.trigger_sync(args.flow_id)

        wait = shipyard.args.convert_to_boolean(args.wait_for_completion)
        flow_status_data = portable.get_sync_status(flow_id)
        flow_disposition = flow_status_data["disposition"]
        exit_code = portable.determine_sync_status(flow_status_data)
        if wait:
            logger.info("Waiting for flow to complete")
            while flow_disposition not in ("SUCCEEDED", "FAILED"):
                flow_status = portable.get_sync_status(args.flow_id)
                flow_disposition = flow_status["disposition"]
                exit_code = portable.determine_sync_status(flow_status)
                time.sleep(INTERVAL)

        logger.info(f"Flow has a status of {flow_disposition}, exiting now")

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(Etl.EXIT_CODE_UNKNOWN_ERROR)
    else:
        sys.exit(exit_code)


if __name__ == "__main__":
    main()

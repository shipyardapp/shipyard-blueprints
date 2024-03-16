import sys
import time
import argparse
import shipyard_bp_utils as shipyard
from shipyard_coalesce import CoalesceClient
from shipyard_bp_utils.artifacts import Artifact
from shipyard_templates import ExitCodeException, ShipyardLogger, Etl
from ast import literal_eval

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--environment-id", dest="environment_id", required=True)
    parser.add_argument("--job-id", dest="job_id", required=False, default=None)
    parser.add_argument(
        "--snowflake-username", dest="snowflake_username", required=True
    )
    parser.add_argument(
        "--snowflake-password", dest="snowflake_password", required=True
    )
    parser.add_argument("--snowflake-role", dest="snowflake_role", required=False)
    parser.add_argument(
        "--snowflake-warehouse", dest="snowflake_warehouse", required=True
    )
    parser.add_argument("--parallelism", dest="parallelism", required=False, default=16)
    parser.add_argument(
        "--include-nodes-selector", dest="include_nodes", required=False, default=""
    )
    parser.add_argument(
        "--exclude-nodes-selector", dest="exclude_nodes", required=False, default=""
    )
    parser.add_argument("--wait-for-completion", dest="wait_for_completion")
    parser.add_argument("--poke-interval", dest="poke_interval", default=1)
    parser.add_argument("--parameters", dest="parameters", required=False, default="")

    return parser.parse_args()


def main():
    try:
        args = get_args()
        access_token = args.access_token
        wait = shipyard.args.convert_to_boolean(args.wait_for_completion)
        sync_args = {
            "environment_id": args.environment_id,
            "job_id": None if args.job_id == "" else args.job_id,
            "snowflake_username": args.snowflake_username,
            "snowflake_password": args.snowflake_password,
            "snowflake_role": None
            if args.snowflake_role == ""
            else args.snowflake_role,
            "snowflake_warehouse": (
                None if args.snowflake_role == "" else args.snowflake_warehouse
            ),
            "parallelism": args.parallelism,
            "include_nodes_selector": (
                None if args.include_nodes == "" else args.include_nodes
            ),
            "exclude_nodes_selector": (
                None if args.exclude_nodes == "" else args.exclude_nodes
            ),
            "parameters": None
            if args.parameters == ""
            else literal_eval(args.parameters),
        }
        client = CoalesceClient(access_token)
        response_json = client.trigger_sync(**sync_args)
        if wait and (0 < int(args.poke_interval) <= 60):
            run_id = response_json["runCounter"]
            status = client.determine_sync_status(run_id)

            while status not in (
                client.EXIT_CODE_FINAL_STATUS_COMPLETED,
                client.EXIT_CODE_FINAL_STATUS_CANCELLED,
                client.EXIT_CODE_FINAL_STATUS_ERRORED,
            ):
                logger.info(f"Waiting {args.poke_interval} minute(s)...")
                time.sleep(int(args.poke_interval) * 60)
                status = client.determine_sync_status(run_id)

            sys.exit(status)
        elif wait:
            logger.error("Poke interval must be between 1 and 60 minutes")
            sys.exit(client.EXIT_CODE_SYNC_INVALID_POKE_INTERVAL)
        else:
            Artifact("coalesce").responses.create_pickle(
                "coalesce_response", response_json
            )
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to trigger job. Message from Coalesce: {e}"
        )
        sys.exit(Etl.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()

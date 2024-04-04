import argparse
import sys

from shipyard_templates import ShipyardLogger, ExitCodeException, DataVisualization
from shipyard_looker import LookerClient
from shipyard_bp_utils.artifacts import Artifact

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", dest="base_url", required=True)
    parser.add_argument("--client-id", dest="client_id", required=True)
    parser.add_argument("--client-secret", dest="client_secret", required=True)
    parser.add_argument(
        "--connection-target",
        dest="connection_target",
        required=True,
        choices={"connection_name", "model_name"},
    )
    parser.add_argument("--connection-value", dest="connection_value", required=True)
    parser.add_argument("--sql", dest="sql", required=True)
    args = parser.parse_args()
    return args


def main():
    try:
        args = get_args()
        base_url = args.base_url
        client_id = args.client_id
        client_secret = args.client_secret
        connection_target = args.connection_target
        connection_value = args.connection_value
        sql = args.sql

        looker = LookerClient(
            base_url=base_url, client_id=client_id, client_secret=client_secret
        )

        # create SQL Query
        if connection_target == "connection_name":
            new_slug = looker.create_sql_query(
                sql_body, connection_name=connection_value
            )
        elif connection_target == "model_name":
            new_slug = looker.create_sql_query(sql_body, model_name=connection_value)

        logger.info(f"Looker slug {new_slug} created successfully")
        # save slug to pickle file
        artifact = Artifact("looker")
        artifact.variables.write("slug", "pickle", new_slug)

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(DataVisualization.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()

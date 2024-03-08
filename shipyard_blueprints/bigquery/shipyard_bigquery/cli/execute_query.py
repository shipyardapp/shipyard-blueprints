import argparse
import sys
from shipyard_templates import ShipyardLogger, ExitCodeException
from shipyard_bigquery import BigQueryClient
from shipyard_bigquery.utils.exceptions import QueryError, EXIT_CODE_QUERY_ERROR

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", dest="query", required=True)
    parser.add_argument("--service-account", dest="service_account", required=True)
    args = parser.parse_args()
    return args


def main():
    try:
        args = get_args()
        query = args.query
        logger.debug(f"Query is {query}")
        client = BigQueryClient(args.service_account)
        client.connect()
        logger.info("Successfully connected to BigQuery")
        logger.debug(f"Service account email is {client.email}")
        client.execute_query(query)
    except (QueryError, ExitCodeException) as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(f"Error in executing query: {str(e)}")
        sys.exit(EXIT_CODE_QUERY_ERROR)
    else:
        logger.info("Successfully executed query")


if __name__ == "__main__":
    main()

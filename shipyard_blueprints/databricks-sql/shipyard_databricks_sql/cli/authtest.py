import os
import sys
from shipyard_databricks_sql import DatabricksSqlClient
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def main():
    try:
        conn = DatabricksSqlClient(
            access_token=os.getenv("DATABRICKS_SQL_ACCESS_TOKEN"),
            server_host=os.getenv("DATABRICKS_SERVER_HOST"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        ).connect()
    except Exception as e:
        logger.authtest(
            f"Error in connecting to Databricks with provided credentials. Message: {e}"
        )
        sys.exit(1)

    else:
        conn.close()
        sys.exit(0)


if __name__ == "__main__":
    main()

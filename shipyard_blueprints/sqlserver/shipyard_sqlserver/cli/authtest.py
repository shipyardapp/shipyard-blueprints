import os
import sys
from shipyard_sqlserver import SqlServerClient
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def main():
    try:
        client = SqlServerClient(
            user=os.getenv("MSSQL_USERNAME"),
            pwd=os.getenv("MSSQL_PASSWORD"),
            host=os.getenv("MSSQL_HOST"),
            database=os.getenv("MSSQL_DATABASE"),
            port=os.getenv("MSSQL_PORT"),
        )
        client.connect()
    except Exception as e:
        logger.authtest(
            f"Error in connecting to SQL Server. Message from the server reads: {e}"
        )
        sys.exit(1)
    else:
        logger.authtest("Successfully connected to SQL Server")
        sys.exit(0)


if __name__ == "__main__":
    main()

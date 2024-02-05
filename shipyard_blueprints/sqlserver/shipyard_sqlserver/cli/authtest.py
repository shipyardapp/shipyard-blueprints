import os
import sys
from shipyard_sqlserver import SqlServerClient


def get_args():
    return {
        "user": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "host": os.getenv("MSSQL_HOST"),
        "database": os.getenv("MSSQL_DATABASE"),
        "port": os.getenv("MSSQL_PORT"),
    }


def main():
    args = get_args()
    host = args["host"]
    user = args["user"]
    pwd = args["password"]
    port = args["port"]
    database = args["database"]
    sqlserver = SqlServerClient(
        user=user, pwd=pwd, host=host, port=port, database=database
    )
    try:
        con = sqlserver.connect()
        sqlserver.logger.info("Successfully connected to Sql Server")
        sys.exit(0)
    except Exception as e:
        sqlserver.logger.error("Could not connect to Sql Server with given credentials")
        sys.exit(1)


if __name__ == "__main__":
    main()

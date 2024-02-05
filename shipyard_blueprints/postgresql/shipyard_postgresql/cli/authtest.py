import os
import sys
from shipyard_postgresql import PostgresqlClient


def get_args():
    return {
        "host": os.environ.get("POSTGRES_HOST"),
        "username": os.environ.get("POSTGRES_USERNAME"),
        "password": os.environ.get("POSTGRES_PASSWORD"),
        "port": os.environ.get("POSTGRES_PORT"),
        "database": os.environ.get("POSTGRES_DATABASE"),
    }


def main():
    args = get_args()
    host = args["host"]
    user = args["username"]
    pwd = args["password"]
    port = args["port"]
    database = args["database"]
    postgres = PostgresqlClient(
        user=user, pwd=pwd, host=host, port=port, database=database
    )
    try:
        con = postgres.connect()
        postgres.logger.info("Successfully established connection")
        sys.exit(0)
    except Exception as e:
        postgres.logger.error("Could not connect to postgres with given credentials")
        sys.exit(1)


if __name__ == "__main__":
    main()

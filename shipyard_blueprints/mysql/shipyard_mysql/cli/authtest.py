import os
import sys
from shipyard_mysql import MySqlClient


def get_args():
    return {
        "host": os.environ.get("MYSQL_HOST"),
        "port": os.environ.get("MYSQL_PORT"),
        "username": os.environ.get("MYSQL_USERNAME"),
        "password": os.environ.get("MYSQL_PASSWORD"),
        "database": os.environ.get("MYSQL_DATABASE"),
    }


def main():
    args = get_args()
    host = args["host"]
    user = args["username"]
    password = args["password"]
    port = args["port"]
    database = args["database"]
    mysql = MySqlClient(
        username=user, pwd=password, host=host, port=port, database=database
    )
    try:
        con = mysql.connect()
        mysql.logger.info(f"Connected to MySQL server {host}")
        sys.exit(0)
    except Exception as e:
        mysql.logger.error(
            f"Could not connect to MySQL server {host} with the provided credentials"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()

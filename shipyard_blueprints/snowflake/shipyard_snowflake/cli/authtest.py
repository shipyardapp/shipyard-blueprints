import argparse
import os
from shipyard_blueprints import SnowflakeClient
from shipyard_templates import ExitCodeException


def get_args():
    args = {}
    args["user"] = os.getenv("SNOWFLAKE_USERNAME")
    args["password"] = os.getenv("SNOWFLAKE_PASSWORD")
    args["account"] = os.getenv("SNOWFLAKE_ACCOUNT")
    return args


def main():
    args = get_args()
    user = args["user"]
    pwd = args["password"]
    account = args["account"]

    snowflake = SnowflakeClient(username=user, pwd=pwd, account=account)
    try:
        conn = snowflake.connect()
        snowflake.logger.info("Successfully connected to Snowflake")
        return 0
    except ExitCodeException as e:
        snowflake.logger.error("Could not connect to Snowflake")
        return 1


if __name__ == "__main__":
    main()

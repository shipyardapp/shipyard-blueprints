import os
import sys
from shipyard_snowflake import SnowflakeClient
from shipyard_templates import ExitCodeException


def main():
    snowflake = SnowflakeClient(
        username=os.getenv("SNOWFLAKE_USERNAME"),
        pwd=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
    )
    try:
        conn = snowflake.connect()
        snowflake.logger.info("Successfully connected to Snowflake")
        sys.exit(0)
    except ExitCodeException as e:
        snowflake.logger.error("Could not connect to Snowflake")
        sys.exit(1)


if __name__ == "__main__":
    main()

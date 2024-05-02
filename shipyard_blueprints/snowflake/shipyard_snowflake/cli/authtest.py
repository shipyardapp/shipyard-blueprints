import os
import sys
from shipyard_snowflake import SnowflakeClient
from shipyard_templates import ExitCodeException, ShipyardLogger

logger = ShipyardLogger.get_logger()


def main():
    snowflake = SnowflakeClient(
        username=os.getenv("SNOWFLAKE_USERNAME"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
    )
    try:
        conn = snowflake.connect()
        sys.exit(0)
    except ExitCodeException as e:
        logger.authtest(f"Credential test failed for the following reason: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

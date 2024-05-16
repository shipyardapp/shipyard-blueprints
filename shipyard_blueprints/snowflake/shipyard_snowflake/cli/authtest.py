import os
import sys
from shipyard_snowflake import SnowflakeClient
from shipyard_templates import ExitCodeException, ShipyardLogger

logger = ShipyardLogger.get_logger()


def main():
    # NOTE: When using a private key, ensure the private key passphrase is set via the environment variable
    # 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'. The passphrase is required but not directly set in the code.

    snowflake = SnowflakeClient(
        username=os.getenv("SNOWFLAKE_USERNAME"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        rsa_key=os.getenv("SNOWFLAKE_PRIVATE_KEY"),
    )
    try:
        snowflake.connect()
        sys.exit(0)
    except ExitCodeException as e:
        logger.authtest(f"Credential test failed for the following reason: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

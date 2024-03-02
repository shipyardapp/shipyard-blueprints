import os
import sys

from shipyard_templates import ExitCodeException, ShipyardLogger
from shipyard_domo import DomoClient

logger = ShipyardLogger.get_logger()


def main():
    try:
        client = DomoClient(
            client_id=os.getenv("DOMO_CLIENT_ID"),
            secret_key=os.getenv("DOMO_SECRET_KEY"),
            access_token=os.getenv("DOMO_ACCESS_TOKEN"),
            domo_instance=os.getenv("DOMO_INSTANCE"),
        ).connect()
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(1)
    else:
        logger.info("Sucessfully connected")
        sys.exit(0)

    # sys.exit(
    #     DomoClient(
    #         client_id=os.getenv("DOMO_CLIENT_ID"),
    #         secret_key=os.getenv("DOMO_SECRET_KEY"),
    #         access_token=os.getenv("DOMO_ACCESS_TOKEN"),
    #         domo_instance=os.getenv("DOMO_INSTANCE_ID"),
    #     ).connect()
    # )


if __name__ == "__main__":
    main()

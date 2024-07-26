import os
import sys

from shipyard_microsoft_power_bi import MicrosoftPowerBiClient
from shipyard_templates.shipyard_logger import ShipyardLogger

logger = ShipyardLogger.get_logger()
logger.setLevel("AUTHTEST")


def main():
    sys.exit(
        MicrosoftPowerBiClient(
            client_id=os.getenv("MICROSOFT_POWER_BI_CLIENT_ID"),
            client_secret=os.getenv("MICROSOFT_POWER_BI_CLIENT_SECRET"),
            tenant_id=os.getenv("MICROSOFT_POWER_BI_TENANT_ID"),
        ).connect()
    )


if __name__ == "__main__":
    main()

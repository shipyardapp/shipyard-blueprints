import requests
import os
import sys
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def main():
    url = "https://api.app.shipyardapp.com/orgs"
    token = os.getenv("SHIPYARD_API_TOKEN")

    headers = {
        "accept": "application/json",
        "X-Shipyard-API-Key": token,
    }

    response = requests.get(url, headers=headers)

    if response.ok:
        logger.authtest("Successfully connected to the Shipyard API")
        sys.exit(0)
    else:
        logger.authtest(
            "Failed to connect to to the Shipyard API, ensure that the API token provided is correct"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()

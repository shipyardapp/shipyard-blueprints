# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "requests",
#     "shipyard-templates"
# ]
# ///
import os
import sys

import requests
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def connect(token: str):
    url = "https://api.smartsheet.com/2.0/users/me"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    response = requests.get(url, headers=headers)
    if response.ok:
        return 0

    logger.error("Error in connecting to Smartsheet")
    logger.error(response.text)
    return 1


def main():
    sys.exit(connect(token=os.getenv("SMARTSHEET_ACCESS_TOKEN")))


if __name__ == "__main__":
    main()

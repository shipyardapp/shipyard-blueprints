import os
import requests
import logging
import sys


def get_logger():
    logger = logging.getLogger("Shipyard")
    logger.setLevel(logging.DEBUG)
    # Add handler for stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # add specific format
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s -%(lineno)d: %(message)s"
    )
    console.setFormatter(formatter)
    logger.addHandler(console)
    return logger


def connect(logger: logging.Logger, token: str):
    url = "https://api.smartsheet.com/2.0/users/me"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    response = requests.get(url, headers=headers)
    if response.ok:
        return 0

    logger.error("Error in connecting to Smartsheet")
    logger.error(response.text)
    return 1


def main():
    logger = get_logger()
    sys.exit(connect(logger, token=os.getenv("SMARTSHEET_ACCESS_TOKEN")))


if __name__ == "__main__":
    main()

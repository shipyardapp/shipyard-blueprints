import argparse
import sys
import os

from shipyard_magnite import MagniteClient
from shipyard_templates import ShipyardLogger
from shipyard_templates import errors

logger = ShipyardLogger.get_logger()


def get_args():
    pass


def main():
    try:
        user = "wes.poulsen@pmg.com"
        pwd = "Y3LUNJfGwNcxvfD*9V2."
        camp_id = "74255"
        client = MagniteClient(user, pwd)
        client.connect()
        data = client.read("campaigns", camp_id)
        print(f"Data is {data}")
    except Exception as e:
        logger.error("Error in reading data from api")
        logger.error(f"Message reads: {e}")


if __name__ == "__main__":
    main()

# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "shipyard-clickup",
# ]
# ///
import os
import sys
from shipyard_clickup import ClickupClient


def main():
    sys.exit(ClickupClient(access_token=os.getenv("CLICKUP_ACCESS_TOKEN")).connect())


if __name__ == "__main__":
    main()

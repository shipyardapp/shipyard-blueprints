# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "shipyard-hex",
# ]
# ///
import os
import sys
from shipyard_hex import HexClient


def main():
    sys.exit(HexClient(api_token=os.getenv("HEX_API_TOKEN"))).connect(
        os.getenv("HEX_PROJECT_ID")
    )


if __name__ == "__main__":
    main()

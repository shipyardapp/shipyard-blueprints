import os
import sys
from shipyard_hex import HexClient


def main():
    sys.exit(
        HexClient(
            api_token=os.getenv("HEX_API_TOKEN"), project_id=os.getenv("HEX_PROJECT_ID")
        ).connect()
    )


if __name__ == "__main__":
    main()

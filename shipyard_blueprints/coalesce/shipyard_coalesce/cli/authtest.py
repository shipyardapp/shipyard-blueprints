import os
import sys
from shipyard_coalesce import CoalesceClient


def main():
    client = CoalesceClient(
        os.environ.get("COALESCE_ACCESS_TOKEN"), os.environ.get("COALESCE_REGION")
    )

    sys.exit(client.connect())


if __name__ == "__main__":
    main()

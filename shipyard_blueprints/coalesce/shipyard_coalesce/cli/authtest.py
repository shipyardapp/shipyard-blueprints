import os
import sys
from shipyard_coalesce import CoalesceClient


def main():
    client = CoalesceClient(os.getenv("COALESCE_ACCESS_TOKEN"))
    sys.exit(client.connect())


if __name__ == "__main__":
    main()

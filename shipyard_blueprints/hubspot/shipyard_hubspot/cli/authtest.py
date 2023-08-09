import os
import sys
from shipyard_hubspot import HubspotClient


def main():
    sys.exit(HubspotClient(access_token=os.getenv("HUBSPOT_ACCESS_TOKEN")).connect())


if __name__ == "__main__":
    main()

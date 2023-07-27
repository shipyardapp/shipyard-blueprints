import os
import sys
from shipyard_thoughtspot import ThoughtSpotClient


def main():
    sys.exit(ThoughtSpotClient(token=os.getenv("THOUGHTSPOT_TOKEN")).connect())


if __name__ == "__main__":
    main()

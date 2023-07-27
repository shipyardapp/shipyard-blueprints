import os
import sys
from shipyard_blueprints import CensusClient


def main():
    sys.exit(CensusClient(os.getenv("CENSUS_API_KEY")).connect())


if __name__ == "__main__":
    main()

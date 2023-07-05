import os
from shipyard_blueprints import CensusClient


def main():
    return CensusClient(os.getenv("CENSUS_API_KEY")).connect()


if __name__ == "__main__":
    main()

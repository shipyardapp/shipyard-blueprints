import os
from shipyard_thoughtspot import ThoughtSpotClient


def main():
    return ThoughtSpotClient(token=os.getenv("THOUGHTSPOT_TOKEN")).connect()


if __name__ == "__main__":
    main()

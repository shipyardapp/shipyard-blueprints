import os
import sys
from shipyard_magnite import MagniteClient


def main():
    sys.exit(
        MagniteClient(
            os.getenv("MAGNITE_USERNAME"), os.getenv("MAGNITE_PASSWORD")
        ).connect()
    )


if __name__ == "__main__":
    main()

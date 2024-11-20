import os
import sys
from shipyard_magnite import MagniteClient


def main():
    user = os.getenv("MAGNITE_USERNAME")
    password = os.getenv("MAGNITE_PASSWORD")
    client = MagniteClient(user, password)
    sys.exit(client.connect())


if __name__ == "__main__":
    main()

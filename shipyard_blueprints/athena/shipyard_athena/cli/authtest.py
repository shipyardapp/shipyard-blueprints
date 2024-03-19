import os
import sys

from shipyard_athena import AthenaClient


def main():
    athena = AthenaClient(
        os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    sys.exit(athena.connect())


if __name__ == "__main__":
    main()

import os
import sys
from shipyard_portable import PortableClient


def main():
    access_token = os.getenv("PORTABLE_API_TOKEN")
    portable = PortableClient(access_token)
    sys.exit(portable.connect())


if __name__ == "__main__":
    main()

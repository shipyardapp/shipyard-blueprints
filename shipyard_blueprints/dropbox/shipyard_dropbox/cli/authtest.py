import sys
import argparse
from shipyard_blueprints import DropboxClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-key", dest="access_key", required=True)
    return parser.parse_args()


def main():
    args = get_args()
    key = args.access_key
    dropbox = DropboxClient(key)
    try:
        dropbox.connect()
        dropbox.logger.info("Successfully connected to Dropbox")
        sys.exit(0)
    except Exception as e:
        dropbox.logger.error(
            "Could not connect to Dropbox with the access key provided"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()

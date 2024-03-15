import sys
import os
from shipyard_dropbox import DropboxClient
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def main():
    try:
        sys.exit(DropboxClient(os.getenv("DROPBOX_ACCESS_TOKEN")).connect())
    except Exception as e:
        logger.authtest(
            f"Could not connect to Dropbox with the access key provided due to {e}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()

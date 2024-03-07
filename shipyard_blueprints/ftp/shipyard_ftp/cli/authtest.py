import os
import sys

from shipyard_templates import ShipyardLogger

from shipyard_ftp import FtpClient

logger = ShipyardLogger().get_logger()


def main():
    try:
        sys.exit(
            FtpClient(
                host=os.getenv("FTP_HOST"),
                user=os.getenv("FTP_USERNAME"),
                pwd=os.getenv("FTP_PASSWORD"),
                port=os.getenv("FTP_PORT"),
            ).connect()
        )
    except Exception as e:
        logger.authtest(e)
        sys.exit(1)


if __name__ == "__main__":
    main()

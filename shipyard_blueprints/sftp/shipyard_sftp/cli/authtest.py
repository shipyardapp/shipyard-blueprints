import os
import sys

from shipyard_templates.shipyard_logger import ShipyardLogger

from shipyard_sftp.sftp import SftpClient

logger = ShipyardLogger().get_logger()


def main():
    logger.setLevel("AUTHTEST")
    sys.exit(
        SftpClient(
            host=os.getenv("SFTP_HOST"),
            port=os.getenv("SFTP_PORT"),
            user=os.getenv("SFTP_USERNAME"),
            pwd=os.getenv("SFTP_PASSWORD"),
            key=os.getenv("SFTP_RSA_KEY_FILE"),
        ).connect()
    )


if __name__ == "__main__":
    main()

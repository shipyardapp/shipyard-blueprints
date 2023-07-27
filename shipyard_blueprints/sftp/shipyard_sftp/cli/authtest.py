import os
import sys
from shipyard_blueprints import SftpClient


def main():
    sftp = SftpClient(host=os.getenv('SFTP_HOST'), port=os.getenv('SFTP_PORT'), username=os.getenv('SFTP_USERNAME'),
                      password=os.getenv('SFTP_PASSWORD'), key=None)
    try:
        sftp.connect()
        sftp.logger.info("Successfully connected to SFTP server")
        sys.exit(0)
    except Exception as e:
        sftp.logger.error("Could not connect to SFTP server")
        sys.exit(1)


if __name__ == "__main__":
    main()

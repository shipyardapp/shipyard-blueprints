import os
import sys
from shipyard_ftp import FtpClient


def get_args():
    return {
        "host": os.getenv("FTP_HOST"),
        "port": os.getenv("FTP_PORT"),
        "username": os.getenv("FTP_USERNAME"),
        "password": os.getenv("FTP_PASSWORD"),
    }


def main():
    args = get_args()
    host = args["host"]
    port = args["port"]
    user = args["username"]
    pwd = args["password"]
    ftp = FtpClient(host, user, pwd, port)
    try:
        ftp.connect()
        ftp.logger.info("Successfully connected to FTP server")
        sys.exit(0)
    except Exception as e:
        ftp.logger.error("Could not connect to FTP server with the given credentials")
        sys.exit(1)


if __name__ == "__main__":
    main()

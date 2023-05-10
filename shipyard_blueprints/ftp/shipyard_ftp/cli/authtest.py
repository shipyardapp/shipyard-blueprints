import os
from shipyard_blueprints import FtpClient


def get_args():
    args = {}
    args['host']  = os.getenv("FTP_HOST")
    args['port'] = os.getenv("FTP_PORT")
    args['username'] = os.getenv("FTP_USERNAME")
    args['password'] = os.getenv("FTP_PASSWORD")
    return args


def main():
    args = get_args()
    host = args['host']
    port = args['port']
    user = args['username']
    pwd = args['password']
    ftp = FtpClient(host, user, pwd, port)
    try:
        ftp.connect()
        ftp.logger.info("Successfully connected to FTP server")
        return 0
    except Exception as e:
        ftp.logger.error(
            "Could not connect to FTP server with the given credentials")
        return 1


if __name__ == "__main__":
    main()

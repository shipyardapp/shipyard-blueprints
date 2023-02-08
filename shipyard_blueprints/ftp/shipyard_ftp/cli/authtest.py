from shipyard_blueprints import FtpClient
import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", dest='host', required=True)
    parser.add_argument("--port", dest='port', default=21, required=True)
    parser.add_argument("--user", dest='user', required=True)
    parser.add_argument("--pwd", dest="pwd", required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    host = args.host
    port = int(args.port)
    user = args.user
    pwd = args.pwd
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

from shipyard_blueprints import SftpClient
import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", dest='host', required=True)
    parser.add_argument("--port", dest='port', default='21', required=True)
    parser.add_argument('--username', dest='username', required=False)
    parser.add_argument("--password", dest='password', required=False)
    parser.add_argument('--key', dest='key', default=None, required=False)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    host = args.host
    port = args.port
    username = args.username
    password = args.password
    key = args.key
    sftp = SftpClient(host, port, key, username, password)
    try:
        sftp.connect()
        sftp.logger.info("Successfully connected to SFTP server")
        return 0
    except Exception as e:
        sftp.logger.error("Could not connect to SFTP server")
        return 1


if __name__ == "__main__":
    main()

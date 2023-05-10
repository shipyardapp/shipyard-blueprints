import os
from shipyard_blueprints import SftpClient


def get_args():
    args = {}
    args['host'] = os.environ.get('SFTP_HOST')
    args['port'] = os.environ.get('SFTP_PORT')
    args['username'] = os.environ.get('SFTP_USERNAME')
    args['password'] = os.environ.get('SFTP_PASSWORD')
    args['key'] = os.environ.get('SFTP_RSA_KEY_FILE')
    return args


def main():
    args = get_args()
    host = args['host']
    port = args['port']
    username = args['username']
    password = args['password']
    key = args['key']
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

import paramiko
from shipyard_templates import CloudStorage


class SftpClient(CloudStorage):
    def __init__(
            self, host: str, port: int, key: str = None, user: str = None, pwd: str = None
    ) -> None:
        self.host = host
        self.port = port
        self.key = key
        self.user = user
        self.pwd = pwd
        super().__init__(host=host, port=port, key=key, user=user, pwd=pwd)

    def connect(self):
        if self.key is not None:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            k = paramiko.RSAKey.from_private_key_file(self.key)
            ssh.connect(hostname=self.host, port=self.port, username=self.user, pkey=k)
            client = ssh.open_sftp()
        else:
            transport = paramiko.Transport((self.host, int(self.port)))
            transport.connect(None, self.user, self.pwd)
            client = paramiko.SFTPClient.from_transport(transport)

        return client

    def move_or_rename_files(self):
        pass

    def remove_files(self):
        pass

    def upload_files(self):
        pass

    def download_files(self):
        pass

    def download(self):
        pass

    def upload(self):

        pass

    def move(self):
        pass

    def remove(self):
        pass

    def upload(self):
        pass

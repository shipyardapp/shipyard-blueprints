from templates.cloudstorage import CloudStorage
import ftplib


class FtpClient(CloudStorage):
    def __init__(self, host: str, user: str, pwd: str, port: int) -> None:
        self.host = host
        self.user = user
        self.pwd = pwd
        self.port = port
        super().__init__(host=host, user=user, pwd=pwd, port=port)

    def connect(self):
        client = ftplib.FTP()
        client.connect(self.host, self.port)
        client.login(self.user, self.pwd)
        return client

    def move_or_rename_files(self):
        pass

    def remove_files(self):
        pass

    def upload_files(self):
        pass

    def upload_files(self):
        pass

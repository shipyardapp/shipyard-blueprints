from shipyard_templates import CloudStorage
import boto3


class S3Client(CloudStorage):

    def __init__(self, aws_access_key: str, aws_secret_access_key: str, region: str) -> None:
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.region = region
        super().__init__(aws_access_key=aws_access_key,
                         aws_secret_access_key=aws_secret_access_key, region=region)

    def connect(self):
        client = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region
        ).resource('s3')
        self.logger.info("Successfully connected to S3")
        return client

    def move_or_rename_files(self):
        pass

    def remove_files(self):
        pass

    def upload_files(self):
        pass

    def download_files(self):
        pass

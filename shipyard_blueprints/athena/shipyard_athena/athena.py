from shipyard_templates import Database
import boto3
import sys


class AthenaClient(Database):
    def __init__(self, user: str, pwd: str, region: str = None) -> None:
        self.aws_access_key = user
        self.aws_secret_key = pwd
        self.region = region
        super().__init__(
            user=self.aws_access_key, pwd=self.aws_secret_key, region=self.region
        )

    def connect(self):
        try:
            client = boto3.client(
                "athena",
                region_name=self.region,
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
            )
            return client
        except Exception as e:
            self.logger.error("Could not connect with the provided credentials")
            return 1

    def execute_query(self, query: str):
        pass

    def fetch(self, query: str):
        pass

    def upload(self, file: str):
        pass

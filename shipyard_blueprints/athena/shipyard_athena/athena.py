from shipyard_templates import Database
import boto3
import sys


class AthenaClient(Database):
    def __init__(self, user: str, pwd: str, region: str = None) -> None:
        self.aws_access_key = user
        self.aws_secret_key = pwd
        self.region = region
        super().__init__(user = self.user, pwd = self.pwd, region= self.region)

    def connect(self):
        try:
            client = boto3.client('athena',
                                  region_name=self.region,
                                  aws_access_key_id=self.aws_access_key,
                                  aws_secret_access_key=self.aws_secret_key)
            return client
        except Exception as e:
            self.logger.error(
                "Could not connect with the provided credentials")
            return 1

    def execute_query(self, query: str):
        pass

    def fetch_query_results(self, query: str):
        pass

    def upload_csv_to_table(self, file: str):
        pass

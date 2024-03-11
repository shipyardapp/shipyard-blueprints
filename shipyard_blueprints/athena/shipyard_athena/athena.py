import boto3
import sys
from shipyard_templates import Database, ShipyardLogger
from typing import Dict, Optional

logger = ShipyardLogger.get_logger()


class AthenaClient:
    def __init__(
        self, aws_access_key: str, aws_secret_key: str, region: Optional[str] = None
    ) -> None:
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.region = region

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
            logger.error("Could not connect with the provided credentials")
            return 1

    def execute_query(self, query: str):
        pass

    def fetch(self, query: str):
        pass

    def upload(self, file: str):
        pass

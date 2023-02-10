from shipyard_blueprints import S3Client
from settings import S3

access_key = S3.AWS_ACCESS_KEY_ID
secret_key = S3.AWS_SECRET_ACCESS_KEY
region = S3.AWS_REGION

client = S3Client(access_key, secret_key, region)


def test_connection():
    def helper():
        try:
            conn = client.connect()
            return 0
        except Exception as e:
            return 1
    assert helper() == 0

import boto3
import shipyard_bp_utils as shipyard
import sys
from shipyard_templates import CloudStorage, ShipyardLogger
from typing import Optional
from shipyard_s3.utils.exceptions import (
    DownloadError,
    InvalidCredentials,
    RemoveError,
    UploadError,
)


logger = ShipyardLogger.get_logger()


class S3Client(CloudStorage):
    def __init__(
        self,
        aws_access_key: str,
        aws_secret_access_key: str,
        region: Optional[str] = None,
    ) -> None:
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.region = region

    # @property
    # def s3_conn(self):
    #     return self.connect()

    def _auth_test(self):
        try:
            client = boto3.client(
                "sts",
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_access_key,
                config=None,
            )
            response = client.get_caller_identity()
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                logger.info("Successfully connected to S3")
                return 0
        except Exception as e:
            logger.error(
                f"Exception occured when attempting to connect to S3. Response from the server: {str(e)}"
            )
            return 1
        else:
            logger.error(
                f"Could not connect to S3, status code returned was {response['ResponseMetadata']['HTTPStatusCode']}"
            )
            return 1

    def connect(self):
        try:
            logger.debug("Establishing connection with S3")
            client = boto3.client(
                "s3",
                region_name=self.region,
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_access_key,
                config=None,
            )
        except Exception as e:
            logger.error(str(e))
            raise InvalidCredentials
        else:
            logger.debug(
                "Successfully established connection, initializing connection property"
            )
            return client

    def move(
        self, s3_conn, src_bucket: str, dest_bucket: str, src_path: str, dest_path: str
    ):
        try:
            # create a source dictionary that specifies bucket name and key name of the object to be copied
            copy_source = {"Bucket": src_bucket, "Key": src_path}
            bucket = s3_conn.Bucket(dest_bucket)
            bucket.copy(copy_source, dest_path)
            # delete the original file
            s3_conn.Object(src_bucket, src_path).delete()

        except Exception as e:
            raise

    def remove(self, s3_conn, bucket_name: str, src_path: str):
        try:
            s3_response = s3_conn.delete_object(Bucket=bucket_name, Key=src_path)
        except Exception as e:
            raise RemoveError(message=str(e))
        else:
            logger.debug(f"Response from s3: {s3_response}")

    def upload(
        self, s3_conn, bucket_name: str, source_file: str, destination_path: str
    ):
        try:
            s3_conn.upload_file(source_file, bucket_name, destination_path)
        except Exception as e:
            raise UploadError(f"Error in uploading to S3: {str(e)}")

    def download(self, s3_conn, bucket_name: str, s3_path: str, dest_path: str):
        try:
            s3_conn.download_file(bucket_name, s3_path, dest_path)
        except Exception as e:
            raise DownloadError(
                f"Error in downloading s3 file to {dest_path}: {str(e)}"
            )

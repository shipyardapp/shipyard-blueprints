import boto3
import shipyard_bp_utils as shipyard
from shipyard_templates import CloudStorage, ShipyardLogger
from typing import Optional
from shipyard_s3.utils.exceptions import InvalidCredentials, UploadError


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

    @property
    def s3_conn(self):
        return self.connect()

    def connect(self):
        try:
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
            logger.debug("Establishing connection with S3")
            return client

    def move(self, src_bucket: str, dest_bucket: str, src_path: str, dest_path: str):
        # create a source dictionary that specifies bucket name and key name of the object to be copied
        try:
            # move or rename the original file
            copy_source = {"Bucket": source_bucket_name, "Key": source_full_path}
            bucket = self.s3_conn.Bucket(dest_bucket)
            bucket.copy(copy_source, dest_path)
            # delete the original file
            self.s3_conn.Object(src_bucket, src_path).delete()

        except Exception as e:
            raise

    def remove(self, bucket_name: str):
        pass

    def upload(self, bucket_name: str, source_file: str, destination_path: str):
        try:
            self.s3_conn.upload_file(source_file, bucket_name, destination_path)
        except Exception as e:
            raise UploadError(f"Error in uploading to S3: {str(e)}")

    def download(self, bucket_name: str, s3_path: str, dest_file_name: str):
        pass

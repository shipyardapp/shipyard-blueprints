import os
from typing import Optional, Dict, Any, List

import boto3
from boto3.exceptions import S3UploadFailedError
from shipyard_templates import CloudStorage, ExitCodeException, ShipyardLogger

from shipyard_s3.utils import utils
from shipyard_s3.utils.exceptions import (
    BucketDoesNotExist,
    DownloadError,
    InvalidBucketAccess,
    InvalidCredentials,
    InvalidRegion,
    ListObjectsError,
    MoveError,
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
        self._s3_conn = None

    @property
    def s3_conn(self):
        if not self._s3_conn:
            self._s3_conn = self.get_connection()
        return self._s3_conn

    def connect(self):
        try:
            boto3.client(
                "sts",
                region_name=self.region,
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_access_key,
            ).get_caller_identity()
        except Exception as e:
            logger.authtest(
                f"Could not validate credentials to S3 with the provided credentials. Response from the "
                f"server: {str(e)}"
            )
            return 1
        else:
            logger.authtest("Successfully connected to S3")
            return 0

    def get_connection(self):
        if self._s3_conn:
            return self._s3_conn  # reuse existing connection
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

    def move(self, src_bucket: str, dest_bucket: str, src_path: str, dest_path: str):
        """Move or rename an object within an S3 bucket

        Args:
            src_bucket: The source bucket
            dest_bucket: The destination bucket (can be the same as the src_bucket) if you are not moving objects between buckets
            src_path: The path of where the source object is
            dest_path: The path to where the source objects should be moved to

        Raises:
            MoveError:
        """
        try:
            # check the access to the buckets
            self.check_bucket(src_bucket)
            self.check_bucket(dest_bucket)
            copy_source = {"Bucket": src_bucket, "Key": src_path}
            # move the object(s)
            self.s3_conn.copy_object(
                Bucket=dest_bucket, CopySource=copy_source, Key=dest_path
            )
            # delete the original
            self.s3_conn.delete_object(Bucket=src_bucket, Key=src_path)

        except (BucketDoesNotExist, InvalidBucketAccess):
            raise
        except Exception as e:
            if "IllegalLocationConstraintException" in str(e):
                logger.debug(f"Response from the server: {str(e)}")
                raise InvalidRegion(region=self.region)
            raise MoveError(f"Error in attempting to move file: {str(e)}")

    def remove(self, bucket_name: str, src_path: str):
        """Delete a file in an S3 bucket

        Args:
            bucket_name: The name of the bucket where the target file resides
            src_path: The full path (if nested in a folders) where the target file resides

        Raises:
            RemoveError:
        """
        try:
            folder = os.path.dirname(src_path)
            files = self.list_files(bucket_name=bucket_name, s3_folder=folder)
            if src_path not in files:
                raise ExitCodeException(
                    f"File {src_path} was not found. Ensure that the file name and folder name provided are correct.",
                    exit_code=CloudStorage.EXIT_CODE_FILE_NOT_FOUND,
                )
            # check the access to the bucket
            self.check_bucket(bucket_name)

            s3_response = self.s3_conn.delete_object(Bucket=bucket_name, Key=src_path)
        except (BucketDoesNotExist, InvalidBucketAccess, ListObjectsError):
            raise ExitCodeException(
                f"File {src_path} was not found. Ensure that the file name and folder name provided are correct.",
                exit_code=CloudStorage.EXIT_CODE_FILE_NOT_FOUND,
            )
        except ExitCodeException:
            raise
        except Exception as e:
            if "IllegalLocationConstraintException" in str(e):
                logger.debug(f"Response from the server: {str(e)}")
                raise InvalidRegion(region=self.region)
            raise RemoveError(message=str(e))
        else:
            logger.debug(f"Response from s3: {s3_response}")

    def upload(
        self,
        bucket_name: str,
        source_file: str,
        destination_path: Optional[str] = None,
        extra_args: Optional[Dict[Any, Any]] = None,
    ):
        """Upload a file to an S3 Bucket

        Args:
            bucket_name: The name of the bucket to load to
            source_file: The name or file path of the target file to load
            destination_path: The optional path of where the file should be loaded to
            extra_args: Additional optional configuration arguments to be passed

        Raises:
            UploadError
        """
        try:
            # check the access to the bucket
            self.check_bucket(bucket_name)
            s3_upload_config = boto3.s3.transfer.TransferConfig()
            s3_transfer = boto3.s3.transfer.S3Transfer(
                client=self.s3_conn, config=s3_upload_config
            )
            s3_transfer.upload_file(
                source_file, bucket_name, destination_path, extra_args=extra_args
            )
        except (BucketDoesNotExist, InvalidBucketAccess):
            raise
        except S3UploadFailedError as se:
            if "IllegalLocationConstraintException" in str(se):
                logger.debug(f"Response from the server: {str(se)}")
                raise InvalidRegion(region=self.region)
        except Exception as e:
            raise UploadError(f"Error in uploading to S3: {str(e)}")

    def download(self, bucket_name: str, s3_path: str, dest_path: str):
        """Download a file from an S3 bucket
        Args:
            bucket_name: The bucket to download from
            s3_path: The file path of the object(s) to fetch
            dest_path: The path to download the target file(s) to

        Raises:
            DownloadError:
        """
        try:
            # check the access to the bucket
            self.check_bucket(bucket_name)
            self.s3_conn.download_file(bucket_name, s3_path, dest_path)
        except (BucketDoesNotExist, InvalidBucketAccess):
            raise
        except Exception as e:
            raise DownloadError(
                f"Error in downloading s3 file to {dest_path}: {str(e)}"
            )

    def list_files(self, bucket_name: str, s3_folder: Optional[str]) -> List[str]:
        """Returns the list of all the files (objects) in in a bucket and folder

        Args:
            bucket_name: The bucket to scan
            s3_folder: The optional folder path to scan, if omitted the the root of the bucket will be used

        Returns:

        """
        try:
            response = utils.list_objects(
                s3_conn=self.s3_conn, bucket_name=bucket_name, prefix=s3_folder
            )
            logger.debug(
                f"Response from S3 when attempting to list objects: {response}"
            )
            file_names = utils.get_files(response)
            continuation_token = response.get("NextContinuationToken")

            while continuation_token:
                response = utils.list_objects(
                    s3_conn=self.s3_conn,
                    bucket_name=bucket_name,
                    prefix=s3_folder,
                    continuation_token=continuation_token,
                )
                file_names = file_names.append(utils.get_files(response))
                continuation_token = response.get("NextContinuationToken")
            return file_names
        except Exception as e:
            logger.debug(f"Response from S3 is {str(e)}")
            raise ListObjectsError(bucket_name, s3_folder)

    def check_bucket(self, bucket_name: str):
        try:
            response = self.s3_conn.list_objects_v2(Bucket=bucket_name)
            logger.debug(f"Able to access bucket {bucket_name}")
            # logger.debug(f"Contents of response: {response}")
        except self.s3_conn.exceptions.NoSuchBucket:
            raise BucketDoesNotExist(bucket_name)
        except Exception as e:
            raise InvalidBucketAccess(bucket_name)

import json
import os
import tempfile

from google.cloud import storage
from google.cloud.exceptions import *
from shipyard_templates import ShipyardLogger, ExitCodeException, CloudStorage
from google.oauth2 import credentials, service_account

logger = ShipyardLogger().get_logger()
CHUNK_SIZE = 128 * 1024 * 1024

EXIT_CODE_BUCKET_NOT_FOUND = 100


def set_environment_variables(credentials: str):
    """
    Set GCP credentials as environment variables if they're provided via keyword
    arguments rather than seeded as environment variables. This will override
    system defaults.
    """
    try:
        json.loads(credentials)
        fd, path = tempfile.mkstemp()
        logger.info(f"Storing json credentials temporarily at {path}")
        with os.fdopen(fd, "w") as tmp:
            tmp.write(credentials)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        return path
    except Exception:
        logger.error("Using specified json credentials file")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials
        return


def get_bucket(*, gclient, bucket_name):
    """
    Fetches and returns the bucket from Google Cloud Storage
    """

    try:
        return gclient.get_bucket(bucket_name)
    except NotFound as e:
        raise ExitCodeException(
            f"Bucket {bucket_name} does not exist\n {e}",
            EXIT_CODE_BUCKET_NOT_FOUND,
        ) from e


def upload_file(bucket, source_full_path, destination_full_path):
    """
    Uploads a single file to Google Cloud Storage.
    """
    try:
        blob = bucket.blob(destination_full_path, chunk_size=CHUNK_SIZE)
        blob.upload_from_filename(source_full_path)

        logger.info(
            f"{source_full_path} successfully uploaded to "
            f"{bucket.name}/{destination_full_path}"
        )
    except Exception as e:
        raise ExitCodeException(
            f"Failed to upload {source_full_path} to {bucket.name}/{destination_full_path}",
            CloudStorage.EXIT_CODE_UPLOAD_ERROR,
        ) from e


def get_gclient():
    """
    Attempts to create the Google Cloud Storage Client with the associated
    environment variables
    """
    try:
        return storage.Client(credentials=_get_credentials())
    except ExitCodeException:
        raise
    except Exception as e:
        raise ExitCodeException(
            f"Error accessing Google Cloud Storage with the provided credentials due to {e}",
            CloudStorage.EXIT_CODE_INVALID_CREDENTIALS,
        ) from e


def get_storage_blob(bucket, source_folder_name, source_file_name):
    """
    Fetches and returns the single source file blob from the buck on
    Google Cloud Storage
    """
    source_path = source_file_name
    if source_folder_name or source_folder_name == ".":
        source_path = f"{source_folder_name}/{source_file_name}"
    blob = bucket.get_blob(source_path)
    try:
        if blob is None:
            raise ExitCodeException(
                f"File {source_path} does not exist",
                CloudStorage.EXIT_CODE_FILE_NOT_FOUND,
            )
        else:
            blob.exists()
            return blob
    except Exception as e:
        raise ExitCodeException(
            f"File {source_path} does not exist due to {e}",
            CloudStorage.EXIT_CODE_FILE_NOT_FOUND,
        ) from e


def _get_credentials():
    """Get the credentials for Google Cloud Storage, which are either an access token or a service account.

    Raises:
        ValueError:

    Returns: The credentials object for GCS

    """
    try:
        if access_token := os.environ.get("OAUTH_ACCESS_TOKEN"):
            logger.debug("Using access token for Google Cloud Storage")
            return credentials.Credentials(
                access_token,
                scopes=[
                    "https://www.googleapis.com/auth/devstorage.read_write",
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/devstorage.full_control",
                ],
            )

        elif creds := os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            logger.debug("Using service account for Google Cloud Storage")
            logger.debug(f"Creds: {creds}")
            try:
                json_creds = json.loads(creds)
                return service_account.Credentials.from_service_account_info(json_creds)
            except Exception:
                raise ExitCodeException(
                    "The provided credentials are not valid JSON",
                    CloudStorage.EXIT_CODE_INVALID_CREDENTIALS,
                )
        raise ValueError("Either service account or access token must be provided")

    except ExitCodeException:
        raise
    except Exception as e:
        raise ExitCodeException(
            f"Error in connecting to Google Cloud Storage. {e}",
            CloudStorage.EXIT_CODE_INVALID_CREDENTIALS,
        )

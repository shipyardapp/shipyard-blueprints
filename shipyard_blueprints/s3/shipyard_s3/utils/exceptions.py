from shipyard_templates import ExitCodeException, CloudStorage
from typing import Optional

EXIT_CODE_MOVE_ERROR = 101
EXIT_CODE_REMOVE_ERR0R = 102
EXIT_CODE_DOWNLOAD_ERROR = 103
EXIT_CODE_INVALID_REGION = 104
EXIT_CODE_BUCKET_DNE = 105
EXIT_CODE_BUCKET_ACCESS = 106
EXIT_CODE_LIST_OBJECT_ERROR = 107


class InvalidCredentials(ExitCodeException):
    def __init__(self):
        self.message = "The provided credentials are incorrect, pleasure ensure you enter valid access key and secret"
        self.exit_code = CloudStorage.EXIT_CODE_INVALID_CREDENTIALS


class UploadError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = CloudStorage.EXIT_CODE_UPLOAD_ERROR


class MoveError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = EXIT_CODE_MOVE_ERROR


class RemoveError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = EXIT_CODE_REMOVE_ERR0R


class DownloadError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = EXIT_CODE_DOWNLOAD_ERROR


class NoMatchesFound(ExitCodeException):
    def __init__(self, regex: str):
        self.message = f"No matches were found for the regex `{regex}`"
        self.exit_code = CloudStorage.EXIT_CODE_FILE_MATCH_ERROR


class InvalidRegion(ExitCodeException):
    def __init__(self, region: str):
        self.message = f"The provided Access Key and Secret do not have access to the following region: {region}"
        self.exit_code = EXIT_CODE_INVALID_REGION


class InvalidBucketAccess(ExitCodeException):
    def __init__(self, bucket: str):
        self.message = f"The bucket `{bucket}` is not accesible to the provided Access Key and Secret"
        self.exit_code = EXIT_CODE_BUCKET_ACCESS


class BucketDoesNotExist(ExitCodeException):
    def __init__(self, bucket: str):
        self.message = f"The bucket `{bucket}` does not exist"
        self.exit_code = EXIT_CODE_BUCKET_DNE


class ListObjectsError(ExitCodeException):
    def __init__(self, bucket_name: str, s3_folder: Optional[str]):
        msg = f"Error in listing the contents of s3://{bucket_name}/"
        self.message = msg + s3_folder if s3_folder else msg
        self.exit_code = EXIT_CODE_LIST_OBJECT_ERROR

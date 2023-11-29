import json
import tempfile
import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from shipyard_templates import CloudStorage, ExitCodeException
from typing import Optional, Dict, List, Any, Union
from googleapiclient.http import MediaIoBaseDownload
from shipyard_googledrive import drive_utils


class GoogleDriveClient(CloudStorage):
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    EXIT_CODE_DRIVE_ACCESS_ERROR = 209

    def __init__(
        self,
        service_account: str,
        shared_drive_name: Optional[str] = None,
    ) -> None:
        self.service_account = service_account
        self.shared_drive_name = shared_drive_name
        self.service = self.connect()
        self.drive_id = None
        self.folder_id = None
        self.folder_name = None
        super().__init__(
            service_account=self.service_account,
            service=self.service,
            shared_drive_name=self.shared_drive_name,
            drive_id=self.drive_id,
            folder_id=self.folder_id,
            folder_name=self.folder_name,
        )

    @property
    def email(self):
        json_creds = json.loads(self.service_account)
        return json_creds["client_email"]

    def _set_env_vars(self):
        try:
            fd, path = tempfile.mkstemp()
            with os.fdopen(fd, "w") as tmp:
                tmp.write(self.service_account)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
            return path
        except Exception as e:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.service_account

    def _store_json_credentials(self):
        with open("credentials.json", "w") as f:
            json.dump(self.service_account, f)

    def connect(self):
        tmp_path = self._set_env_vars()
        credentials = service_account.Credentials.from_service_account_file(
            tmp_path, scopes=self.SCOPES
        )
        service = build("drive", "v3", credentials=credentials)
        return service

    def upload(
        self,
        file_path: str,
        drive_folder: Optional[str] = None,
        drive_file_name: Optional[str] = None,
        drive: Optional[str] = None,
    ):
        """Uploads a file to a shared Google Drive

        Args:
            file_path: The path of the file to load
            drive_file_name: The name that the uploaded file in drive will have
            drive_folder: The ID of the folder in Google drive
            drive: The name or ID of the shared drive
        """

        # NOTE: check to see that the folder is shared with the service account
        if drive:
            self.shared_drive_name = drive
            self.drive_id = drive_utils.get_drive_id(
                service=self.service, drive_id=self.shared_drive_name
            )
            # if the drive is provided, but not found, then the service account doesn't have access and needs it to be shared
            if not self.drive_id:
                raise ExitCodeException(
                    f"{self.email} does not have access to the following drive: {self.shared_drive_name}. Please visit the authorization guide to see how to share the Drive to the service account",
                    self.EXIT_CODE_DRIVE_ACCESS_ERROR,
                )

        try:
            if drive_folder:
                self.folder_name = drive_folder
                self.folder_id = drive_utils.get_folder_id(
                    service=self.service,
                    folder_identifier=self.folder_name,
                    drive_id=self.drive_id,
                )
                if not self.folder_id and not drive_utils.is_folder_id(
                    self.folder_name
                ):
                    folder_results = drive_utils.create_remote_folder(
                        folder_name=self.folder_name,
                        service=self.service,
                        drive_id=self.drive_id,
                    )
                    self.folder_id = folder_results
                # else:
                #     if not drive_utils.is_folder_shared(service_account_email=self.email,folder_id = self.folder_id, drive_service= self.service):
                #         raise ExitCodeException(f'Error: The folder {self.folder_name} exists in Google Drive but has not been shared with the service account associated with {self.email}. Share the folder, then retry the upload', self.EXIT_CODE_FOLDER_ACCESS_ERROR)

            # use the base name of the file if not provided
            if not drive_file_name:
                drive_file_name = os.path.basename(file_path)
            file_metadata = {"name": drive_file_name, "parents": []}
            # if the folder exists, check to see if it has been shared with the drive correctly, if not then throw exception
            if self.folder_id:
                # FIXME: This throws an exception every time, needs to be fixed
                # if drive_utils.is_folder_shared(
                #     service_account_email=self.email,
                #     folder_id=self.folder_id,
                #     drive_service=self.service,
                # ):
                file_metadata["parents"].append(self.folder_id)
                # raise ExitCodeException(
                #     f"Aborting upload. The folder with ID {self.folder_id} exists but has not been shared with the service account {self.email}. Please share that folder with the service account then retry uploading.",
                #     exit_code=self.EXIT_CODE_FOLDER_ACCESS_ERROR,
                # )
            elif self.drive_id:
                self.folder_id = self.drive_id
                file_metadata["parents"].append(self.folder_id)
            else:
                self.folder_id = "root"
                file_metadata["parents"].append(self.folder_id)

            # check to see if the file exists or not
            update = False
            if drive_utils.does_file_exist(
                logger=self.logger,
                parent_folder_id=self.folder_id,
                file_name=drive_file_name,
                service=self.service,
                drive_id=self.drive_id,
            ):
                parents = file_metadata.pop("parents")
                update = True
                if parents != []:
                    self.folder_id = parents[0]  # update the folder ID

            media = MediaFileUpload(file_path, resumable=True)
            if update:
                file_id = drive_utils.get_file_id(
                    file_name=drive_file_name,
                    service=self.service,
                    drive_id=self.drive_id,
                    folder_id=self.folder_id,  # NOTE: This was added after tests. Need to retest
                )
                upload_file = (
                    self.service.files()
                    .update(
                        fileId=file_id,
                        body=file_metadata,
                        media_body=media,
                        supportsAllDrives=True,
                        fields=("id"),
                        addParents=self.folder_id,
                    )
                    .execute()
                )
                self.logger.info(f"Updated file {file_id}")

            else:
                upload_file = (
                    self.service.files()
                    .create(
                        body=file_metadata,
                        media_body=media,
                        fields=("id"),
                        supportsAllDrives=True,
                    )
                    .execute()
                )
                self.logger.info(f"Newly created file is {upload_file.get('id')}")
        except FileNotFoundError as fe:
            raise ExitCodeException(
                message=str(fe), exit_code=self.EXIT_CODE_FILE_NOT_FOUND
            )
        except ExitCodeException as ec:
            raise ExitCodeException(message=ec.message, exit_code=ec.exit_code)
        except Exception as e:
            raise ExitCodeException(
                message=f"Error in uploading file to google drive: {str(e)}",
                exit_code=self.EXIT_CODE_UPLOAD_ERROR,
            )

    def move(self):
        pass

    def remove(self):
        pass

    def download(
        self,
        file_id: str,
        drive_file_name: str,
        drive_folder: Optional[str] = None,
        drive: Optional[str] = None,
        destination_path: Optional[str] = None,
        destination_file_name: Optional[str] = None,
    ):
        """Downloads a file from Google Drive locally

        Args:
            file_id: The ID of the file to download
            drive_file_name: The name of the file to download
            drive_folder: The optional name of the folder or the ID of folder. If not provided, then it will look for the file within the root directory of the drive
            drive: The optional name or ID of the shared drive
            destination_path: The optional path to download the file to, if not provided, then the file will be downloaded to the current working directory
            destination_file_name: The optional name of the downloaded file to have. If not provided, then the file will have the same name as it did in Google Drive
        """
        if drive:
            self.drive_id = drive_utils.get_drive_id(
                drive_id=drive, service=self.service
            )

        if drive_folder:
            try:
                self.folder_id = drive_utils.get_folder_id(
                    folder_identifier=drive_folder,
                    service=self.service,
                    drive_id=self.drive_id,
                )
            except ExitCodeException as ec:
                raise ExitCodeException(ec.message, ec.exit_code)

        if destination_path:
            # if exists, then skip
            if not os.path.exists(destination_path):
                os.mkdir(destination_path)
        else:
            destination_path = os.getcwd()

        if destination_file_name:
            destination_path = os.path.join(destination_path, destination_file_name)
        else:
            destination_path = os.path.join(destination_path, drive_file_name)

        try:
            request = self.service.files().get_media(fileId=file_id)
            fh = open(destination_path, "wb")
            downloader = MediaIoBaseDownload(fh, request)
            complete = False
            while not complete:
                status, complete = downloader.next_chunk()
        except Exception as e:
            raise ExitCodeException(
                message=str(e), exit_code=self.EXIT_CODE_DOWNLOAD_ERROR
            )
        else:
            return

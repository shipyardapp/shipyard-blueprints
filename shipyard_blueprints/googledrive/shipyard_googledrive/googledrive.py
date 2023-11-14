import json
import tempfile
import os
import google.auth
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from shipyard_templates import CloudStorage
from typing import Optional, Dict, List, Any, Union
from shipyard_googledrive import utils
from googleapiclient.http import MediaIoBaseDownload


class GoogleDriveClient(CloudStorage):
    SCOPES = ["https://www.googleapis.com/auth/drive"]

    def __init__(
        self, service_account: str, shared_drive: Optional[str] = None
    ) -> None:
        self.service_account = service_account
        self.shared_drive = shared_drive
        self.service = self.connect()
        super().__init__(service_account=service_account)

    def _set_env_vars(self):
        try:
            json_credentials = json.loads(self.service_account)
            fd, path = tempfile.mkstemp()
            print(f"Storing json credentials temporarily at {path}")
            with os.fdopen(fd, "w") as tmp:
                # tmp.write(self.service_account)
                tmp.write(json_credentials)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
            return path
        except Exception as e:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.service_account

    def connect(self):
        tmp_path = self._set_env_vars()
        credentials = service_account.Credentials.from_service_account_file(
            tmp_path, scopes=self.SCOPES
        )
        service = build("drive", "v3", credentials=credentials)
        return service

    def upload(
        self, file_path: str, drive_file_name: str, drive_folder: Optional[str] = None
    ):
        drive_file = utils.generate_path(drive_file_name, drive_folder)
        try:
            file_data = {"name": drive_file}
        except Exception as e:
            pass

    def upload_google_drive_file(
        self, service, source_path, destination_path, parent_folder_id, drive
    ):
        """
        Uploads a single file to Google Drive.
        """
        file_metadata = {"name": destination_path}
        file_name = destination_path.rsplit("/", 1)[-1]
        if file_name:
            file_metadata = {"name": file_name, "parents": []}

        drive_id = None
        if drive:
            drive_id = utils.get_shared_drive_id(service, drive)

        if parent_folder_id:
            file_metadata["parents"].append(parent_folder_id)
        elif drive_id:
            parent_folder_id = drive_id
            file_metadata["parents"].append(drive_id)
        else:
            parent_folder_id = "root"

        # Check if file exists
        update = False
        if drive_id:
            query = f"name='{file_name}' and '{parent_folder_id}' in parents"
            exists = (
                service.files()
                .list(
                    q=query,
                    includeItemsFromAllDrives=True,
                    corpora="drive",
                    driveId=drive_id,
                    supportsAllDrives=True,
                )
                .execute()
            )
        else:
            query = f"name='{file_name}' and '{parent_folder_id}' in parents"
            exists = service.files().list(q=query).execute()

        if exists.get("files", []) != []:
            file_id = exists["files"][0]["id"]
            update = True
            parents = file_metadata.pop("parents")
            if parents != []:
                parent_folder_id = parents[0]

        try:
            media = MediaFileUpload(source_path, resumable=True)
        except Exception as e:
            self.logger.error(f"The file {source_path} does not exist")
            raise (e)

        try:
            if update:
                _file = (
                    service.files()
                    .update(
                        fileId=file_id,
                        body=file_metadata,
                        media_body=media,
                        supportsAllDrives=True,
                        fields=("id"),
                        addParents=parent_folder_id,
                    )
                    .execute()
                )
            else:
                _file = (
                    service.files()
                    .create(
                        body=file_metadata,
                        media_body=media,
                        supportsAllDrives=True,
                        fields=("id"),
                    )
                    .execute()
                )
        except Exception as e:
            if e.content == b"Failed to parse Content-Range header.":
                self.logger.error(
                    f"Failed to upload file {source_path} due to invalid size"
                )
            self.logger.error(f"Failed to upload file: {file_name}")
            raise (e)

        self.logger.info(
            f"{source_path} successfully uploaded to " f"{destination_path}"
        )

    def move(self):
        pass

    def remove(self):
        pass

    def download(self):
        pass

    def download_google_drive_file(self, service, blob, destination_file_name=None):
        """
        Download a selected file from Google Drive to local storage in
        the current working directory.
        """
        local_path = os.path.normpath(f"{os.getcwd()}/{destination_file_name}")
        name = blob["name"]
        path = local_path.rsplit("/", 1)[0]
        if not os.path.exists(path):
            os.mkdir(path)
        fh = io.FileIO(local_path, "wb+")
        try:
            request = service.files().get_media(fileId=blob["id"])
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
        except Exception as e:
            self.logger.error(f"{name} failed to downoad")
            raise (e)

        self.logger.info(f"{name} successfully downloaded to {local_path}")

        return

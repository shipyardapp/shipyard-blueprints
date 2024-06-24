import requests
import re
from shipyard_templates import CloudStorage, ShipyardLogger, ExitCodeException
from shipyard_templates.errors import (
    InvalidCredentialError,
    BadRequestError,
    handle_errors,
)
from msal import ConfidentialClientApplication
from typing import Optional, List

logger = ShipyardLogger.get_logger()


# NOTE: Access token obtained by Oauth is currently not supported and will need to be added in the future
class OneDriveClient(CloudStorage):
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant: str,
        user_email: Optional[str] = None,
    ) -> None:
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant = tenant
        self.user_email = user_email
        self.access_token = None  # will be set during the connect

        super().__init__()

    def connect(
        self,
    ):
        app = ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant}",
            client_credential=self.client_secret,
        )
        result = app.acquire_token_for_client(
            scopes=["https://graph.microsoft.com/.default"]
        )
        if "access_token" in result:
            self.access_token = result["access_token"]
            logger.info("Successfully connected to OneDrive using basic authentication")

        else:
            logger.error("Failed to connect to OneDrive using basic authentication")
            raise InvalidCredentialError(
                "Failed to connect to OneDrive using basic authentication"
            )

    def get_user_id(self) -> str:
        """Get the user ID of the user with the given email
        Raises:
            BadRequestError:

        Returns: The ID of the user

        """
        if not self.user_email:
            raise ValueError(
                "User email was not provided when initializing the client. Please provide the user email"
            )
        url = f"{self.base_url}/users/{self.user_email}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        response = requests.get(url, headers=headers)
        if response.ok:
            user_info = response.json()
            user_id = user_info["id"]
            return user_id
        else:
            logger.error(f"Failed to get user ID")
            handle_errors(response.text, response.status_code)

    def get_drive_id(self, user_id: str) -> str:
        """Get the drive ID of the user with the given ID

        Args:
            user_id: The ID of the user to the associated drive ID for

        Raises:
            ExitCodeException:

        Returns: The Drive ID of the user

        """
        url = f"{self.base_url}/users/{user_id}/drive"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        response = requests.get(url, headers=headers)
        if response.ok:
            drive_info = response.json()
            drive_id = drive_info["id"]
            return drive_id
        else:
            handle_errors(response.text, response.status_code)

    def upload(self, file_path: str, drive_id: str, drive_path: Optional[str]):
        """Uploads a file to OneDrive
        Args:
            file_path: The path of the local file to upload
            drive_id: The ID of the drive to upload the file to, this is only necessary if using basic authentic
            drive_path: The drive path to upload the file to
        Raises:
            ExitCodeException:
        """
        url = f"{self.base_url}/drives/{drive_id}/root:/{drive_path}:/content"

        with open(file_path, "rb") as file:
            file_content = file.read()
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/octet-stream",
        }
        response = requests.put(url, headers=headers, data=file_content)
        if response.ok:
            logger.info(
                f"Successfully uploaded {file_path} to {drive_path} in OneDrive"
            )
        else:
            raise ExitCodeException(
                f"Failed to upload {file_path} to {drive_path} in OneDrive: {response.text}",
                self.EXIT_CODE_UPLOAD_ERROR,
            )

    def download(self, file_path: str, drive_path: str, drive_id: Optional[str]):
        """Downloads a file from OneDrive

        Args:
            file_path: The path to write to
            drive_path: The path of the file to download in OneDrive
            drive_id: The ID of the drive to download from (only necessary if using basic authentication)
        """
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{drive_path}:/content"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/octet-stream",
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            with open(file_path, "wb") as file:
                file.write(response.content)
            logger.info(f"File downloaded successfully to {file_path}")
        else:
            logger.debug(
                f"Failed to download {file_path} from OneDrive. Ensure that the file and folder (if provide exist)"
            )
            raise ExitCodeException(
                f"Failed to download file from OneDrive: {response.text}",
                self.EXIT_CODE_DOWNLOAD_ERROR,
            )

    def move(
        self,
        src_name: str,
        src_dir: Optional[str],
        target_name: Optional[str],
        target_dir: Optional[str],
        drive_id: Optional[str],
    ):
        item_id = self.get_file_id(src_name, drive_id, src_dir)

        data = {}
        if target_dir and src_dir != target_dir:
            folder_id = self.get_folder_id(target_dir, drive_id)
            if not folder_id and target_dir != "":
                logger.info(f"Creating folder '{target_dir}' in OneDrive")
                folder_id = self.create_folder(target_dir, drive_id)
                logger.debug(f"Folder ID: {folder_id}")
            data["parentReference"] = {"id": folder_id}

        if target_name and src_name != target_name:
            data["name"] = target_name

        if not data:
            logger.info(
                "No operation to perform (same source and target locations and names)."
            )
            return

        url = f"{self.base_url}/drives/{drive_id}/items/{item_id}"

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        logger.debug(f"Item ID: {item_id}")
        response = requests.patch(url, headers=headers, json=data)

        if response.ok:
            logger.info(
                f"Successfully moved/renamed {src_name} to {target_name or src_name} in OneDrive"
            )
        else:
            raise ExitCodeException(
                f"Failed to move/rename {src_name} to {target_name or src_name} in OneDrive: {response.text}",
                self.EXIT_CODE_UNKNOWN_ERROR,
            )

    def remove(
        self,
        drive_path: str,
        drive_id: Optional[str],
    ):
        """Removes a file from OneDrive

        Args:
            drive_path: The path of the file to remove in OneDrive
            drive_id: The ID of the drive to remove (only necessary if using basic authentication)
        """
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{drive_path}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
        }

        response = requests.delete(url, headers=headers)

        if response.ok:
            logger.info(f"Successfully removed {drive_path} from OneDrive")
        else:
            raise ExitCodeException(
                f"Failed to remove {drive_path} from OneDrive: {response.text}",
                self.EXIT_CODE_UNKNOWN_ERROR,
            )

    def get_folder_id(self, folder_name: str, drive_id: Optional[str]) -> Optional[str]:
        """Returns the folder ID of a folder in OneDrive

        Args:
            folder_name: The name of the folder to fetch the ID of
            drive_id: The ID of the drive to search in (only necessary if using basic authentication)

        Raises:
            BadRequestError:

        Returns: The ID of the folder or None

        """
        url = f"{self.base_url}/drives/{drive_id}/root/children"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        response = requests.get(url, headers=headers)
        if response.ok:
            folders = response.json()
            for folder in folders["value"]:
                if folder["name"] == folder_name:
                    return folder["id"]
            else:
                logger.info(
                    f"Did not find a folder in OneDrive matching '{folder_name}'"
                )
                return
        else:
            logger.error("Failed to get folder ID")
            handle_errors(response.text, response.status_code)

    def get_file_id(
        self, file_name: str, drive_id: Optional[str], folder_name: Optional[str] = ""
    ) -> Optional[str]:
        """Returns the file ID of a file in OneDrive

        Args:
            file_name: The name of the file to fetch the ID of
            drive_id: The ID of the drive to search in (only necessary if using basic authentication)

        Raises:
            BadRequestError:

        Returns: The ID of the file or None

        """
        if folder_name:
            url = f"{self.base_url}/drives/{drive_id}/root:/{folder_name}:/children"
        else:
            url = f"{self.base_url}/drives/{drive_id}/root/children"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        response = requests.get(url, headers=headers)
        if response.ok:
            files = response.json()
            for file in files.get("value", []):
                if file["name"] == file_name and not file.get("folder"):
                    return file["id"]
            else:
                logger.error(f"Did not find a file in OneDrive matching '{file_name}'")
                return
        else:
            logger.error(f"Failed to get file ID")
            handle_errors(response.text, response.status_code)

    def create_folder(self, folder_name: str, drive_id: Optional[str]) -> str:
        """Creates a folder in OneDrive

        Args:
            folder_name: The name of the folder to create
            drive_id: The ID of the drive to create the folder in (only necessary if using basic authentication)

        Raises:
            BadRequestError:

        Returns: The ID of the created folder

        """
        url = f"{self.base_url}/drives/{drive_id}/root/children"

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        data = {
            "name": folder_name,
            "folder": {},
            "@microsoft.graph.conflictBehavior": "rename",
        }

        response = requests.post(url, headers=headers, json=data)

        if response.ok:
            logger.info(f"Successfully created folder '{folder_name}' in OneDrive")
            return response.json()["id"]

        else:
            logger.error(f"Failed to create folder '{folder_name}' in OneDrive")
            handle_errors(response.text, response.status_code)

    def get_file_matches(
        self, folder: str, pattern: str, drive_id: Optional[str]
    ) -> List[str]:
        """Returns a list of files that match the given pattern

        Args:
            pattern: The pattern to match on
            drive_id: The ID of the drive to search in (only necessary if using basic authentication)

        Returns: A list of files that match the pattern. If no matches are found then an empty list will be returned

        """
        url = f"{self.base_url}/drives/{drive_id}/root:/{folder}:/children"

        headers = {"Authorization": f"Bearer {self.access_token}"}

        matching_files = []
        response = requests.get(url, headers=headers)
        if response.ok:
            logger.debug("Successfully fetched files from OneDrive")
            files = response.json().get("value", [])
            regex = re.compile(pattern)
            for file in files:
                if "name" in file and regex.search(file["name"]):
                    matching_files.append(file)

        else:
            logger.warning(
                f"Failed to fetch file matches from OneDrive: {response.text}"
            )
        return matching_files

    def download_from_graph_url(self, download_url: str, file_name: str):
        """Download a file from a given URL

        Args:
            download_url: The download URL from the Graph API
            file_name: The name of the file to write to

        Raises:
            BadRequestError:
        """
        response = requests.get(download_url)
        if response.ok:
            with open(file_name, "wb") as file:
                file.write(response.content)
        else:
            handle_errors(response.text, response.status_code)

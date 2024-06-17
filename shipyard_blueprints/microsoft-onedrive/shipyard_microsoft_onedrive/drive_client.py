import requests
from shipyard_templates import CloudStorage, ShipyardLogger, ExitCodeException
from shipyard_templates.errors import InvalidCredentialError, BadRequestError
from msal import ConfidentialClientApplication
from typing import Optional

logger = ShipyardLogger.get_logger()


class OneDriveClient(CloudStorage):
    def __init__(self, auth_type: str, access_token: Optional[str] = None) -> None:
        if auth_type not in ["basic", "access_token"]:
            raise ValueError(
                "Invalid auth_type. Must be either 'basic' or 'access_token'"
            )
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.access_token = access_token
        self.auth_type = auth_type
        if auth_type == "basic":
            self.client_id = None
            self.client_secret = None
            self.tenant = None
        else:
            # fill out the attribute variables for the oauth flow
            self.headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
            }

        super().__init__()

    def connect(
        self,
        client_id: str,
        client_secret: str,
        tenant: str,
    ):
        app = ConfidentialClientApplication(
            client_id,
            authority=f"https://login.microsoftonline.com/{tenant}",
            client_credential=client_secret,
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

    def get_user_id(self, user_email: str):
        # endpoint = f'https://graph.microsoft.com/v1.0/users/{user_email}'
        url = f"{self.base_url}/users/{user_email}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            user_info = response.json()
            user_id = user_info["id"]
            return user_id
        else:
            raise BadRequestError(f"Failed to get user ID: {response.text}")

    def get_drive_id(self, user_id: str):
        # endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/drive'
        url = f"{self.base_url}/users/{user_id}/drive"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            drive_info = response.json()
            drive_id = drive_info["id"]
            return drive_id
        else:
            raise BadRequestError(f"Failed to get drive ID: {response.text}")

    def upload(self, file_path: str, drive_id: str, drive_path: Optional[str]):
        """Uploads a file to OneDrive
        Args:
            file_path: The path of the local file to upload
            drive_id: The ID of the drive to upload the file to, this is only necessary if using basic authentic
            drive_path: The drive path to upload the file to
        Raises:
            ExitCodeException:
        """
        # url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{drive_path}:/content'
        if self.auth_type == "basic":
            url = f"{self.base_url}/drives/{drive_id}/root:/{drive_path}:/content"
        else:
            url = f"{self.base_url}/me/drive/root:/{drive_path}:/content"

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
        if self.auth_type == "basic":
            url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{drive_path}:/content"
        else:
            url = (
                f"https://graph.microsoft.com/v1.0/me/drive/root:/{drive_path}:/content"
            )
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
            raise BadRequestError(
                f"Failed to download file from OneDrive: {response.text}"
            )

    def move(
        self,
        src_name: str,
        src_dir: str,
        target_name: str,
        target_dir: str,
        drive_id: Optional[str],
    ):
        item_id = self.get_file_id(src_name, drive_id)
        if src_dir:
            folder_id = self.get_folder_id(target_dir, drive_id)
            if not folder_id:
                logger.info(f"Creating folder '{target_dir}' in OneDrive")
                folder_id = self.create_folder(target_dir, drive_id)
                logger.debug(f"Folder ID: {folder_id}")
            data = {"parentReference": {"id": folder_id}, "name": target_name}
        else:
            data = {"name": target_name}

        if self.auth_type == "basic":
            url = f"{self.base_url}/drives/{drive_id}/items/{item_id}"
        else:
            url = f"https://graph.microsoft.com/v1.0/me/drive/items/{item_id}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        logger.debug(f"Item ID: {item_id}")
        response = requests.patch(url, headers=headers, json=data)
        if response.ok:
            logger.info(f"Successfully moved {src_name} to {target_name} in OneDrive")
        else:
            raise BadRequestError(
                f"Failed to move {src_name} to {target_name} in OneDrive: {response.text}"
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
        if self.auth_type == "basic":
            url = (
                f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{drive_path}"
            )
        else:
            url = f"https://graph.microsoft.com/v1.0/me/drive/root:/{drive_path}"

        headers = {
            "Authorization": f"Bearer {self.access_token}",
        }

        response = requests.delete(url, headers=headers)

        if response.ok:
            logger.info(f"Successfully removed {drive_path} from OneDrive")
        else:
            raise BadRequestError(
                f"Failed to remove {drive_path} from OneDrive: {response.text}"
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
        if self.auth_type == "basic":
            url = f"{self.base_url}/drives/{drive_id}/root/children"
        else:
            url = f"{self.base_url}/me/drive/root/children"

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
            raise BadRequestError(f"Failed to get folder ID: {response.text}")

    def get_file_id(self, file_name: str, drive_id: Optional[str]) -> Optional[str]:
        """Returns the file ID of a file in OneDrive

        Args:
            file_name: The name of the file to fetch the ID of
            drive_id: The ID of the drive to search in (only necessary if using basic authentication)

        Raises:
            BadRequestError:

        Returns: The ID of the file or None

        """
        if self.auth_type == "basic":
            url = f"{self.base_url}/drives/{drive_id}/root/children"
        else:
            url = f"{self.base_url}/me/drive/root/children"

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
            raise BadRequestError(f"Failed to get file ID: {response.text}")

    def create_folder(self, folder_name: str, drive_id: Optional[str]):
        if self.auth_type == "basic":
            url = f"{self.base_url}/drives/{drive_id}/root/children"
        else:
            url = f"{self.base_url}/me/drive/root/children"

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
            raise BadRequestError(
                f"Failed to create folder '{folder_name}' in OneDrive: {response.text}"
            )

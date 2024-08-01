import re
from json import JSONDecodeError
from typing import Optional, List, Dict, Any

import msal
from requests import request
from shipyard_templates import CloudStorage, ShipyardLogger, ExitCodeException
from shipyard_templates.errors import InvalidCredentialError, handle_errors

logger = ShipyardLogger.get_logger()


class OneDriveClient(CloudStorage):
    def __init__(
        self,
        access_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        tenant: Optional[str] = None,
    ) -> None:
        self._access_token = access_token
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant = tenant
        self.base_url = "https://graph.microsoft.com/v1.0"

    @property
    def access_token(self):
        if not self._access_token:
            if self.username and self.password:
                try:
                    self._generate_token_from_un_pw()
                except Exception as e:
                    logger.error(
                        f"Failed to generate token using username/password: {e}"
                    )
            if self.client_id and self.client_secret:
                try:
                    self._generate_token_from_client()
                except Exception as e:
                    logger.error(
                        f"Failed to generate token using client credentials: {e}"
                    )

        if not self._access_token:
            raise InvalidCredentialError("Invalid credentials provided")
        return self._access_token

    @access_token.setter
    def access_token(self, access_token):
        self._access_token = access_token

    def _generate_token_from_client(self):
        result = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=f"https://login.microsoftonline.com/{self.tenant}",
        ).acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        self._set_access_token(result)

    def _generate_token_from_un_pw(self):
        result = msal.PublicClientApplication(
            client_id=self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant}",
        ).acquire_token_by_username_password(
            username=self.username,
            password=self.password,
            scopes=["https://graph.microsoft.com/.default"],
        )
        self._set_access_token(result)

    def _set_access_token(self, result):
        if "access_token" not in result:
            raise InvalidCredentialError(
                f"Failed to connect to OneDrive using basic authentication. Error: {result}"
            )
        self.access_token = result["access_token"]
        logger.info("Successfully connected to OneDrive")

    def connect(self) -> int:
        try:
            self.access_token
        except InvalidCredentialError as e:
            logger.authtest(e)
            return 1
        else:
            return 0

    def _request(
        self,
        method: str,
        endpoint: str,
        headers_override: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Make an HTTP request to the OneDrive API.

        Args:
            method (str): HTTP method (e.g., 'GET', 'POST').
            endpoint (str): API endpoint.
            headers_override (Optional[Dict[str, str]]): Optional headers to override default headers.
            **kwargs: Additional arguments for the request.

        Returns:
            Dict[str, Any]: The JSON response.

        Raises:
            ExitCodeException: If the request fails.
        """
        headers = headers_override or {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        response = request(
            method, f"{self.base_url}/{endpoint}", headers=headers, **kwargs
        )
        if response.ok:
            try:
                return response.json()
            except JSONDecodeError:
                return {}
        else:
            handle_errors(response.text, response.status_code)

    def get_user_id(self, user_email: str) -> str:
        """Get the user ID of the user with the given email.

        Args:
            user_email (str): The email of the user.

        Returns:
            str: The ID of the user.

        Raises:
            ValueError: If the user email is not provided.
            ExitCodeException: If the user ID cannot be retrieved or requests fail.
        """
        logger.debug(f"Attempting to get user ID for {user_email}...")
        if not user_email:
            raise ValueError(
                "User email was not provided when initializing the client. Please provide the user email."
            )

        user_info = self._request("GET", f"users/{user_email}")
        logger.debug(f"User ID: {user_info['id']}")
        return user_info["id"]

    def get_drive_id(self, user_id: str) -> str:
        """Get the drive ID of the user with the given ID.

        Args:
            user_id (str): The ID of the user to get the associated drive ID for.

        Returns:
            str: The Drive ID of the user.
        """
        logger.debug(f"Attempting to get drive ID for user ID {user_id}...")
        drive_info = self._request("GET", f"users/{user_id}/drive")
        logger.debug(f"Successfully retrieved Drive ID: {drive_info['id']}")
        return drive_info["id"]

    def upload(self, file_path: str, drive_id: str, drive_path: Optional[str]) -> None:
        """Uploads a file to OneDrive.

        Args:
            file_path (str): The path of the local file to upload.
            drive_id (str): The ID of the drive to upload the file to.
            drive_path (Optional[str]): The drive path to upload the file to.

        Raises:
            ExitCodeException: If the upload fails.
        """
        with open(file_path, "rb") as file:
            file_content = file.read()
        try:
            self._request(
                "PUT",
                f"drives/{drive_id}/root:/{drive_path}:/content",
                headers_override={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/octet-stream",
                },
                data=file_content,
            )
            logger.info(
                f"Successfully uploaded {file_path} to {drive_path} in OneDrive"
            )
        except Exception as e:
            raise ExitCodeException(
                f"Failed to upload {file_path} to {drive_path} in OneDrive: {e}",
                self.EXIT_CODE_UPLOAD_ERROR,
            ) from e

    def download(self, file_path: str, drive_path: str, drive_id: str) -> None:
        """Downloads a file from OneDrive.

        Args:
            file_path (str): The path to write to.
            drive_path (str): The path of the file to download in OneDrive.
            drive_id (str): The ID of the drive to download from.

        Raises:
            ExitCodeException: If the download fails.
        """

        response = request(
            "GET",
            f"{self.base_url}/drives/{drive_id}/root:/{drive_path}:/content",
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/octet-stream",
            },
        )

        if response.status_code == 200:
            with open(file_path, "wb") as file:
                file.write(response.content)
            logger.info(f"File downloaded successfully to {file_path}")
        else:
            logger.debug(
                f"Failed to download {file_path} from OneDrive. Ensure that the file and folder (if provided) exist."
            )
            raise ExitCodeException(
                f"Failed to download file from OneDrive: {response.text}",
                self.EXIT_CODE_DOWNLOAD_ERROR,
            )

    def move(
        self,
        src_name: str,
        src_dir: str,
        target_name: Optional[str],
        target_dir: str,
        drive_id: str,
    ) -> None:
        """Moves or renames a file in OneDrive.

        Args:
            src_name (str): The source file name.
            src_dir (str): The source directory.
            target_name (Optional[str]): The target file name.
            target_dir (str): The target directory.
            drive_id (str): The ID of the drive.

        Raises:
            ExitCodeException: If the move or rename operation fails.
        """
        data = {}
        item_id = self.get_file_id(src_name, drive_id, src_dir)

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

        logger.debug(f"Item ID: {item_id}")
        try:
            self._request("PATCH", f"drives/{drive_id}/items/{item_id}", json=data)
            logger.info(
                f"Successfully moved/renamed {src_name} to {target_name or src_name} in OneDrive"
            )
        except Exception as e:
            logger.error(
                f"Failed to move/rename {src_name} to {target_name or src_name} in OneDrive: {e}"
            )
            raise e

    def remove(self, drive_path: str, drive_id: str) -> None:
        """Removes a file from OneDrive.

        Args:
            drive_path (str): The path of the file to remove in OneDrive.
            drive_id (str): The ID of the drive to remove from.

        Raises:
            ExitCodeException: If the removal fails.
        """
        try:
            self._request("DELETE", f"drives/{drive_id}/root:/{drive_path}")
            logger.info(f"Successfully removed {drive_path} from OneDrive")
        except ExitCodeException as e:
            raise e
        except Exception as e:
            raise ExitCodeException(
                f"Failed to remove {drive_path} from OneDrive: {e}",
                self.EXIT_CODE_UNKNOWN_ERROR,
            ) from e

    def get_folder_id(self, folder_name: str, drive_id: str) -> Optional[str]:
        """Returns the folder ID of a folder in OneDrive.

        Args:
            folder_name (str): The name of the folder to fetch the ID of.
            drive_id (str): The ID of the drive to search in.

        Returns:
            Optional[str]: The ID of the folder or None.
        """
        folders = self._request("GET", f"drives/{drive_id}/root/children")

        folder_id = next(
            (
                folder["id"]
                for folder in folders["value"]
                if folder["name"] == folder_name
            ),
            None,
        )
        if folder_id is None:
            logger.info(f"Did not find a folder in OneDrive matching '{folder_name}'")
        return folder_id

    def get_file_id(
        self, file_name: str, drive_id: str, folder_name: str = ""
    ) -> Optional[str]:
        """Returns the file ID of a file in OneDrive.

        Args:
            file_name (str): The name of the file to fetch the ID of.
            drive_id (str): The ID of the drive to search in.
            folder_name (str, optional): The name of the folder to search in.

        Returns:
            Optional[str]: The ID of the file or None.
        """
        if folder_name:
            endpoint = f"drives/{drive_id}/root:/{folder_name}:/children"
        else:
            endpoint = f"drives/{drive_id}/root/children"
        try:
            files = self._request("GET", endpoint)
        except Exception as e:
            logger.error(f"Failed to fetch files from OneDrive: {e}")
            raise e

        file_id = next(
            (
                file["id"]
                for file in files["value"]
                if file["name"] == file_name and not file.get("folder")
            ),
            None,
        )
        if file_id is None:
            logger.info(f"Did not find a file in OneDrive matching '{file_name}'")
        return file_id

    def create_folder(self, folder_name: str, drive_id: str) -> str:
        """Creates a folder in OneDrive.

        Args:
            folder_name (str): The name of the folder to create.
            drive_id (str): The ID of the drive to create the folder in.

        Returns:
            str: The ID of the created folder.

        Raises:
            ExitCodeException: If the folder creation fails.
        """
        try:
            response = self._request(
                "POST",
                f"drives/{drive_id}/root/children",
                json={
                    "name": folder_name,
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "rename",
                },
            )
        except Exception as e:
            logger.error(f"Failed to create folder '{folder_name}' in OneDrive")
            raise e
        else:
            logger.info(f"Successfully created folder '{folder_name}' in OneDrive")
            return response["id"]

    def get_file_matches(self, folder: str, pattern: str, drive_id: str) -> List[str]:
        """Returns a list of files that match the given pattern.

        Args:
            folder (str): The folder to search in.
            pattern (str): The pattern to match on.
            drive_id (str): The ID of the drive to search in.

        Returns:
            List[str]: A list of files that match the pattern. If no matches are found, an empty list will be returned.
        """
        matching_files = []

        response = self._request("GET", f"drives/{drive_id}/root:/{folder}:/children")

        logger.debug("Successfully fetched files from OneDrive")
        files = response.get("value", [])
        regex = re.compile(pattern)
        matching_files.extend(
            file for file in files if "name" in file and regex.search(file["name"])
        )

        return matching_files

    def download_from_graph_url(self, download_url: str, file_name: str) -> None:
        """Download a file from a given URL.

        Args:
            download_url (str): The download URL from the Graph API.
            file_name (str): The name of the file to write to.

        Raises:
            ExitCodeException: If the download fails.
        """
        response = request("GET", download_url)
        if response.ok:
            with open(file_name, "wb") as file:
                file.write(response.content)
        else:
            handle_errors(response.text, response.status_code)

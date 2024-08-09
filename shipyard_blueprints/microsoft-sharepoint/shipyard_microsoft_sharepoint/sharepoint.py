import re
from json import JSONDecodeError
from typing import Optional, List, Dict, Any

import requests
from msal import ConfidentialClientApplication, PublicClientApplication
from requests import request
from shipyard_templates import ShipyardLogger, CloudStorage, ExitCodeException
from shipyard_templates.errors import InvalidCredentialError, handle_errors

from shipyard_microsoft_sharepoint.errs import SharepointSiteNotFoundError

logger = ShipyardLogger.get_logger()


class SharePointClient(CloudStorage):
    def __init__(
        self,
        access_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        tenant: Optional[str] = None,
        site_name: Optional[str] = None,
    ) -> None:
        self._access_token = access_token
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant = tenant
        self.base_url = "https://graph.microsoft.com/v1.0"

        self.site_name = site_name
        self._site_id = None

        super().__init__()

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

    def _set_access_token(self, result):
        if "access_token" not in result:
            raise InvalidCredentialError(
                f"Failed to connect to OneDrive using basic authentication. Error: {result}"
            )
        self.access_token = result["access_token"]
        logger.info("Successfully connected to SharePoint")

    def _generate_token_from_client(self):
        result = ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=f"https://login.microsoftonline.com/{self.tenant}",
        ).acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        self._set_access_token(result)

    def _generate_token_from_un_pw(self):
        result = PublicClientApplication(
            client_id=self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant}",
        ).acquire_token_by_username_password(
            username=self.username,
            password=self.password,
            scopes=["https://graph.microsoft.com/.default"],
        )
        self._set_access_token(result)

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
            logger.debug(
                f"Failed to {method} {endpoint}. Response <{response.status_code}>: {response.text}"
            )
            handle_errors(response.text, response.status_code)

    @property
    def site_id(self):
        try:
            if not self._site_id:
                self._site_id = self.get_site_id()

            return self._site_id
        except ExitCodeException:
            raise
        except Exception as e:
            raise SharepointSiteNotFoundError(self.site_name) from e

    def connect(self):
        try:
            self.access_token
        except InvalidCredentialError as e:
            logger.authtest(e)
            return 1
        else:
            return 0

    def upload(self, file_path: str, drive_path: Optional[str]):
        """Uploads a file to SharePoint
        Args:
            file_path: The path of the local file to upload
            drive_path: The drive path to upload the file to
        Raises:
            ExitCodeException:
        """

        with open(file_path, "rb") as file:
            file_content = file.read()

        self._request(
            "PUT",
            f"sites/{self.site_id}/drive/root:/{drive_path}:/content",
            headers_override={
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/octet-stream",
            },
            data=file_content,
        )

        logger.info("Successfully uploaded file to SharePoint")

    def download(self, file_path: str, drive_path: str):
        """Downloads a file from SharePoint

        Args:
            file_path: The path to write to
            drive_path: The path of the file to download in SharePoint
        """
        url = f"{self.base_url}/sites/{self.site_id}/drive/root:/{drive_path}:/content"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/octet-stream",
        }

        response = requests.get(url, headers=headers)
        logger.debug(f"Download url is {url}")
        logger.debug(
            f"Response: {response.text} and status code is {response.status_code}"
        )

        if response.ok:
            with open(file_path, "wb") as file:
                file.write(response.content)
            logger.info(f"File downloaded successfully to {file_path}")
        else:
            logger.error(
                f"Failed to download {file_path} from SharePoint. Ensure that the file and folder (if provide exist)"
            )
            handle_errors(response.text, response.status_code)

    def move(
        self,
        src_name: str,
        src_dir: Optional[str],
        target_name: Optional[str],
        target_dir: Optional[str],
    ):
        logger.debug(f"Src name is {src_name} and src dir is {src_dir}")
        item_id = self.get_file_id(src_name, src_dir)
        logger.debug(f"Item ID: {item_id}")

        data = {}
        if target_dir and src_dir != target_dir:
            folder_id = self.get_folder_id(target_dir)
            if not folder_id and target_dir != "":
                logger.info(f"Creating folder '{target_dir}' in SharePoint")
                folder_id = self.create_folder(target_dir)
                logger.debug(f"Folder ID: {folder_id}")
            data["parentReference"] = {"id": folder_id}

        if target_name and src_name != target_name:
            data["name"] = target_name

        if not data:
            logger.info(
                "No operation to perform (same source and target locations and names)."
            )
            return

        self._request("PATCH", f"sites/{self.site_id}/drive/items/{item_id}", json=data)
        logger.info(
            f"Successfully moved/renamed {src_name} to {target_name or src_name} in SharePoint"
        )

    def remove(
        self,
        drive_path: str,
    ):
        """Removes a file from SharePoint

        Args:
            drive_path: The path of the file to remove in SharePoint
        """

        self._request("DELETE", f"sites/{self.site_id}/drive/root:/{drive_path}")
        logger.info(f"Successfully removed {drive_path} from SharePoint")

    def get_folder_id(self, folder_name: str) -> Optional[str]:
        """Returns the folder ID of a folder in SharePoint

        Args:
            folder_name: The name of the folder to fetch the ID of

        Raises:
            BadRequestError:

        Returns: The ID of the folder or None

        """

        folders = self._request("GET", f"sites/{self.site_id}/drive/root/children")

        if folder_id := next(
            (
                folder["id"]
                for folder in folders["value"]
                if folder["name"] == folder_name
            ),
            None,
        ):
            return folder_id
        logger.info(f"Folder {folder_name} not found in SharePoint site")
        return

    def get_file_id(
        self, file_name: str, folder_name: Optional[str] = ""
    ) -> Optional[str]:
        """Returns the file ID of a file in SharePoint

        Args:
            file_name: The name of the file to fetch the ID of
            folder_name: The optional name of the folder to search within. If omitted, the root directory will be used

        Raises:
            BadRequestError:

        Returns: The ID of the file or None

        """
        if folder_name:
            endpoint = f"sites/{self.site_id}/drive/root:/{folder_name}:/children"
        else:
            endpoint = f"sites/{self.site_id}/drive/root/children"

        files = self._request("GET", endpoint)

        file_id = next(
            (
                file["id"]
                for file in files.get("value", [])
                if file["name"] == file_name and not file.get("folder")
            ),
            None,
        )

        if file_id:
            return file_id
        else:
            logger.error(
                f"Did not find a file in SharePoint site matching '{file_name}'"
            )
            return

    def create_folder(self, folder_name: str) -> str:
        """Creates a folder in SharePoint

        Args:
            folder_name: The name of the folder to create

        Raises:
            BadRequestError:

        Returns: The ID of the created folder

        """

        response = self._request(
            "POST",
            f"sites/{self.site_id}/drive/root/children",
            json={
                "name": folder_name,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "rename",
            },
        )

        logger.info(f"Successfully created folder '{folder_name}' in SharePoint")
        return response["id"]

    def get_file_matches(self, folder: str, pattern: str) -> List[str]:
        """Returns a list of files that match the given pattern

        Args:
            pattern: The pattern to match on
            folder: The name of the folder to search within

        Returns: A list of files that match the pattern. If no matches are found then an empty list will be returned

        """

        matching_files = []

        response = self._request(
            "GET", f"sites/{self.site_id}/drive/root:/{folder}:/children"
        )

        logger.debug("Successfully fetched files from SharePoint")
        files = response.get("value", [])
        regex = re.compile(pattern)
        matching_files.extend(
            file for file in files if "name" in file and regex.search(file["name"])
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
            logger.error(f"Failed to download file from {download_url} via url")
            handle_errors(response.text, response.status_code)

    def get_site_id(self) -> Optional[str]:
        """Returns the site ID of the SharePoint site

        Returns: The site ID if found

        """
        try:
            response = self._request("GET", f"sites?search={self.site_name}")
            if len(response["value"]) > 1:
                raise SharepointSiteNotFoundError(
                    f"Multiple sites matching the name '{self.site_name}' were found"
                )
            else:
                return response["value"][0]["id"]

        except ExitCodeException as e:
            if e.exit_code == self.EXIT_CODE_UNKNOWN_ERROR:
                raise SharepointSiteNotFoundError(
                    f"Failed to find site with name '{self.site_name}. Error: {e}"
                ) from e
            raise e

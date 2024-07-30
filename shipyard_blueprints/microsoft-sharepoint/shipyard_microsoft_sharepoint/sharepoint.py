import requests
import re

from typing import Optional, List, Dict
from msal import ConfidentialClientApplication
from shipyard_templates import ShipyardLogger, CloudStorage, ExitCodeException
from shipyard_templates.errors import (
    InvalidCredentialError,
    BadRequestError,
    handle_errors,
)

from shipyard_microsoft_sharepoint.errs import SharepointSiteNotFoundError


logger = ShipyardLogger.get_logger()


class SharePointClient(CloudStorage):
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant: str,
        site_name: Optional[str] = None,
    ) -> None:
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant = tenant
        self.site_name = site_name
        self.access_token = None  # will be set during the connect
        self._site_id = None

        super().__init__()

    @property
    def site_id(self):
        try:
            if not self._site_id:
                conn_result = self.connect()
                if conn_result == 0:
                    self._site_id = self.get_site_id()
                else:
                    raise InvalidCredentialError(
                        "Failed to connect to SharePoint using basic authentication. Ensure that the client ID, secret value, and tenant provided are correct"
                    )
            return self._site_id
        except ExitCodeException as ec:
            raise
        except Exception as e:
            raise SharepointSiteNotFoundError(self.site_name) from e

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
            logger.authtest(
                "Successfully connected to SharePoint using basic authentication"
            )
            return 0
        else:
            logger.authtest(
                "Failed to connect to SharePoint using basic authentication. Ensure that the client ID, secret value, and tenant provided are correct"
            )
            return 1

    def upload(self, file_path: str, drive_path: Optional[str]):
        """Uploads a file to SharePoint
        Args:
            file_path: The path of the local file to upload
            drive_path: The drive path to upload the file to
        Raises:
            ExitCodeException:
        """
        url = f"{self.base_url}/sites/{self.site_id}/drive/root:/{drive_path}:/content"

        with open(file_path, "rb") as file:
            file_content = file.read()
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/octet-stream",
        }
        response = requests.put(url, headers=headers, data=file_content)
        if response.ok:
            logger.info(
                f"Successfully uploaded {file_path} to {drive_path} in SharePoint"
            )
        else:
            logger.error(f"Failed to upload {file_path} to {drive_path} in SharePoint")
            handle_errors(response.text, response.status_code)

    def download(self, file_path: str, drive_path: str):
        """Downloads a file from SharePoint

        Args:
            file_path: The path to write to
            drive_path: The path of the file to download in SharePoint
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root:/{drive_path}:/content"
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

        url = f"{self.base_url}/sites/{self.site_id}/drive/items/{item_id}"

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        response = requests.patch(url, headers=headers, json=data)

        if response.ok:
            logger.info(
                f"Successfully moved/renamed {src_name} to {target_name or src_name} in SharePoint"
            )
        else:
            logger.error(
                f"Failed to move/rename {src_name} to {target_name or src_name} in SharePoint"
            )
            handle_errors(response.text, response.status_code)

    def remove(
        self,
        drive_path: str,
    ):
        """Removes a file from SharePoint

        Args:
            drive_path: The path of the file to remove in SharePoint
            drive_id: The ID of the drive to remove (only necessary if using basic authentication)
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root:/{drive_path}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
        }

        response = requests.delete(url, headers=headers)

        if response.ok:
            logger.info(f"Successfully removed {drive_path} from SharePoint")
        else:
            logger.error(f"Failed to remove {drive_path} from SharePoint")
            handle_errors(response.text, response.status_code)

    def get_folder_id(self, folder_name: str) -> Optional[str]:
        """Returns the folder ID of a folder in SharePoint

        Args:
            folder_name: The name of the folder to fetch the ID of

        Raises:
            BadRequestError:

        Returns: The ID of the folder or None

        """
        url = f"{self.base_url}/sites/{self.site_id}/drive/root/children"
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
            url = f"{self.base_url}/sites/{self.site_id}/drive/root:/{folder_name}:/children"
        else:
            url = f"{self.base_url}/sites/{self.site_id}/drive/root/children"
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
                logger.error(
                    f"Did not find a file in SharePoint site matching '{file_name}'"
                )
                return
        else:
            logger.error("Failed to get file ID")
            handle_errors(response.text, response.status_code)

    def create_folder(self, folder_name: str) -> str:
        """Creates a folder in SharePoint

        Args:
            folder_name: The name of the folder to create
            drive_id: The ID of the drive to create the folder in (only necessary if using basic authentication)

        Raises:
            BadRequestError:

        Returns: The ID of the created folder

        """

        url = f"{self.base_url}/sites/{self.site_id}/drive/root/children"

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
            logger.info(f"Successfully created folder '{folder_name}' in SharePoint")
            return response.json()["id"]

        else:
            logger.error(f"Failed to create folder '{folder_name}' in SharePoint")
            handle_errors(response.text, response.status_code)

    def get_file_matches(self, folder: str, pattern: str) -> List[str]:
        """Returns a list of files that match the given pattern

        Args:
            pattern: The pattern to match on
            folder: The name of the folder to search within

        Returns: A list of files that match the pattern. If no matches are found then an empty list will be returned

        """
        url = f"{self.base_url}/sites/{self.site_id}/drive/root:/{folder}:/children"

        headers = {"Authorization": f"Bearer {self.access_token}"}

        matching_files = []
        response = requests.get(url, headers=headers)
        if response.ok:
            logger.debug("Successfully fetched files from SharePoint")
            files = response.json().get("value", [])
            regex = re.compile(pattern)
            for file in files:
                if "name" in file and regex.search(file["name"]):
                    matching_files.append(file)

        else:
            logger.warning(
                f"Failed to fetch file matches from SharePoint: {response.text}"
            )
            handle_errors(response.text, response.status_code)
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
            url = f"{self.base_url}/sites?search={self.site_name}"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            resp_json = response.json()
            if len(resp_json["value"]) > 1:
                logger.error(
                    f"Multiple sites found with the name {self.site_name}. Please provide a unique site name"
                )
                raise SharepointSiteNotFoundError(self.site_name)
            site_id = resp_json["value"][0]["id"]
            return site_id
        except Exception as e:
            raise SharepointSiteNotFoundError(self.site_name) from e

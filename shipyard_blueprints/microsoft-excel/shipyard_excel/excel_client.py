import requests
import pandas as pd
from msal import ConfidentialClientApplication
from shipyard_microsoft_onedrive import OneDriveClient
from shipyard_templates import ExitCodeException, ShipyardLogger, Spreadsheets
from typing import Optional, List

logger = ShipyardLogger.get_logger()


class ExcelClient(OneDriveClient):
    def __init__(self, auth_type: str, access_token: Optional[str] = None):
        super().__init__(auth_type, access_token)

    def get_sheet_id(
        self, sheet_name: str, file_id: str, drive_id: Optional[str] = None
    ) -> str:
        """

        Args:
            sheet_name: The name of the sheet to get the ID of
            file_id: The ID of the file
            drive_id: The ID of the drive (only necessary if using basic auth)

        Raises:
            ExitCodeException:
            ExitCodeException:

        Returns: The ID of the sheet

        """
        if self.auth_type == "basic":
            url = (
                f"{self.base_url}/drives/{drive_id}/items/{file_id}/workbook/worksheets"
            )
        elif self.auth_type == "oauth":
            url = f"self.base_url/me/drive/items/{file_id}/workbook/worksheets"

        headers = {"Authorization": f"Bearer {self.access_token}"}

        response = requests.get(url, headers=headers)

        if response.ok:
            data = response.json()
            for sheet in data["value"]:
                if sheet["name"] == sheet_name:
                    return sheet["id"]
            raise ExitCodeException(
                f"Sheet {sheet_name} not found in file {file_id}",
                Spreadsheets.EXIT_CODE_BAD_REQUEST,
            )
        else:
            raise ExitCodeException(
                f"Error getting sheet id: {response.text}",
                Spreadsheets.EXIT_CODE_BAD_REQUEST,
            )

    def get_sheet_data(self, file_id: str, sheet: str, drive_id: Optional[str] = None):
        """Get the data from a sheet in an Excel file

        Args:
            file_id: The ID of the file
            sheet: The name or the ID of the sheet
            drive_id: The optional ID of the drive (only necessary if using basic auth)

        Raises:
            ExitCodeException:

        Returns: The data from the sheet in the form of JSON

        """
        if self.auth_type == "basic":
            url = f"{self.base_url}/drives/{drive_id}/items/{file_id}/workbook/worksheets/{sheet}/usedRange"
        else:
            url = f"{self.base_url}/me/drive/items/{file_id}/workbook/worksheets/{sheet}/usedRange"

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }

        response = requests.get(url, headers=headers)

        if response.ok:
            return response.json()
        else:
            raise ExitCodeException(
                f"Error getting sheet data: {response.text}",
                Spreadsheets.EXIT_CODE_BAD_REQUEST,
            )

    def get_sheet_data_as_df(
        self, file_id: str, sheet: str, drive_id: Optional[str] = None
    ):
        try:
            data = self.get_sheet_data(file_id, sheet, drive_id)["values"]
            df = pd.DataFrame(data[1:], columns=data[0])
            return df
        except ExitCodeException as ec:
            logger.error(ec)
            raise

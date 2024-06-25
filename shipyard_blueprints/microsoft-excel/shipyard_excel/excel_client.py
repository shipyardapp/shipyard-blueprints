import requests
import pandas as pd
from shipyard_microsoft_onedrive import OneDriveClient
from shipyard_templates import ExitCodeException, ShipyardLogger, Spreadsheets
from shipyard_templates import handle_errors
from typing import Optional

logger = ShipyardLogger.get_logger()


# NOTE: OAUTH suport has been removed for the time being. The only form of auth allowed at this point is the client credentials flow
class ExcelClient(OneDriveClient):
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        user_email: Optional[str] = None,
    ):
        super().__init__(client_id, client_secret, tenant_id, user_email)

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
        url = f"{self.base_url}/drives/{drive_id}/items/{file_id}/workbook/worksheets"

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
            logger.error("Error in getting the Sheet Id")
            handle_errors(response.text, response.status_code)

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
        url = f"{self.base_url}/drives/{drive_id}/items/{file_id}/workbook/worksheets/{sheet}/usedRange"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }

        response = requests.get(url, headers=headers)

        if response.ok:
            return response.json()
        else:
            logger.error("Error getting sheet data")
            handle_errors(response.text, response.status_code)

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

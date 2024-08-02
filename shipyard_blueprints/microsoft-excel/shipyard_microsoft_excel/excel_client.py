from typing import Optional

import pandas as pd
from shipyard_microsoft_onedrive import OneDriveClient
from shipyard_templates import ExitCodeException, ShipyardLogger, Spreadsheets

logger = ShipyardLogger.get_logger()


class ExcelClient(OneDriveClient):
    def __init__(
            self,
            access_token: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            client_id: Optional[str] = None,
            client_secret: Optional[str] = None,
            tenant: Optional[str] = None,
    ):
        super().__init__(
            access_token=access_token,
            username=username,
            password=password,
            client_id=client_id,
            client_secret=client_secret,
            tenant=tenant,
        )

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

        data = self._request("GET", f"drives/{drive_id}/items/{file_id}/workbook/worksheets")
        if sheet_id := next(
                (
                        sheet["id"]
                        for sheet in data["value"]
                        if sheet["name"] == sheet_name
                ),
                None,
        ):
            return sheet_id
        else:
            raise ExitCodeException(
                f"Sheet {sheet_name} not found in file {file_id}",
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
        return self._request("GET",
                             f"drives/{drive_id}/items/{file_id}/workbook/worksheets/{sheet}/usedRange")

    def get_sheet_data_as_df(
            self, file_id: str, sheet: str, drive_id: Optional[str] = None
    ):
        try:
            data = self.get_sheet_data(file_id, sheet, drive_id)["values"]
            return pd.DataFrame(data[1:], columns=data[0])
        except ExitCodeException as ec:
            raise ec

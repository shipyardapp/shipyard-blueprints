import requests

from shipyard_templates import Notebooks


class HexClient(Notebooks):
    def __init__(self, api_token: str, project_id: str) -> None:
        self.api_token = api_token
        self.project_id = project_id
        self.api_headers = {"Authorization": f"Bearer {self.api_token}"}
        super().__init__()

    def connect(self) -> int:
        """Connect to Hex

        Returns:
            int: exit code
        """

        try:
            response = requests.get(
                url=f"https://app.hex.tech/api/v1/project/{self.project_id}/runs",
                headers=self.api_headers,
            )
            response.raise_for_status()
        except Exception as e:
            print(e)
            return 1
        else:
            return 0

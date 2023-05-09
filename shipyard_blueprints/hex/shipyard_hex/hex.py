import requests
from shipyard_templates import Notebooks


class HexClient(Notebooks):
    def __init__(self, api_token: str, project_id: str) -> None:
        self.api_token = api_token
        self.project_id = project_id
        self.base_url = 'https://app.hex.tech/api/v1'
        self.api_headers = {"Authorization": f"Bearer {self.api_token}"}
        super().__init__()

    def connect(self) -> int:
        """ Connect to Hex

        Returns:
            int: HTTP status code
        """
        response = requests.get(
            url = f"https://app.hex.tech/api/v1/project/{self.projectId}/runs", headers= self.api_headers)
        return response.status_code

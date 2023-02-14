import requests
from templates.notebooks import Notebooks


class HexClient(Notebooks):
    def __init__(self, api_token: str, project_id: str) -> None:
        self.api_token = api_token
        self.project_id = project_id
        self.base_url = 'https://app.hex.tech/api/v1'
        self.api_headers = {"Authorization": f"Bearer {self.api_token}"}
        super().__init__()

    # TODO - replace with the hextoolkit or the hex_api package
    def connect(self):
        response = requests.get(
            url=f"{self.base_url}/project/{self.project_id}/run")
        return response

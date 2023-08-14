from shipyard_templates import DataVisualization
from pydomo import Domo
import requests
import os


class DomoClient(DataVisualization):
    def __init__(
        self,
        client_id: str = None,
        secret_key: str = None,
        access_token: str = None,
        domo_instance=None,
    ) -> None:
        self.client_id = client_id
        self.secret_key = secret_key
        self.domo_instance = domo_instance
        self.access_token = access_token

        # super().__init__(client_id=client_id, secret_key=secret_key)

    def connect_with_client_id_and_secret_key(self):
        try:
            self.domo_client = Domo(
                self.client_id, self.secret_key, api_host="api.domo.com"
            )
        except Exception as e:
            print(e)
            return 1
        else:
            return 0

    def connect_with_access_token(self):
        try:
            response = requests.get(
                f"https://{self.domo_instance}/api/content/v1/cards",
                headers={
                    "Content-Type": "application/json",
                    "x-domo-developer-token": self.access_token,
                },
            )
        except Exception as e:
            print(f"Error connecting with access token: {e}")
            return 1
        else:
            return 0 if response.ok else 1

    def connect(self):
        check_access_token = bool(self.access_token or self.domo_instance)
        check_client_id_secret = bool(self.client_id or self.secret_key)
        if check_access_token and check_client_id_secret:
            print(
                "Both Client ID and Secret Key and Access Token and Domo Instance ID were provided."
            )
            return (
                1
                if self.connect_with_client_id_and_secret_key()
                or self.connect_with_access_token() == 1
                else 0
            )
        elif check_client_id_secret:
            print("Only Client ID and Secret Key were provided.")
            return self.connect_with_client_id_and_secret_key()
        elif check_access_token:
            print("Only Access Token and Domo Instance ID were provided.")
            return self.connect_with_access_token()
        else:
            print(
                "Be sure to provide Client ID and Secret Key or Access Token and Domo Instance ID"
            )
            return 1

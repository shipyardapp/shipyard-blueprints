import requests
from requests.auth import HTTPBasicAuth
from shipyard_templates import DataVisualization, ShipyardLogger

logger = ShipyardLogger.get_logger()


class ModeClient(DataVisualization):
    def __init__(self, api_token: str, api_secret: str, account: str) -> None:
        self.api_token = api_token
        self.api_secret = api_secret
        self.account = account
        self.mode_api_base = f"https://app.mode.com/api/{self.account}"
        super().__init__(api_token=api_token, api_secret=api_secret, account=account)

    def connect(self):
        try:
            response = requests.get(
                url=f"{self.mode_api_base}/spaces",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/hal+json",
                },
                auth=HTTPBasicAuth(self.api_token, self.api_secret),
            )
        except Exception as error:
            logger.error(
                f"Could not connect to Mode API with the token id and token secret provided. Error: {error}"
            )
            return 1
        else:
            if response.status_code == 200:
                logger.info(f"Successfully connected to Mode API. {response.json()}")

                return 0
            else:
                logger.error(
                    f"Could not connect to Mode API with the token id and token secret provided {response.json()}"
                )
                return 1

import requests
from shipyard_templates import Etl


class DbtClient(Etl):
    def __init__(self, access_token: str, account_id: str):
        self.access_token = access_token
        self.account_id = account_id
        self.headers = {"Authorization": f"Bearer {access_token}"}
        self.account_url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/"
        self.base_url = f"https://cloud.getdbt.com/api/v2/"

        super().__init__(
            access_token=access_token,
            account_id=account_id,
            headers=self.headers,
            base_url=self.base_url,
            account_url=self.account_url,
        )

    def trigger_sync(self):
        pass

    def determine_sync_status(self):
        pass

    def connect(self):
        try:
            response = requests.get(self.account_url, headers=self.headers)
            if response.status_code == 200:
                self.logger.info("Successfully connected to DBT")
                return 0
            else:
                self.logger.error("Could not connect to DBT")
                return 1
        except Exception as e:
            self.logger.error("Could not connect to DBT")
            return 1

from templates.datavisualization import DataVisualization
from pydomo import Domo


class DomoClient(DataVisualization):
    def __init__(self, client_id: str, secret_key: str) -> None:
        self.client_id = client_id
        self.secret_key = secret_key
        self.api_host = "api.domo.com"
        super().__init__(client_id=client_id, secret_key=secret_key)

    def connect(self):
        domo = Domo(self.client_id, self.secret_key, self.api_host)
        return domo

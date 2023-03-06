from templates.datavisualization import DataVisualization
import tableauserverclient as tsc


class TableauClient(DataVisualization):
    def __init__(self, username: str, password: str, server_url: str, site: str = '') -> None:
        self.username = username
        self.password = password
        self.server_url = server_url
        self.site = site
        super().__init__(username=username, password=password, site=site)

    def connect(self, **kwargs):
        tableau_auth = tsc.TableauAuth(
            self.username, self.password, site_id=self.site)
        server = tsc.Server(self.server_url, use_server_version=True)
        conn = server.auth.sign_in(tableau_auth)
        return conn

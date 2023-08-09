import tableauserverclient as tsc
from shipyard_templates import DataVisualization
import os


class TableauClient(DataVisualization):
    def __init__(
        self, username: str, password: str, server_url: str, site: str = ""
    ) -> None:
        self.username = username
        self.password = password
        self.server_url = server_url
        self.site = site
        super().__init__(username=username, password=password, site=site)

    def connect(self, sign_in_method, **kwargs):
        if sign_in_method not in ["username_password", "access_token"]:
            self.logger.error(f"Invalid sign in method: {sign_in_method}")
            return 1
        elif sign_in_method == "username_password":
            tableau_auth = tsc.TableauAuth(
                self.username, self.password, site_id=self.site
            )
        elif sign_in_method == "access_token":
            tableau_auth = tsc.PersonalAccessTokenAuth(
                token_name=self.username,
                personal_access_token=self.password,
                site_id=self.site,
            )
        try:
            server = tsc.Server(self.server_url, use_server_version=True)
            server.auth.sign_in(tableau_auth)
        except Exception as error:
            self.logger.error(f"Could not connect to Tableau: {error}")
            return 1
        else:
            self.logger.info("Successfully connected to Tableau")
            return 0

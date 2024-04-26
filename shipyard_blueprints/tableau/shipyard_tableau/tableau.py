import tableauserverclient as tsc
from shipyard_templates import DataVisualization, ShipyardLogger

logger = ShipyardLogger.get_logger()


class TableauClient(DataVisualization):
    def __init__(
        self, username: str, password: str, server_url: str, site: str = ""
    ) -> None:
        self.username = username
        self.password = password
        self.server_url = server_url
        self.site = site

    def connect(self, sign_in_method="", **kwargs):
        if not sign_in_method:
            logger.debug("No sign in method provided.")
            try:
                self.connect_username_password()
                logger.authtest("Successfully connected with username_password")
                return 0
            except Exception as username_password_error:
                logger.debug(f"Failed to connect with UN & PW. Message from Tableau Server {username_password_error} \n"
                             f"Attempting with token...")
                try:
                    self.connect_access_token()
                    logger.authtest("Successfully connected with Access Token")
                    return 0
                except Exception as token_error:
                    logger.debug(f"Failed to connect with token: {token_error}")
                    logger.authtest(
                        "Authentication attempt failed.\n"
                        "Attempted with Username & Password and Personal Access Token but could not verify "
                        "credentials.\n"
                        "Please ensure the account does not have MFA enabled when using Username & Password.\n"
                        "Error messages from Tableau server:\n"
                        f" - Username & Password error: {username_password_error}\n"
                        f" - Personal Access Token error: {token_error}"
                    )
                    return 1

        try:
            if sign_in_method not in ["username_password", "access_token"]:
                logger.authtest(f"Invalid sign in method: {sign_in_method}")
                return 1
            elif sign_in_method == "username_password":
                self.connect_username_password()

            elif sign_in_method == "access_token":
                self.connect_access_token()
        except Exception as error:
            logger.authtest(f"Could not connect to Tableau: {error}")
            return 1
        logger.authtest("Successfully connected to Tableau")
        return 0

    def connect_username_password(self):
        logger.debug("Attempting to Tableau with username and password")
        tableau_auth = tsc.TableauAuth(
            self.username, self.password, site_id=self.site
        )
        server = tsc.Server(self.server_url, use_server_version=True)
        server.auth.sign_in(tableau_auth)
        logger.debug("Successfully connected to Tableau with username and password")
        return server

    def connect_access_token(self):
        logger.debug("Attempting to connect to Tableau with token...")
        tableau_auth = tsc.PersonalAccessTokenAuth(
            token_name=self.username,
            personal_access_token=self.password,
            site_id=self.site,
        )
        server = tsc.Server(self.server_url, use_server_version=True)
        server.auth.sign_in(tableau_auth)
        logger.debug("Successfully connected to Tableau with token")
        return server

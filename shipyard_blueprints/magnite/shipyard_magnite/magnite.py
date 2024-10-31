from shipyard_templates import DigitalAdverstising


class MagniteClient(DigitalAdverstising):
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password

    def _get_token():
        """
        Queries the API for an access token with the given username and password
        """
        pass

    def connect(self):
        pass

    def update(self, **kwargs):
        pass

    def create(self, **kwargs):
        pass

    def delete(self, **kwargs):
        pass

    def read(self, **kwargs):
        pass

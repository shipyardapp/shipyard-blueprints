import requests
from shipyard_templates import ProjectManagement

class ClickupClient(ProjectManagement):
    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        super().__init__(access_token)


    def connect():
        pass

    def create_ticket():
        pass

    def get_ticket():
        pass

    def update_ticket():
        pass


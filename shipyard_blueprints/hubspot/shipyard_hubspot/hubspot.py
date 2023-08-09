import json

from logging import DEBUG
from requests import request
from shipyard_templates import Crm, ExitCodeException


class HubspotClient(Crm):
    def __init__(self, access_token: str, verbose: bool = False) -> None:
        self.access_token = access_token



        super().__init__(access_token)

        self.logger.info("HubspotClient initialized")
        if verbose:
            self.logger.info("Verbose mode enabled")
            self.logger.setLevel(DEBUG)

    def _requests(self, endpoint: str, method: str = "GET", payload: dict = None):
        """
        Helper function to make requests to the Hubspot API

        :param endpoint: The endpoint to make the request to
        :param method: The HTTP method to use
        :param payload: The payload to send with the request
        :return: The response from the request
        """

        headers = {"Authorization": f"Bearer {self.access_token}"}
        if payload:
            headers["Content-Type"] = "application/json"

        response = request(
            method=method,
            url=f"https://api.hubapi.com/{endpoint}",
            data=json.dumps(payload),
            headers=headers,
        )

        self.logger.info(f"Response status code: {response.status_code}")
        try:
            response_details = response.json()
        except json.decoder.JSONDecodeError:
            self.logger.debug(f"Response body is not JSON: {response.text}")
            response_details = {}

        if response.ok:
            return response_details
        if response.status_code in {401, 403}:
            raise ExitCodeException(
                response_details.get("message", "Invalid credentials"),
                self.EXIT_CODE_INVALID_CREDENTIALS,
            )
        elif response.status_code == 429:
            raise ExitCodeException(
                response_details.get("message", "Rate limit exceeded"),
                self.EXIT_CODE_RATE_LIMIT,
            )
        elif response.status_code in {502, 504, 522, 524}:
            raise ExitCodeException(
                response_details.get("message", "Gateway timeout"), self.TIMEOUT
            )
        elif response.status_code in {503, 521, 523}:
            raise ExitCodeException(
                response_details.get("message", "Service unavailable"),
                self.EXIT_CODE_SERVICE_UNAVAILABLE,
            )
        elif response.status_code == 415:
            raise ExitCodeException(
                response_details.get("message", "Bad Request"),
                self.EXIT_CODE_BAD_REQUEST,
            )
        else:
            raise ExitCodeException(response.text, self.EXIT_CODE_UNKNOWN_ERROR)

    def connect(self):
        """
        Method for verifying connection to the Hubspot API
        """
        self.logger.debug("Verifying connection to Hubspot API")
        try:
            self._requests("crm/v3/imports/")
        except ExitCodeException:
            return 1
        else:
            self.logger.info("Successfully connected to Hubspot API")
            return 0

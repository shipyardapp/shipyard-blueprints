import json
import os
import requests
import pandas as pd
from shipyard_templates import DigitalAdvertising, ShipyardLogger, ExitCodeException
from shipyard_templates import InvalidCredentialError
from shipyard_magnite.errs import (
    ReadError,
)

from shipyard_magnite.dataclasses.targeting_spend_profile import TargetingSpendProfile
from typing import Dict, List, Any, Optional, Union

logger = ShipyardLogger.get_logger()


class MagniteClient(DigitalAdvertising):
    API_BASE_URL = "https://console.springserve.com/api/v0"

    def __init__(self, username: str, password: str) -> None:
        """
        Initialize the Magnite client with credentials.
        """
        self.username = username
        self.password = password
        self._token = None

    @property
    def token(self) -> str:
        """
        Retrieves the access token. Authenticates if the token is not already set.
        """
        if not self._token:
            url = f"{self.API_BASE_URL}/auth"
            payload = json.dumps({"email": self.username, "password": self.password})
            response = requests.post(
                url, headers={"Content-Type": "application/json"}, data=payload
            )
            if response.ok:
                self._token = response.json().get("token")
            else:
                raise InvalidCredentialError(
                    f"Invalid credentials. HTTP {response.status_code}: {response.reason}. Response: {response.text}"
                )
        return self._token

    def _request(self, method: str, endpoint: str, **kwargs):
        """
        Makes a request to the Magnite API

        Args:
            method: The HTTP method to use
            endpoint: The endpoint to hit
            **kwargs: Additional arguments to pass to the request

        Returns:
            The response from the API
        """

        url = f"{self.API_BASE_URL}/{endpoint}"
        logger.debug(f"Attempting to make a {method} request to {url}")
        response = requests.request(
            method=method,
            url=url,
            headers={"Content-Type": "application/json", "Authorization": self.token},
            **kwargs,
        )
        logger.debug(f"Response code: {response.status_code}")
        logger.debug(f"Response data: {response.text}")
        if response.status_code == 401:
            raise InvalidCredentialError(
                f"Invalid credentials <{response.status_code}>: Response from Magnite Server {response.reason}. Response: {response.text}"
            )
        response.raise_for_status()

        try:
            return response.json()
        except json.JSONDecodeError:
            logger.debug("No JSON data in response. Returning raw response.")
            return response

    def connect(self):
        """
        Queries the API for an access token with the given username and password
        """
        try:
            self.token
        except Exception:
            return 1
        else:
            return 0

    def create(self, **kwargs):
        raise NotImplementedError("Create functionality is not implemented yet.")

    def read(self, endpoint):
        try:
            return self._request("GET", endpoint=endpoint)
        except ExitCodeException:
            raise
        except Exception as e:
            raise ReadError(f"Error in reading data: {e}")

    def update(self, **kwargs):
        raise NotImplementedError("Update functionality is not implemented yet.")

    def delete(self, **kwargs):
        raise NotImplementedError("Delete functionality is not implemented yet.")

    def get_campaign_by_id(self, id: str):
        logger.debug(f"Attempting to get campaign {id}...")
        response = self.read(endpoint=f"campaigns/{id}")
        logger.debug(f"Successfully retrieved campaign {id}: \n{response}")
        return response

    def export_campaign_by_id(self, id: str, filename: str = None):
        logger.debug(f"Attempting to export campaign {id} to {filename}...")
        valid_file_types = ["json", "csv"]
        filename = filename or f"campaign_{id}.json"
        file_type = os.path.splitext(filename)[-1].lstrip(".")
        if file_type not in valid_file_types:
            raise ValueError(
                f"Invalid file type. Please use one of the following: {valid_file_types}"
            )

        campaign_data = self.get_campaign_by_id(id)
        if file_type == "csv":
            if isinstance(campaign_data, list):
                df = pd.json_normalize(campaign_data)
            else:
                df = pd.json_normalize(
                    [campaign_data]
                )  # Wrap single dictionary in a list

            df.to_csv(filename, index=False)
        elif file_type == "json":
            with open(filename, "w") as f:
                json.dump(campaign_data, f)
        logger.debug(f"Successfully exported campaign {id} to {filename}")

    def update_campaign_budgets(
        self, campaign_id, target_spend_profile: TargetingSpendProfile
    ):
        if not isinstance(target_spend_profile, TargetingSpendProfile):
            raise TypeError(
                "Expected a TargetingSpendProfile object. There are utils to help you create one."
            )
        try:
            logger.debug(
                f"Attempting to update campaign {campaign_id} with budget data: {target_spend_profile}..."
            )
            response = self._request(
                "PUT",
                endpoint=f"campaigns/{campaign_id}",
                json=target_spend_profile.to_payload(),
            )
            logger.debug(f"Successfully updated campaign {campaign_id}: {response}")
            return response
        except ExitCodeException:
            raise
        except Exception as e:
            logger.error(f"Error in updating campaign {campaign_id}: {e}")
            raise

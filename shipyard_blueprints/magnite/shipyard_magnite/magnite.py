import json
import requests
from shipyard_templates import DigitalAdverstising, ShipyardLogger, ExitCodeException

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Any, Optional, Union

logger = ShipyardLogger.get_logger()


class MagniteClient(DigitalAdverstising):
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password
        self.base_url = "https://console.springserve.com/api/v0"
        self.headers = {"Content-Type": "application/json"}

    def connect(self):
        """
        Queries the API for an access token with the given username and password
        """
        url = f"{self.base_url}/auth"

        payload = json.dumps({"email": self.username, "password": self.password})
        headers = {"Content-Type": "application/json"}
        response = requests.post(url, headers=headers, data=payload)
        if response.ok:
            self.token = response.json().get("token")
            self.headers["Authorization"] = self.token
            return 0
        else:
            return 1

    def update(
        self, id: Optional[str], file: Optional[str], budget_value: Optional[str]
    ):
        if id and file:
            # TODO: replace the hardcoded error value
            raise ExitCodeException("Both an ID and file cannot be provided", 200)
        if id:
            self.update_single(id, budget_value)
        elif file:
            self.update_multiple()

    def create(self, **kwargs):
        pass

    def delete(self, **kwargs):
        pass

    def read(self, endpoint: str, id: str) -> Dict[Any, Any]:
        """Returns the JSON from the springserve API for the given endpoint (assuming it is valid)

        Args:
            endpoint: The Springserve endpoint to query
            id: The ID of the item to fetch

        """
        self._form_url(endpoint)
        endpoint_url = f"{self.endpoint_url}/{id}"
        response = requests.get(endpoint_url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def update_single(
        self, endpoint: str, id: str, budget_value: Union[str, float, int], **kwargs
    ):
        """

        Args:
            endpoint:
            id:
            budget_value:
            **kwargs:

        Raises:
            ExitCodeException:
        """
        try:
            budget_payload = self._make_campaign_budget_payload(
                id, budget_value, **kwargs
            )
            self._form_url(endpoint)

            url = f"{self.endpoint_url}/{id}"

            resp = requests.put(url, headers=self.headers, json=budget_payload)
            resp.raise_for_status()

            logger.info(f"Successfully updated budget for ID {id}")
            logger.debug(f"Response code: {resp.status_code}")
            logger.debug(f"Response data: {resp.text}")
        except Exception as e:
            raise ExitCodeException(f"Error in updating budget for {id}: {e}", 1)

    def update_multiple(self):
        pass

    def _make_campaign_budget_payload(
        self, campaign_id: str, budget_value: Union[str, float, int], **kwargs
    ):
        """

        Args:
            campaign_id:
            budget_value:
            **kwargs:

        Returns:

        """
        return {
            "targeting_spend_profile": {
                "id": 224960,
                "budgets": [
                    {
                        "id": 945679,
                        "budget_value": budget_value,
                        # "budget_pacing": kwargs.get("budget_pacing", None),
                        # "budget_period": kwargs.get("budget_period", None),
                        # "budget_metric": kwargs.get("budget_metric", None),
                    }
                ],
            }
        }

    def _form_url(self, endpoint: str):
        self.endpoint_url = f"{self.base_url}/{endpoint}"

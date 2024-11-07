import json
import requests
import pandas as pd
from shipyard_templates import DigitalAdverstising, ShipyardLogger, ExitCodeException
from shipyard_magnite.errs import EXIT_CODE_INVALID_ARGS, UpdateError

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
        self,
        endpoint: str,
        id: Optional[str],
        file: Optional[str],
        budget_value: Optional[str],
        budget_period=budget_period,
        budget_pacing=budget_pacing,
    ):
        try:
            if id and file:
                raise ExitCodeException(
                    "Both an ID and file cannot be provided", EXIT_CODE_INVALID_ARGS
                )
            if id:
                data = self.read(endpoint, id)
                self.update_single(
                    endpoint=endpoint,
                    id=id,
                    campaign_data=data,
                    budget_value=budget_value,
                )
            elif file:
                df = pd.read_csv(file)
                for i, row in df.iterrows():
                    id = row["id"]
                    data = self.read(endpoint, id)
                    budget_value = row["budget_value"]
                    self.update_single(
                        endpoint=endpoint,
                        id=id,
                        campaign_data=data,
                        budget_value=budget_value,
                    )
        except ExitCodeException:
            raise
        except Exception as e:
            pass

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
        self,
        endpoint: str,
        id: str,
        budget_value: Union[str, float, int],
        campaign_data: Dict[str, Any],
        **kwargs,
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
                id, budget_value, campaign_data=campaign_data, **kwargs
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
        self,
        campaign_id: str,
        budget_value: Union[str, float, int],
        campaign_data: Dict[str, Any],
        **kwargs,
    ):
        """

        Args:
            campaign_id:
            budget_value:
            **kwargs:

        Returns:

        """
        data = {
            "targeting_spend_profile": {
                "id": campaign_data.get("targeting_spend_profile").get("id"),
                "budgets": [
                    {
                        "id": campaign_data.get("targeting_spend_profile")
                        .get("budgets")[0]
                        .get("id"),
                        "budget_value": budget_value,
                    }
                ],
            }
        }
        if budget_pacing := kwargs.get("budget_pacing", None):
            data["targeting_spend_profile"]["budgets"][0][
                "budget_pacing"
            ] = budget_pacing
        if budget_period := kwargs.get("budget_period", None):
            data["targeting_spend_profile"]["budgets"][0][
                "budget_period"
            ] = budget_period
        if budget_metric := kwargs.get("budget_metric", None):
            data["targeting_spend_profile"]["budgets"][0][
                "budget_period"
            ] = budget_metric

        return data

    def _form_url(self, endpoint: str):
        self.endpoint_url = f"{self.base_url}/{endpoint}"

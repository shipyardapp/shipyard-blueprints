import json
import requests
import pandas as pd
from shipyard_templates import DigitalAdverstising, ShipyardLogger, ExitCodeException
from shipyard_magnite.errs import (
    EXIT_CODE_INVALID_ARGS,
    InvalidColumns,
    ReadError,
    UpdateError,
)

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Any, Optional, Union

logger = ShipyardLogger.get_logger()


class MagniteClient(DigitalAdverstising):
    REQUIRED_FIELDS = ["id", "budget_value"]
    OPTIONAL_FIELDS = ["budget_pacing", "budget_period"]  # can expand as needed

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
        **kwargs,
    ):
        """

        Args:
            endpoint: The endpoint to target - should be campaign or demand_tags for now
            id: The optional ID of the associated item
            file: The optional file for bulk uploads. If using a file, the following columns must be present: `campaign_id`, `budget_value`
            budget_value: The new budget value to set
            **kwargs: Additional fields to set such as `budget_metric`, `budget_period`, and `budget_pacing`

        Raises:
            ExitCodeException:
        """
        try:
            results = []
            if id and file:
                raise ExitCodeException(
                    "Both an ID and file cannot be provided", EXIT_CODE_INVALID_ARGS
                )
            if id:
                res = self.update_single(
                    endpoint=endpoint,
                    id=id,
                    budget_value=budget_value,
                    **kwargs,
                )
                results.append(res)

            elif file:
                df = pd.read_csv(file)
                # check the columns
                columns = df.columns.tolist()
                logger.debug(f"Columns are {columns}")
                column_check = list(map(lambda x: x in columns, self.REQUIRED_FIELDS))
                if not all(column_check):
                    raise InvalidColumns(
                        f"The columns in the provided file are incorrect. Please ensure that they match the following: \n ['id', 'budget_value', 'budget_pacing', 'budget_period']"
                    )

                logger.debug(df.head())
                for i, row in df.iterrows():
                    id = str(row["id"])
                    budget_value = str(row["budget_value"])
                    res = self.update_single(
                        endpoint=endpoint,
                        id=id,
                        budget_value=budget_value,
                    )
                    results.append(res)
            return results
        except Exception as e:
            raise UpdateError(
                f"An error occurred in attempting to update item {id}: {e}"
            )

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
        try:
            self._form_url(endpoint)
            endpoint_url = f"{self.endpoint_url}/{id}"
            response = requests.get(endpoint_url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            raise ReadError(f"Error in reading data from {endpoint_url}: {e}")

    def update_single(
        self,
        endpoint: str,
        id: str,
        budget_value: Union[str, float, int],
        **kwargs,
    ) -> bool:
        """
        Helper function to update a single item

        Args:
            endpoint: The endpoint to target - should be campaign or demand_tags for now
            id: The optional ID of the associated item
            file: The optional file for bulk uploads. If using a file, the following columns must be present: `campaign_id`, `budget_value`
            budget_value: The new budget value to set
            **kwargs: Additional fields to set such as `budget_metric`, `budget_period`, and `budget_pacing`

        Returns: True if the response was successful and false otherwise

        """
        try:
            campaign_data = self.read(endpoint=endpoint, id=id)
            budget_payload = self._make_campaign_budget_payload(
                id, budget_value, campaign_data=campaign_data, **kwargs
            )
            self._form_url(endpoint)

            url = f"{self.endpoint_url}/{id}"

            resp = requests.put(url, headers=self.headers, json=budget_payload)

            logger.info(f"Successfully updated budget for ID {id}")
            logger.debug(f"Response code: {resp.status_code}")
            logger.debug(f"Response data: {resp.text}")
            if not resp.ok:
                logger.error(f"Error in updating ID {id}: {resp.text}")
                return False

        except ReadError as re:
            logger.error(f"Error in reading data for ID {id}: {re}")
            return False
        except Exception as e:
            logger.error(f"Error in updating ID {id}: {e}")
            return False
        else:
            return True

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
        if budget_pacing := kwargs.get("budget_pacing"):
            data["targeting_spend_profile"]["budgets"][0][
                "budget_pacing"
            ] = budget_pacing
        if budget_period := kwargs.get("budget_period"):
            data["targeting_spend_profile"]["budgets"][0][
                "budget_period"
            ] = budget_period
        if budget_metric := kwargs.get("budget_metric"):
            data["targeting_spend_profile"]["budgets"][0][
                "budget_period"
            ] = budget_metric

        return data

    def _form_url(self, endpoint: str):
        self.endpoint_url = f"{self.base_url}/{endpoint}"

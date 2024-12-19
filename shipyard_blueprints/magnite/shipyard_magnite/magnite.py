import json
import os
import requests
import pandas as pd
from shipyard_templates import DigitalAdvertising, ShipyardLogger, ExitCodeException
from shipyard_templates import InvalidCredentialError
from shipyard_magnite.errs import (
    InvalidArgs,
    InvalidColumns,
    ReadError,
    UpdateError,
    PartialInvalidBudgetPayload,
)

from shipyard_magnite import utils
from dataclasses import dataclass
from typing import Dict, List, Any, Optional, Union

logger = ShipyardLogger.get_logger()


class MagniteClient(DigitalAdvertising):
    REQUIRED_FIELDS = ["id", "budget_value"]
    API_BASE_URL = "https://console.springserve.com/api/v0"

    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password
        self._token = None

    @property
    def token(self):
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
                    f"Invalid credentials. Response from the server: {response.text}"
                )
        return self._token

    @token.setter
    def token(self, value):
        self._token = value
        self.headers["Authorization"] = value

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

    def update(
        self,
        endpoint: str,
        id: Optional[str] = None,
        file: Optional[str] = None,
        budget_value: Optional[str] = None,
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
            InvalidArgs: If both an ID and file are provided
            UpdateError: If an error occurs in updating the item
        """
        try:
            results = []
            if id and file:
                raise InvalidArgs("Both an ID and file cannot be provided")
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
                columns = df.columns.tolist()
                logger.debug(f"Columns are {columns}")
                column_check = list(map(lambda x: x in columns, self.REQUIRED_FIELDS))
                if not all(column_check):
                    raise InvalidColumns(
                        f"The columns in the provided file are incorrect. Please ensure that they match the following: \n {self.REQUIRED_FIELDS}"
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
        except FileNotFoundError:
            raise UpdateError(f"The file '{file}' does not exist.")
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
            endpoint += f"/{id}"
            logger.debug(f"Reading data from {endpoint}...")
            response = utils._request(self, "GET", endpoint=endpoint)
            logger.debug(
                f"Response: {response.status_code} \n {json.dumps(response.json(), indent=2)}"
            )

            try:
                return response.json()
            except json.JSONDecodeError as e:
                raise ReadError(f"Error decoding JSON response from {endpoint}: {e}")
        except requests.HTTPError as e:
            raise ReadError(f"Error in reading data from {endpoint}: {e}")

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
            budget_value: The new budget value to set
            **kwargs: Additional fields to set such as `budget_metric`, `budget_period`, and `budget_pacing`

        Returns: True if the response was successful and false otherwise

        """
        try:
            campaign_data = self.read(endpoint=endpoint, id=id)
            budget_payload = utils._make_campaign_budget_payload(
                budget_value, campaign_data=campaign_data, **kwargs
            )
            utils._request(
                self, "PUT", endpoint=f"{endpoint}/{id}", json=budget_payload
            )

        except ReadError as re:
            logger.error(f"Error in reading data for ID {id}: {re}")
            return False
        except Exception as e:
            logger.error(f"Error in updating ID {id}: {e}")
            return False
        else:
            logger.info(f"Successfully updated budget for ID {id}")
            return True

    def get_campaign_by_id(self, id: str):
        return self.read("campaigns", id)

    def export_campaign_by_id(self, id: str, filename: str = None):
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
                )  # Wrap single dictionary in a list``

            df.to_csv(filename, index=False)
            # df = pd.DataFrame(campaign_data)
            # df.to_csv(filename)
        elif file_type == "json":
            with open(filename, "w") as f:
                json.dump(campaign_data, f)

    def update_campaign_budgets(self, campaign_id, budget_data):
        # TODO: Support budget_flight_dates
        try:
            logger.debug(
                f"Attempting to update campaign {campaign_id} with budget data: {budget_data}..."
            )
            campaign_data = self.get_campaign_by_id(campaign_id)

            payload = {
                "targeting_spend_profile": {
                    "id": campaign_data["targeting_spend_profile"]["id"]
                }
            }
            budgets, has_errors = utils.validate_budget_data(budget_data)

            if has_errors:
                raise PartialInvalidBudgetPayload(
                    f"Partially invalid budget data for campaign {campaign_id}"
                )

            payload["targeting_spend_profile"]["budgets"] = budgets

            logger.debug(f"Payload: {payload}")

            return utils._request(
                self,
                "PUT",
                endpoint=f"campaigns/{campaign_id}",
                json=payload,
            )

        except ExitCodeException:
            raise
        except Exception as e:
            logger.error(f"Error in updating campaign {campaign_id}: {e}")
            raise


if __name__ == "__main__":
    from dotenv import load_dotenv
    import requests
    from shipyard_magnite import utils

    # from requests import request
    load_dotenv()

    client = MagniteClient(
        username=os.getenv("MAGNITE_USERNAME"), password=os.getenv("MAGNITE_PASSWORD")
    )

    campaign_endpoint = client.API_BASE_URL + "/campaigns"
    campaign_url = f"{campaign_endpoint}/74255"
    headers = {"Content-Type": "application/json", "Authorization": client.token}
    # budgets ={
    #     "targeting_spend_profile":{
    #         "id":224960,
    #         "budgets":[
    #      {
    #         "budget_metric":"gross_cost",
    #         "budget_period":"week",
    #         "budget_pacing":"smooth",
    #         "budget_value":"10000"
    #      },
    #     #  {
    #     #     "budget_metric":"gross_cost",
    #     #     "budget_period":"day",
    #     #     "budget_pacing":"even",
    #     #     "budget_value":"10000"
    #     #  }
    #   ]},
    # }

    budgets = {
        "targeting_spend_profile": {
            "id": 224960,
            "budgets": [
                {
                    "budget_metric": "gross_cost",
                    "budget_period": "week",
                    "budget_pacing": "smooth",
                    "budget_value": "1000",
                },
                {
                    "budget_metric": "gross_cost",
                    "budget_period": "day",
                    "budget_pacing": "even",
                    "budget_value": "10000",
                },
            ],
        }
    }

    response = requests.put(campaign_url, headers=headers, json=budgets)
    print(response.status_code)
    print(response.json())

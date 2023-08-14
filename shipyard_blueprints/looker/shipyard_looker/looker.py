import looker_sdk
from looker_sdk import api_settings
from shipyard_templates import DataVisualization
import os
import logging


class MyApiSettings(api_settings.ApiSettings):
    def __init__(self, *args, **kw_args):
        self.base_url = kw_args.pop("base_url")
        if "https://" not in self.base_url:
            self.base_url = f"https://{self.base_url}"
        self.client_id = kw_args.pop("client_id")
        self.client_secret = kw_args.pop("client_secret")
        super().__init__(*args, **kw_args)

    def read_config(self):
        config = super().read_config()
        config["base_url"] = self.base_url
        config["client_id"] = self.client_id
        config["client_secret"] = self.client_secret
        return config


class LookerClient(DataVisualization):
    def __init__(self, base_url: str, client_id: str, client_secret: str) -> None:
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        super().__init__(
            base_url=base_url, client_id=client_id, client_secret=client_secret
        )

    def _get_sdk(self):
        print(self.base_url)
        try:
            sdk = looker_sdk.init40(
                config_settings=MyApiSettings(
                    base_url=self.base_url,
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                )
            )
        except Exception as e:
            logging.exception(f"Error initializing Looker SDK: {e}")
            raise e
        return sdk

    def connect(self):
        try:
            sdk = self._get_sdk()
            print(sdk.me())
        except Exception as e:
            logging.exception(f"Error connecting to Looker: {e}")
            return 1
        else:
            return 0

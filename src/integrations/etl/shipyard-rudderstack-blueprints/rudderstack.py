import requests
import sys
import shipyard_utils as shipyard
from ..etl import Etl


class RudderStack(Etl):
    def __init__(self, vendor: str, access_token: str) -> None:
        super().__init__(vendor, access_token)
        self.api_headers = {
            "authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        self.api_url = "https://api.rudderstack.com/v2/sources"

    def trigger_sync(self, source_id: str) -> None:
        """ Triggers a Rudderstack sync to run

        Args:
            source_id (str): The identifier tied to the sync to trigger
        """
        # api_url = "https://api.rudderstack.com/v2/sources"
        trigger_sync_url = self.api_url + f"/{source_id}/start"
        # get response from API
        try:
            trigger_sync_response = requests.post(trigger_sync_url, 
                                            headers=self.api_headers)
            # check if successful, if not return error message
            if trigger_sync_response.status_code  in (200, 204, 201):
                self.logger.info(f"Trigger sync for Source ID: {source_id} successful")

            elif trigger_sync_response.status_code == 409:
                self.logger.warn(f"Sync job for Source ID: {source_id} is already running")
                sys.exit(self.EXIT_CODE_SYNC_ALREADY_RUNNING)

            elif trigger_sync_response.status_code == 401:
                self.logger.error(f"Trigger sync failed for Source {source_id} due to Invalid Credentials")
                sys.exit(self.EXIT_CODE_INVALID_CREDENTIALS)

            elif trigger_sync_response.status_code == 500:
                self.logger.error(f"Trigger sync failed. Check if Source {source_id} exists")
                sys.exit(self.EXIT_CODE_SYNC_INVALID_SOURCE_ID)

            else:
                self.logger.error(f"Trigger sync failed. Error code:{trigger_sync_response.status_code}")
                sys.exit(self.EXIT_CODE_BAD_REQUEST)
        except Exception as e:
            self.logger.error(f"Source {source_id} trigger sync failed due to: {e}")
            sys.exit(self.EXIT_CODE_UNKNOWN_ERROR)

    def _get_source_data(self,source_id:str) -> dict:
        source_status_url = self.api_url + f"/{source_id}/status"
        source_status_json = {}

        ## get the api response
        try: 
            source_status_response = requests.get(source_status_url,headers = self.api_headers)
            ## check if successful, if not return error message
            if source_status_response.status_code == requests.codes.ok:
                source_status_json = source_status_response.json()
            elif source_status_response.status_code == 401:
                self.logger.error("Invalid Credentials. Please check access token")
                sys.exit(self.EXIT_CODE_INVALID_CREDENTIALS)
            elif source_status_response.status_code in [404, 500]:
                self.logger.error(
                    f"Failed to run status check. Invalid Source id: {source_id}")
                sys.exit(self.EXIT_CODE_SYNC_INVALID_SOURCE_ID)
            else:
                self.logger.error(
                    f"Source status check failed. Reason: {source_status_response.text}")
                sys.exit(self.EXIT_CODE_BAD_REQUEST)
        except Exception as e:
            self.logger.exception(f"Source {source_id} status check failed due to: {e}")
            sys.exit(self.EXIT_CODE_BAD_REQUEST)
        return source_status_json

    def determine_sync_status(self, source_data:dict, source_id: str) -> int:
        if source_data['status'] == 'finished':
            if source_data.get("error"):
                self.logger.error(f"Rudderstack reports that the most recent run for source {source_id} errored with the following message: {source_data['error']}")
                sys.exit(self.EXIT_CODE_FINAL_STATUS_ERRORED)

            else:
                self.logger.info(
                    f"Rudderstack reports that the most recent run for source {source_id} finished without errors.")
                status_code = self.EXIT_CODE_FINAL_STATUS_SUCCESS
        elif source_data['status'] == 'processing':
            self.logger.info(
                f"Rudderstack reports that the most recent run for source {source_id} is still processing.")
            status_code = self.EXIT_CODE_FINAL_STATUS_INCOMPLETE
        else:

            self.logger.warn(
                f"Sync for {source_id} is incomplete or unknown. Status {source_data['status']}")
            status_code = self.EXIT_CODE_UNKNOWN_STATUS
        return status_code


        

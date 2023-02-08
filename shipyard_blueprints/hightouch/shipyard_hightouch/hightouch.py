from templates.etl import Etl
import requests
import sys


class HightouchClient(Etl):
    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        self.api_headers = {
            'authorization': f"Bearer {self.access_token}",
            'Content-Type': 'application/json'}
        super().__init__(access_token)

    def get_sync_status(self, sync_id: str, sync_run_id: str):
        """
        Gets the sync status from the Hightouch API
        see: https://hightouch.io/docs/api-reference/#operation/ListSyncRuns
        """
        sync_api = f"https://api.hightouch.io/api/v1/syncs/{sync_id}/runs"
        params = {"runId": sync_run_id}
        try:
            sync_run_response = requests.get(sync_api,
                                             params=params,
                                             headers=self.api_headers)

            sync_status_code = sync_run_response.status_code
            # check if successful, if not return error message
            if sync_status_code == requests.codes.ok:
                sync_run_json = sync_run_response.json()

                # Handles an error where the response is successful, but a blank list
                # is returned when runID provide returns no matches.
                if len(sync_run_json['data']) == 0:
                    self.logger.warn(
                        f"Sync request Failed. Invalid Sync ID: {sync_id}")
                    sys.exit(self.EXIT_CODE_SYNC_INVALID_ID)
                else:
                    return sync_run_json['data'][0]

            elif sync_status_code == 400:  # Bad request
                self.logger.error(
                    "Sync status request failed due to Bad Request Error.")
                sys.exit(self.EXIT_CODE_BAD_REQUEST)

            elif sync_status_code == 401:  # Incorrect credentials
                self.logger.error(
                    "Incorrect credentials, check your access token")
                sys.exit(self.EXIT_CODE_INVALID_CREDENTIALS)

            elif sync_status_code == 422:  # Invalid status query
                sync_run_json = sync_run_response.json()
                self.logger.error(
                    f"Check status Validation failed: {sync_run_json['details']}")
                sys.exit(self.EXIT_CODE_BAD_REQUEST)

            else:
                self.logger.error("Unknown Error when sending request")
                sys.exit(self.EXIT_CODE_UNKNOWN_ERROR)

        except Exception as e:
            # Handle an error where a blank list is returned when the
            self.logger.exception(
                f"Failed to grab the sync status for Sync {sync_id}, Sync Run {sync_run_id} due to: {e}")
            sys.exit(self.EXIT_CODE_UNKNOWN_ERROR)

    def determine_sync_status(self, sync_run_data: dict):
        """
        Analyses sync run data to determine status and print sync run information
        Returns:
            status_code: Exit Status code detailing sync status
        """
        run_id = sync_run_data['id']
        status = sync_run_data['status']
        if status == "success":
            self.logger.info(f"Sync run {run_id} completed successfully. ")
            self.logger.info(f"Completed at: {sync_run_data['finishedAt']}")
            status_code = self.EXIT_CODE_FINAL_STATUS_COMPLETED

        elif sync_run_data['finishedAt'] is None:
            self.logger.info(
                f"Sync run {run_id} still Running. ")
            self.logger.info(
                f"Current records processed: {sync_run_data['records_processed']}")

            status_code = self.EXIT_CODE_SYNC_ALREADY_RUNNING

        elif status == "failed":
            error_info = sync_run_data['error']
            self.logger.error(f"Sync run {run_id} failed. {error_info}")
            status_code = self.EXIT_CODE_FINAL_STATUS_ERRORED

        else:
            self.logger.error(f"Unknown Sync status: {status}")
            status_code = self.EXIT_CODE_UNKNOWN_ERROR

        return status_code

    def trigger_sync(self, sync_id: str, full_resync=False):
        sync_api = f"https://api.hightouch.io/api/v1/syncs/{sync_id}/trigger"
        payload = {}

        if full_resync:
            payload['fullResync'] = 'true'
        else:
            payload['fullResync'] = 'false'

        try:
            sync_trigger_response = requests.post(sync_api,
                                                  json=payload,
                                                  headers=self.api_headers)

            sync_status_code = sync_trigger_response.status_code
            # check if successful, if not return error message
            if sync_status_code == requests.codes.ok:
                self.logger.info(
                    f"Sync trigger for {sync_id} successful. Kicked off {'full resync - ' if full_resync else ''}sync run {sync_trigger_response.json()['id']}")
                return sync_trigger_response.json()

            elif sync_status_code == 400:  # Bad request
                self.logger.error(
                    "Sync request failed due to Bad Request Error.")
                sys.exit(self.EXIT_CODE_BAD_REQUEST)

            elif sync_status_code == 401:  # Incorrect credentials
                self.logger.error(
                    "Incorrect credentials, check your access token")
                sys.exit(self.EXIT_CODE_INVALID_CREDENTIALS)

            elif sync_status_code == 404:  # invalid sync id
                self.logger.error(
                    f"Sync request Failed. Invalid Sync ID: {sync_id}")
                sys.exit(self.EXIT_CODE_SYNC_INVALID_SOURCE_ID)

            elif sync_status_code == 422:  # Invalid content
                sync_trigger_json = sync_trigger_response.json()
                self.logger.error(
                    f"Validation failed: {sync_trigger_json['details']}")
                sys.exit(self.EXIT_CODE_BAD_REQUEST)

            else:
                self.logger.error("Unknown Error when sending request")
                sys.exit(self.EXIT_CODE_UNKNOWN_ERROR)

        except Exception as e:
            self.logger.error(f"Sync trigger request failed due to: {e}")
            sys.exit(self.EXIT_CODE_UNKNOWN_ERROR)
import sys
import argparse
import requests
from templates.etl import Etl
import shipyard_utils as shipyard


class CensusClient(Etl):
    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        self.api_headers = {'Content-Type': 'application/json'}
        super().__init__(access_token)

    def get_sync_status(self, sync_run_id):
        """
        This function returns information on a specific sync run.
        see https://docs.getcensus.com/basics/api/sync-runs#get-sync_runs-id
        """
        sync_post_api = f"https://bearer:{self.access_token}@app.getcensus.com/api/v1/sync_runs/{sync_run_id}"
        check_sync_response = {}

        try:
            check_sync_response = requests.get(sync_post_api)
            # check if successful, if not return an error message
            if check_sync_response.status_code == requests.codes.ok:
                sync_run_json = check_sync_response.json()
            else:
                self.logger.error(
                    f"Sync check failed. Reason: {check_sync_response.text}")
                if "Access denied" in check_sync_response.text:
                    self.logger.info(
                        'Check to make sure that your access token doesn\'t have any typos and includes "secret-token:"')
                    sys.exit(self.EXIT_CODE_INVALID_CREDENTIALS)
                sys.exit(self.EXIT_CODE_BAD_REQUEST)
        except Exception as e:
            self.logger.exception(f"Check Sync Run {sync_run_id} failed")
            sys.exit(self.EXIT_CODE_BAD_REQUEST)

        if sync_run_json['status'] == 'success':
            self.logger.info("Successfully managed to check sync")
            return sync_run_json['data']

        else:
            self.logger.error(
                f"Sync run {sync_run_id} was unsuccessful. Reason: {sync_run_json['data']['error_message']}")
            sys.exit(self.EXIT_CODE_SYNC_CHECK_ERROR)

    def determine_sync_status(self, sync_run_data: dict) -> int:
        """
        Analyses sync run data to determine status and print sync run information
        Returns:
            status_code: Exit Status code detailing sync status
        """
        status = sync_run_data['status']
        sync_id = sync_run_data['sync_id']
        sync_run_id = sync_run_data['id']
        status_code = self.EXIT_CODE_FINAL_STATUS_COMPLETED
        if status == 'completed':
            self.logger.info(
                f"Sync run {sync_run_id} for {sync_id} completed successfully, Completed at: {sync_run_data['completed_at']}")
        elif status == 'working':
            self.logger.info(
                f"Sync run {sync_run_id} for {sync_id} still running.")
            self.logger.info(
                f"Curent records processed: {sync_run_data['records_processed']}")
            status_code = self.EXIT_CODE_SYNC_ALREADY_RUNNING

        elif status == 'failed':
            error_code = sync_run_data['error_code']
            error_message = sync_run_data['error_message']
            self.logger.error(
                f"Sync run:{sync_run_id} for {sync_id} failed. {error_code} {error_message}")
            status_code = self.EXIT_CODE_FINAL_STATUS_ERRORED

        else:
            self.logger.error(
                f"An unknown error has occurred with Run:{sync_run_id} with Sync Id {sync_id}")
            self.logger.info(f"Unknown Sync status: {status}")
            status_code = self.EXIT_CODE_UNKNOWN_ERROR

        return status_code

    def trigger_sync(self, sync_id: str) -> dict:
        """
        Executes a Census Sync
        """

        sync_post_api = f"https://bearer:{self.access_token}@app.getcensus.com/api/v1/syncs/{sync_id}/trigger"
        sync_trigger_json = {}
        try:
            sync_trigger_response = requests.post(
                sync_post_api, headers=self.api_headers)

            if sync_trigger_response.status_code == requests.codes.ok:
                sync_trigger_json = sync_trigger_response.json()

            elif sync_trigger_response.status_code == 404:
                self.logger.error(
                    f"Sync request failed. Check if sync ID {sync_id} is valid?")
                sys.exit(self.EXIT_CODE_BAD_REQUEST)
            else:
                self.logger.error(
                    f"Sync request failed. Reason: {sync_trigger_response.text}")
                if "Access denied" in sync_trigger_response.text:
                    self.logger.error(
                        'Check to make sure that your access token does not have any typos and includes "secret-token:"')
                    sys.exit(self.EXIT_CODE_INVALID_CREDENTIALS)
                sys.exit(self.EXIT_CODE_BAD_REQUEST)
        except Exception as e:
            self.logger.error(f"Sync trigger request failed due to: {e}")
            sys.exit(self.EXIT_CODE_BAD_REQUEST)

        if sync_trigger_json['status'] == 'success':
            self.logger.info("Successfully triggered sync")
            return sync_trigger_response.json()

        if sync_trigger_json['status'] == 'error':
            self.logger.error(
                f"Encountered an error - Census says: {sync_trigger_json['message']}")
            sys.exit(self.EXIT_CODE_SYNC_REFRESH_ERROR)
        else:
            self.logger.error(
                f"An unknown error has occurred - API response: {sync_trigger_json}")
            sys.exit(self.EXIT_CODE_UNKNOWN_ERROR)

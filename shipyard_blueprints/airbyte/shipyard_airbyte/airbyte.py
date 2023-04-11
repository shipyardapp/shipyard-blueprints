from templates.etl import Etl
import requests
import os
import sys


class AirbyteClient(Etl):
    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        super().__init__(access_token)

    def trigger_sync(self, connection_id: str, check_status: bool = True) -> dict:
        """
        Args:
            connection_id: The id in which to trigger the airbyte sync
            check_status: flag to determine whether the function will check the status of the sync before exiting

        Returns: The response from the post request 

        """
        url = 'https://api.airbyte.com/v1/jobs'
        payload = {
            'jobType': 'sync',
            'connectionId': connection_id
        }
        headers = {
            'accept': 'application/json',
            'content-type': 'application/json',
            'authorization': f'Bearer {self.access_token}',
            'User-Agent': 'Shipyard User 1.0'
        }
        try:
            response = requests.post(url, json=payload, headers=headers)
            if response.status_code == 200:
                self.logger.info("Airbyte sync successfully triggered")
        except Exception as e:
            self.logger.error(
                f"Error occurred when attempting to trigger sync. Check to see that the connection id and api token are valid ")
            sys.exit(self.EXIT_CODE_BAD_REQUEST)

        if not check_status:
            return response.json()
        else:
            job_response = self.get_sync_status(response.json()['jobId'])
            self.determine_sync_status(job_response['status'])

    def _determine_status_helper(self, sync_response: dict):
        pass

    def get_sync_status(self, job_id: str) -> dict:
        url = 'https://api.airbyte.com/v1/jobs'
        job_url = f"{url}/{job_id}"
        headers = {
            'accept': 'application/json',
            'authorization': f'Bearer {self.access_token}',
            'User-Agent': 'Shipyard User 1.0'
        }
        job_response = requests.get(job_url, headers=headers).json()
        job_status = job_response['status']
        return job_response

    def determine_sync_status(self, job_response: dict):
        # job_response = job_status.json()
        job_status = job_response['status']
        if job_status == 'pending':
            self.logger.info(
                f"Status is pending, Airbyte job has yet to run. Sync started at {job_response['startTime']}")
            return self.EXIT_CODE_FINAL_STATUS_INCOMPLETE
        elif job_status == 'running':
            self.logger.info(
                f"Airbyte job is currently running. Sync started at {job_response['startTime']}")
            return self.EXIT_CODE_SYNC_ALREADY_RUNNING
        elif job_status == 'incomplete':
            self.logger.warn('Airbyte job status is imcomplete')
            return self.EXIT_CODE_FINAL_STATUS_INCOMPLETE
        elif job_status == 'failed':
            self.logger.warn('Airbyte job failed')
            return self.EXIT_CODE_FINAL_STATUS_ERRORED
        elif job_status == 'succeeded':
            self.logger.info(
                f"Airbyte job succeeded. Completed at {job_response['lastUpdatedAt']} with {job_response['rowsSynced']} rows synced")
            return self.EXIT_CODE_FINAL_STATUS_COMPLETED
        elif job_status == 'cancelled':
            self.logger.info('Airbyte job was cancelled')
            return self.EXIT_CODE_SYNC_CANCELLED

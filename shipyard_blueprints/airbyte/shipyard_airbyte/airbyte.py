from templates.etl import Etl
import requests
import os


class AirbyteClient(Etl):
    def __init__(self, access_token: str, connection_id:str = None) -> None:
        self.access_token = access_token
        self.connection_id = connection_id
        self.headers = {
            'accept': 'application/json',
            'content-type': 'application/json',
            'authorization': f'Bearer {self.access_token}',
            'User-Agent': 'Shipyard User 1.0'
        }
        super().__init__(access_token)

    def trigger_sync(self, check_status:bool = True):
        url = 'https://api.airbyte.com/v1/jobs'
        payload = {
            'jobType': 'sync',
            'connectionId':  self.connection_id
        }
        response = requests.post(url, json = payload, headers = self.headers)
        if not check_status:
            return response
        else:
            job_id = response['jobId']
            job_url = f"{url}/{job_id}"
            job_headers = {
                'accept' : 'application/json',
                'authorization': f'Bearer {self.access_token}'
            }
            job_response  = requests.get(job_url, headers = job_headers)

    def determine_sync_status(self, job_id:str = None):
            url = 'https://api.airbyte.com/v1/jobs'
            job_url = f"{url}/{job_id}"
            job_headers = {
                'accept' : 'application/json',
                'authorization': f'Bearer {self.access_token}',
                'User-Agent': 'Shipyard User 1.0'
            }
            job_response  = requests.get(job_url, headers = job_headers)
            job_status = job_response['status']
            if job_status == 'pending':
                 self.logger.info("Status is pending, Airbyte job has yet to run")
            elif job_status == 'running':
                 self.logger.info("Airbyte job is currently running")
            elif job_status == 'incomplete':
                 self.logger.warn('Airbyte job status is imcomplete')
            elif job_status == 'failed':
                 self.logger.warn('Airbyte job failed')
            elif job_status == 'succeeded': 
                 self.logger.info("Airbyte job succeeded")
            elif job_status == 'cancelled':
                 self.logger.info('Airbyte job was cancelled')
    
            
            
            


            

        

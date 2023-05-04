import requests
from shipyard_templates import Etl

class CoalesceClient(Etl):
    def __init__(self,access_token: str):
        self.base_url = "https://app.coalescesoftware.io/scheduler"
        super().__init__(access_token)

    def trigger_sync(self, environment_id:str, job_id:str, snowflake_username:str, snowflake_password:str, snowflake_warehouse:str = None, 
                     snowflake_role:str = 'ADMIN' ,parallelism:int = 16, include_nodes_selector:str = None, exclude_nodes_selector = None) -> dict[any,any]:
        """

        Args:
            exclude_nodes_selector (): 
            environment_id: 
            job_id: 
            snowflake_username: 
            snowflake_password: 
            snowflake_warehouse: 
            snowflake_role: 
            parallelism: 
            include_nodes_selector: 

        Returns:
            
        """
        # reference is available here: https://docs.coalesce.io/reference/startrun
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "Authorization": f"Bearer {self.access_token}"}
        url = f'{self.base_url}/startRun'
        # populate the inner payload for the userCredentials object
        credentials = {
                "snowflakeUsername": snowflake_username,
                "snowflakePassword": snowflake_password,
                "snowflakeRole": snowflake_role,
                "snowflakeAuthType": "Basic"
                }
        if snowflake_warehouse is not None:
            credentials['snowflakeWarehouse'] = snowflake_warehouse
        # user_credentials = {'userCredentials' : credentials} # userCredentials object that is expected in the payload
        # populate the inner payload for the runDetails object
        # environmentId is required and parallelism has a default value
        details = {"environment_id": environment_id,
                   'parallelism': parallelism}
        # jobId, and include/exclude_nodes_selector are optional
        if job_id is not None:
            details['jobId'] = job_id
        if include_nodes_selector is not None:
            details['includeNodesSelector'] = include_nodes_selector
        if exclude_nodes_selector is not None:
            details['excludeNodesSelector'] = exclude_nodes_selector

        # run_details = {'runDetails' : details}

        payload = {'runDetails' : details, 
                   'userCredentials' : credentials}

        response = requests.post(url = url, json = payload, headers = headers)

        if response.status_code == 200:
            self.logger.info("Successfully triggered job")

        elif response.status_code == 400:
            self.logger.error("Error occurred when triggering job")

        elif response.status_code == 401:
            self.logger.error("Error occurred when attempting to authenticate, please ensure that the token provided is valid")

        return response.json()


    def get_run_status(self, run_counter:int) -> dict[any,any]: 
        url = f"{self.base_url}/runStatus?runCounter={run_counter}"
        headers = {
                "accept": "application/json",
                "Authorization": f"Bearer {self.access_token}"
        }
        response = requests.get(url = url, headers = headers)
        return response.json()


    def determine_sync_status(self, run_counter:int) -> int:
        response = self.get_run_status(run_counter)
        if response.status_code == 200:
            status = response.get('runStatus')
            # go through the statuses and return the appropriate exit code
            # documentation does not provide options for status aside from completed
            if status == 'completed':
                self.logger.info(f"Status: completed")
                return self.EXIT_CODE_FINAL_STATUS_COMPLETED
            elif status == 'pending':
                self.logger.info("Status: pending")
                return self.EXIT_CODE_FINAL_STATUS_PENDING
            elif status == 'running':
                self.logger.info("Status: running")
                return self.EXIT_CODE_SYNC_ALREADY_RUNNING
            elif status == 'timeout':
                self.logger.info("Status: timeout")
                return self.EXIT_CODE_FINAL_STATUS_INCOMPLETE
            elif status == 'canceled':
                self.logger.info("Status: canceled")
                    return self.EXIT_CODE_FINAL_STATUS_CANCELLED
            else:
                self.logger.info(f"Status: {status}")
                return self.EXIT_CODE_UNKNOWN_STATUS

        elif response.status_code == 400:
            self.logger.error(f"There was an error when attempting to fetch the status of the job. The message returned from the API is {response['error']['errorString']}")
            return self.EXIT_CODE_BAD_REQUEST
        
        elif response.status_code == 401:
            self.logger.error("Error occurred when attempting to authenticate, please ensure that the token provided is valid")
            return self.EXIT_CODE_INVALID_CREDENTIALS









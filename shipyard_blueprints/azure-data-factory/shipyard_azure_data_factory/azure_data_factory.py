import time

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from shipyard_templates import Etl, ShipyardLogger, ExitCodeException

logger = ShipyardLogger.get_logger()


class AzureDataFactoryClient(Etl):
    def __init__(self, client_id, client_secret, tenant_id, subscription_id):
        """
        Initializes the AzureDataFactory instance with provided Azure credentials.

        :param client_id: Azure client ID
        :param client_secret: Azure client secret
        :param tenant_id: Azure tenant ID
        :param subscription_id: Azure subscription ID
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.subscription_id = subscription_id
        self.adf_client = None

    def setup_client(self):
        """
        Initializes the DataFactoryManagementClient.

        :return: DataFactoryManagementClient instance
        """
        try:
            credentials = ClientSecretCredential(client_id=self.client_id,
                                                 client_secret=self.client_secret,
                                                 tenant_id=self.tenant_id)
            self.adf_client = DataFactoryManagementClient(credentials, self.subscription_id)

            logger.info("Successfully initialized DataFactoryManagementClient.")
        except Exception as e:
            raise ExitCodeException(
                f"Failed to initialize DataFactoryManagementClient: {e}",
                Etl.EXIT_CODE_INVALID_CREDENTIALS,
            ) from e

    def connect(self):
        """
        Establishes connection to Azure services and initializes DataFactoryManagementClient.

        :return: 1 if connection is successful, 0 otherwise
        """
        try:
            self.setup_client()
            logger.info("Successfully connected to Azure Data Factory.")
            return 1
        except Exception as e:
            logger.authtest(f"Error connecting to Azure Data Factory: {e}")
            return 0

    def trigger_sync(self, resource_group: str, data_factory_name: str, pipeline_name: str,
                     wait_for_completion: bool = True, wait_time: int = 1):
        """
        Triggers a pipeline run in Azure Data Factory.

        :param resource_group: Azure resource group name
        :param data_factory_name: Data factory name
        :param pipeline_name: Pipeline name to be triggered
        :param wait_for_completion: If True, waits for the pipeline run to complete
        :param wait_time: Time interval to wait before checking pipeline run status
        """
        if not self.adf_client:
            self.setup_client()

            run_response = self.adf_client.pipelines.create_run(resource_group, data_factory_name, pipeline_name,
                                                                parameters={})
            logger.info(f"Triggered pipeline run with ID: {run_response.run_id}")
            if wait_for_completion:
                run_status = self.determine_sync_status(resource_group, data_factory_name, run_response.run_id)
                while run_status not in {"Succeeded", "Failed", "Cancelled", "Canceling"}:
                    logger.info(f"Waiting {wait_time} minute(s) before checking pipeline run status...")
                    time.sleep(wait_time * 60)
                    run_status = self.determine_sync_status(resource_group, data_factory_name, run_response.run_id)
        else:
            logger.error("Failed to connect to Azure Data Factory. Pipeline run not triggered.")

    def determine_sync_status(self, resource_group, data_factory_name, run_id):
        """
        Checks the status of a pipeline run and logs the activity details.

        :param resource_group: Azure resource group name
        :param data_factory_name: Data factory name
        :param run_id: ID of the pipeline run to check status
        """
        pipeline_run = self.adf_client.pipeline_runs.get(resource_group, data_factory_name, run_id)
        logger.info(f"Pipeline run status: {pipeline_run.status}")

        return pipeline_run.status

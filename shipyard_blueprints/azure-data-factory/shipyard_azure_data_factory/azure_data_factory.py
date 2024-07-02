from shipyard_templates import Etl, ShipyardLogger
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import RunFilterParameters
from datetime import datetime, timedelta

logger = ShipyardLogger.get_logger()

class AzureDataFactory(Etl):
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
        self.credentials = None
        self.adf_client = None

    def connect(self):
        """
        Establishes connection to Azure services and initializes DataFactoryManagementClient.

        :return: 1 if connection is successful, 0 otherwise
        """
        try:
            self.credentials = ClientSecretCredential(client_id=self.client_id,
                                                      client_secret=self.client_secret, tenant_id=self.tenant_id)
            ResourceManagementClient(self.credentials, self.subscription_id)
            self.adf_client = DataFactoryManagementClient(self.credentials, self.subscription_id)
            logger.info("Successfully connected to Azure Data Factory.")
            return 1
        except Exception as e:
            logger.error(f"Error connecting to Azure Data Factory: {e}")
            return 0

    def trigger_sync(self, resource_group, data_factory_name, pipeline_name, wait_for_completion=True):
        """
        Triggers a pipeline run in Azure Data Factory.

        :param resource_group: Azure resource group name
        :param data_factory_name: Data factory name
        :param pipeline_name: Pipeline name to be triggered
        :param wait_for_completion: If True, waits for the pipeline run to complete
        """
        if self.connect():
            run_response = self.adf_client.pipelines.create_run(resource_group, data_factory_name, pipeline_name, parameters={})
            logger.info(f"Triggered pipeline run with ID: {run_response.run_id}")
            if wait_for_completion:
                self.determine_sync_status(resource_group, data_factory_name, run_response.run_id)
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

        filter_params = RunFilterParameters(
            last_updated_after=datetime.now() - timedelta(1),
            last_updated_before=datetime.now() + timedelta(1))
        query_response = self.adf_client.activity_runs.query_by_pipeline_run(
            resource_group, data_factory_name, run_id, filter_params)

        self.print_activity_run_details(query_response.value)

    def print_activity_run_details(self, activity_runs):
        """
        Logs details of activity runs.

        :param activity_runs: List of activity run details
        """
        for activity_run in activity_runs:
            logger.info(f"Activity run details: {activity_run}")

# https://learn.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-python
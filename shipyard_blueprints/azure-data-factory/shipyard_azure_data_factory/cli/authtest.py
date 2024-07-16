import os
import sys
from shipyard_azure_data_factory import AzureDataFactoryClient


def main():
    sys.exit(AzureDataFactoryClient(client_id=os.getenv('AZURE_DATAFACTORY_CLIENT_ID'),
                                    client_secret=os.getenv('AZURE_DATAFACTORY_CLIENT_SECRET'),
                                    tenant_id=os.getenv('AZURE_DATAFACTORY_TENANT_ID'),
                                    subscription_id=os.getenv('AZURE_DATAFACTORY_SUBSCRIPTION_ID')
                                    ).connect()
             )


if __name__ == "__main__":
    main()
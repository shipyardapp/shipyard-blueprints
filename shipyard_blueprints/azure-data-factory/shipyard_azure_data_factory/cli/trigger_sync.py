import argparse
import sys

from shipyard_templates import Etl, ShipyardLogger, ExitCodeException

from shipyard_azure_data_factory import AzureDataFactoryClient

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser(description="Azure Data Factory Trigger Sync")
    parser.add_argument("--client-id", required=True, help="Azure client ID")
    parser.add_argument("--client-secret", required=True, help="Azure client secret")
    parser.add_argument("--tenant-id", required=True, help="Azure tenant ID")
    parser.add_argument("--subscription-id", required=True, help="Azure subscription ID")
    parser.add_argument("--resource-group", required=True, help="Azure resource group name")
    parser.add_argument("--data-factory-name", required=True, help="Data factory name")
    parser.add_argument("--pipeline-name", required=True, help="Pipeline name")
    parser.add_argument("--wait-for-completion", default=True, help="Wait for pipeline run completion")
    parser.add_argument("--wait-time", default=1, help="Wait time in minutes")
    return parser.parse_args()


def main():
    try:
        args = get_args()
        adf_client = AzureDataFactoryClient(args.client_id, args.client_secret, args.tenant_id, args.subscription_id)
        adf_client.trigger_sync(args.resource_group, args.data_factory_name, args.pipeline_name,
                                args.wait_for_completion, args.wait_time)
    except ExitCodeException as e:
        logger.error(e.message)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"Error triggering pipeline run: {e}")
        sys.exit(Etl.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()

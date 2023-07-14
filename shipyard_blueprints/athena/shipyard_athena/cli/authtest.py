import argparse
import os
import boto3
from shipyard_athena import AthenaClient


def get_args():
    args = {}
    args['aws_access_key'] = os.getenv('AWS_ACCESS_KEY_ID')
    args['aws_secret_key'] = os.getenv('AWS_SECRET_ACCESS_KEY')
    args['region'] = os.getenv('AWS_DEFAULT_REGION')
    return args

def main():
    athena = AthenaClient(os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('AWS_SECRET_ACCESS_KEY'))

    try:
        client = boto3.client('sts', aws_access_key_id= os.getenv('AWS_ACCESS_KEY_ID'), aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY'))
        response = client.get_caller_identity()
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            athena.logger.info("Successfully connected to AWS Athena")
            return 0
    except Exception as e:
        athena.logger.error(f"Could not connect to the AWS Athena")
        return 1
    
    else:
        athena.logger.error(f"Could not connect to the AWS Athena")
        return 1

if __name__ == '__main__':
    main()

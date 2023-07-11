import os
import boto3
from shipyard_blueprints import S3Client

def main():
    s3 = S3Client(aws_access_key=os.getenv('AWS_ACCESS_KEY_ID'), aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY')) # only necessary for logging
    try:
        client = boto3.client('sts', aws_access_key_id= os.getenv('AWS_ACCESS_KEY_ID'), aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY'))
        response = client.get_caller_identity()
        if response['ReponseMetadata']['HTTPStatusCode'] == 200:
            return 0
    except Exception as e:
        s3.logger.error(f"Could not connect to the AWS S3")
        return 1
    
    else:
        s3.logger.error(f"Could not connect to the AWS S3")
        return 1


if __name__ == '__main__':
    main()

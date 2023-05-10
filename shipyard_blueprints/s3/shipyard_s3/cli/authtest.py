from shipyard_blueprints import S3Client
import argparse
import os


def get_args():
    args = {}
    args['aws_access_key'] = os.environ.get('AWS_ACCESS_KEY_ID')
    args['aws_secret_key'] = os.environ.get('AWS_SECRET_ACCESS_KEY')
    try:
        args['region'] = os.environ.get('AWS_REGION')
    except Exception as e:
        args['aws_default_region'] = 'us-east-2'
    return args


def main():
    args = get_args()
    aws_access_key = args['aws_access_key']
    aws_secret = args['aws_secret_key']
    aws_region = args['region']

    s3 = S3Client(aws_access_key=aws_access_key,
                  aws_secret_access_key=aws_secret, region=aws_region)
    try:
        s3.connect()
        return 0
    except Exception as e:
        s3.logger.error(f"Could not connect to the AWS S3")
        return 1


if __name__ == '__main__':
    main()

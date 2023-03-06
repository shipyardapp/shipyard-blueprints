from shipyard_blueprints import S3Client
import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--aws-access-key-id",
                        dest='aws_access_key', required=True)
    parser.add_argument("--aws-secret-access-key",
                        dest='aws_secret_key', required=True)
    parser.add_argument('--region', dest='region',
                        required=False, default='us-east-2')
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    aws_access_key = args.aws_access_key
    aws_secret = args.aws_secret_key
    aws_region = args.region

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

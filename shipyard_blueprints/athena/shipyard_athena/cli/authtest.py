import argparse
from shipyard_blueprints import AthenaClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--aws-access-key",
                        dest='aws_access_key', required=True)
    parser.add_argument("--aws-secret-key",
                        dest="aws_secret_key", required=True)
    parser.add_argument('--region', dest='region',
                        required=True, default='us-east-2')
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    access_key = args.aws_access_key
    secret_key = args.aws_secret_key
    region = args.region
    athena = AthenaClient(access_key, secret_key, region)

    con = athena.connect()
    if con == 1:
        return 1
    else:
        return 0


if __name__ == '__main__':
    main()

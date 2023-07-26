import os
import boto3
from shipyard_athena import AthenaClient
from dotenv import load_dotenv, find_dotenv

def conn_helper(client: AthenaClient) -> int:
    try:
        response = client.get_caller_identity()
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return 0
    except Exception as e:
        print("Could not connect to S3")
        return 1
    else:
        return 1

def test_good_connection():
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret = os.environ.get('AWS_SECRET_ACCESS_KEY')
    
    client = boto3.client('sts', aws_access_key_id= aws_access_key, aws_secret_access_key=aws_secret)
    assert conn_helper(client) == 0

def test_bad_access_key():
    aws_access_key = 'bad_access_key'
    aws_secret = os.environ.get('AWS_SECRET_ACCESS_KEY')
    
    client = boto3.client('sts', aws_access_key_id= aws_access_key, aws_secret_access_key=aws_secret)
    assert conn_helper(client) == 1

def test_bad_secret_key():
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret = 'bad_secret_key'
    
    client = boto3.client('sts', aws_access_key_id= aws_access_key, aws_secret_access_key=aws_secret)
    assert conn_helper(client) == 1

def test_bad_access_and_secret():
    aws_access_key = 'bad_access_key'
    aws_secret = 'bad_secret_key'
    
    client = boto3.client('sts', aws_access_key_id= aws_access_key, aws_secret_access_key=aws_secret)
    assert conn_helper(client) == 1

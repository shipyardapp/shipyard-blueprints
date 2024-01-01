import argparse
import requests
import os
import sys
import hashlib


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--method', dest='method', required=True,
                        choices={'GET', 'POST', 'PUT', 'PATCH'})
    parser.add_argument('--url', dest='url', required=True)
    parser.add_argument('--authorization-header', dest='authorization_header',
                        required=False, default=None)
    parser.add_argument(
        '--content-type',
        dest='content_type',
        required=False,
        default=None)
    parser.add_argument('--message', dest='message', required=False)
    parser.add_argument(
        '--print-response',
        dest='print_response',
        default='FALSE',
        choices={
            'TRUE',
            'FALSE'},
        required=False)
    parser.add_argument(
        '--destination-file-name',
        dest='destination_file_name',
        default='response.txt',
        required=False)
    parser.add_argument(
        '--destination-folder-name',
        dest='destination_folder_name',
        default='',
        required=False)
    args = parser.parse_args()
    return args


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')
    combined_name = os.path.normpath(combined_name)

    return combined_name


def clean_folder_name(folder_name):
    """
    Cleans folders name by removing duplicate '/' as well as leading and trailing '/' characters.
    """
    folder_name = folder_name.strip('/')
    if folder_name != '':
        folder_name = os.path.normpath(folder_name)
    return folder_name


def convert_to_boolean(string):
    """
    Shipyard can't support passing Booleans to code, so we have to convert
    string values to their boolean values.
    """
    if string in ['True', 'true', 'TRUE']:
        value = True
    else:
        value = False
    return value


def execute_request(method, url, headers=None, message=None, params=None):
    try:
        if method == 'GET':
            req = requests.get(url, headers=headers, params=params)
        elif method == 'POST':
            req = requests.post(
                url,
                headers=headers,
                data=message,
                params=params)
        elif method == 'PUT':
            req = requests.put(
                url,
                headers=headers,
                data=message,
                params=params)
        elif method == 'PATCH':
            req = requests.patch(
                url, headers=headers, data=message, params=params)
    except requests.exceptions.HTTPError as eh:
        print(
            'URL returned an HTTP Error.\n',
            eh)
        sys.exit(1)
    except requests.exceptions.ConnectionError as ec:
        print(
            'Could not connect to the URL. Check to make sure that it was typed correctly.\n',
            ec)
        sys.exit(2)
    except requests.exceptions.Timeout as et:
        print('Timed out while connecting to the URL.\n', et)
        sys.exit(3)
    except requests.exceptions.RequestException as e:
        print('Unexpected error occured. Please try again.\n', e)
        sys.exit(4)
    return req


def add_to_headers(headers, key, value):
    headers[key] = value
    return headers


def create_folder_if_dne(destination_folder_name):
    if not os.path.exists(destination_folder_name) and (
            destination_folder_name != ''):
        os.makedirs(destination_folder_name)


def write_response_to_file(req, destination_name):
    with open(destination_name, 'w') as response_output:
        response_output.write(req.text)
    return


def print_response_to_output(req):
    print(f'\n\n Response body: {req.content}')


def hash_text(text_var):
    hashed_text = hashlib.sha256(text_var.encode('ascii')).hexdigest()
    return hashed_text


def main():
    args = get_args()
    method = args.method
    url = args.url
    url_hash = hash_text(url)
    authorization_header = args.authorization_header
    content_type = args.content_type
    message = args.message
    print_response = convert_to_boolean(args.print_response)

    artifact_directory_default = f'{os.environ.get("USER")}-artifacts'
    base_folder_name = clean_folder_name(
        f'{os.environ.get("SHIPYARD_ARTIFACTS_DIRECTORY",artifact_directory_default)}/httprequest-blueprints/responses')
    artifact_directory_location = combine_folder_and_file_name(
        base_folder_name, f'{method.lower()}_{url_hash}.txt')
    create_folder_if_dne(base_folder_name)

    destination_file_name = args.destination_file_name
    destination_folder_name = clean_folder_name(args.destination_folder_name)
    destination_name = combine_folder_and_file_name(
        destination_folder_name, destination_file_name)
    headers = {}

    create_folder_if_dne(destination_folder_name)
    if content_type:
        headers = add_to_headers(headers, 'Content-Type', content_type)
    if authorization_header:
        headers = add_to_headers(
            headers,
            'Authorization',
            authorization_header)
    req = execute_request(method, url, headers, message)
    write_response_to_file(req, destination_name)

    print(
        f'Successfully sent request {url} and stored response to {destination_name}.')

    write_response_to_file(req, artifact_directory_location)
    print(f'Artifact stored at {artifact_directory_location}')

    if print_response:
        print_response_to_output(req)


if __name__ == '__main__':
    main()

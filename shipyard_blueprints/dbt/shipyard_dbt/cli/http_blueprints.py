import sys
import requests


def execute_request(method, url, headers=None, message=None, params=None):
    try:
        if method == "GET":
            req = requests.get(url, headers=headers, params=params)
        elif method == "POST":
            req = requests.post(url, headers=headers, data=message, params=params)
        elif method == "PUT":
            req = requests.put(url, headers=headers, data=message, params=params)
        elif method == "PATCH":
            req = requests.patch(url, headers=headers, data=message, params=params)
    except requests.exceptions.HTTPError as eh:
        print("URL returned an HTTP Error.\n", eh)
        sys.exit(1)
    except requests.exceptions.ConnectionError as ec:
        print(
            "Could not connect to the URL. Check to make sure that it was typed correctly.\n",
            ec,
        )
        sys.exit(2)
    except requests.exceptions.Timeout as et:
        print("Timed out while connecting to the URL.\n", et)
        sys.exit(3)
    except requests.exceptions.RequestException as e:
        print("Unexpected error occured. Please try again.\n", e)
        sys.exit(4)
    return req


def download_file(url, destination_name, headers=None, params=None):
    print(f"Currently downloading the file from {url}...")
    try:
        with requests.get(url, headers=headers, stream=True, params=params) as r:
            r.raise_for_status()
            with open(destination_name, "wb") as f:
                for chunk in r.iter_content(chunk_size=(16 * 1024 * 1024)):
                    f.write(chunk)

        print(f"Successfully downloaded {url} to {destination_name}.")
    except requests.exceptions.HTTPError as eh:
        print("URL returned an HTTP Error.\n", eh)
        sys.exit(1)
    except requests.exceptions.ConnectionError as ec:
        print(
            "Could not connect to the URL. Check to make sure that it was typed correctly.\n",
            ec,
        )
        sys.exit(2)
    except requests.exceptions.Timeout as et:
        print("Timed out while connecting to the URL.\n", et)
        sys.exit(3)
    except requests.exceptions.RequestException as e:
        print("Unexpected error occured. Please try again.\n", e)
        sys.exit(4)
    return

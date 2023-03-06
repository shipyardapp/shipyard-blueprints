from shipyard_blueprints import DropboxClient
import argparse

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-key", dest = "access_key", required = True)
    args = parser.parse_args()
    return args

def main():
    args = get_args()
    key = args.access_key
    dropbox = DropboxClient(key)
    try:
        dropbox.connect()
        dropbox.logger.info("Successfully connected to Dropbox")
        return 0
    except Exception as e:
        dropbox.logger.error("Could not connect to Dropbox with the access key provided")
        return 1

if __name__ == "__main__":
    main()
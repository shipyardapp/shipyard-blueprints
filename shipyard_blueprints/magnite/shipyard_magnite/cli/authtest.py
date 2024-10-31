import os
from shipyard_magnite import MagniteClient

# NOTE: REMOVE AFTER TESTING
from dotenv import load_dotenv, find_dotenv


def main():
    load_dotenv(find_dotenv())
    user = os.getenv("MAGNITE_USERNAME")
    password = os.getenv("MAGNITE_PASSWORD")
    client = MagniteClient(user, password)
    client.connect()


if __name__ == "__main__":
    main()

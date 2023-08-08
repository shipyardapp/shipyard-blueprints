import os
import sys
import requests


def main():
    try:
        response = requests.post(os.getenv("MICROSOFT_TEAMS_WEBHOOK_URL"), json={})
    except Exception as e:
        print(f"Could not connect to Microsoft Teams due to {e}")
        sys.exit(1)
    else:
        if response.text == "Invalid webhook URL":
            sys.exit(1)
        elif response.text == "Summary or Text is required.":
            sys.exit(0)
        else:
            print(f"Unexpected error message: {response.text}")
            sys.exit(1)


if __name__ == "__main__":
    main()

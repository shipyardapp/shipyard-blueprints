import os
from shipyard_thoughtspot import ThoughtSpotClient


def get_args():
    args = {}
    args["token"] = os.getenv("THOUGHTSPOT_TOKEN")
    return args


def main():
    args = get_args()
    token = args["token"]
    tc = ThoughtSpotClient(token=token)
    try:
        resp = tc.connect()
        if resp.status_code == 200:
            return 0
        else:
            return 1
    except Exception as e:
        return 1


if __name__ == "__main__":
    main()

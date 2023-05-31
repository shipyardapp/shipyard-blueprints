import argparse
import sys

from shipyard_shortcut import ShortcutClient
from shipyard_shortcut.error_handler import handle_error


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--story-id", dest="story_id", required=True)
    parser.add_argument("--comment", dest="comment", required=True)
    return parser.parse_args()


def main():
    args = get_args()
    shortcut = ShortcutClient(access_token=args.access_token)

    try:
        shortcut.add_comment(story_id=args.story_id, comment=args.comment)
    except Exception as error:
        shortcut.logger.error(error)
        handle_error(shortcut, error)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()

import argparse
import sys
from shipyard_clickup import ClickupClient
from shipyard_templates import ExitCodeError


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', required=True, help='Access token for ClickUp API')
    parser.add_argument('--task-id', required=True, help='ID of the ClickUp task')
    parser.add_argument('--comment', required=True, help='Comment to add to the card')
    parser.add_argument('--notify_all', help='If you wish to notify all assignees, set this to True')
    return parser.parse_args()


def main():
    args = get_args()
    clickup = ClickupClient(access_token=args.access_token)
    notify_all = args.notify_all.upper() == 'TRUE' if args.notify_all else False

    try:
        clickup.add_comment(task_id=args.task_id,
                            comment=args.comment,
                            notify_all=notify_all)
    except ExitCodeError as error:
        clickup.logger.error(error.message)
        sys.exit(error.exit_code)
    except Exception as error:
        clickup.logger.error(error)
        sys.exit(ClickupClient.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == '__main__':
    main()

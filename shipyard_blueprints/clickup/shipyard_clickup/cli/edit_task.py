import argparse
import sys
from shipyard_clickup import ClickupClient
from shipyard_templates import ExitCodeError


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', required=True, help='Access token for ClickUp API')
    parser.add_argument('--task-id', required=True, help='ID of the ClickUp task')
    parser.add_argument('--name',  help='Name of the task')
    parser.add_argument('--description')
    parser.add_argument('--tags')
    parser.add_argument('--due-date')
    parser.add_argument('--status')
    parser.add_argument('--parent', help='Use if ticket should be a subtask of another ticket')
    return parser.parse_args()


def main():
    args = get_args()
    args_dict = vars(args)
    clickup = ClickupClient(access_token=args.access_token)
    args_dict.pop('access_token')

    if args_dict['tags']:
        args_dict['tags'] = args_dict['tags'].split(',')

    update_ticket_args = {key: value for key, value in args_dict.items() if value not in (None, '')}

    try:
        clickup.update_ticket(**update_ticket_args)
    except ExitCodeError as error:
        clickup.logger.error(error.message)
        sys.exit(error.exit_code)
    except Exception as error:
        clickup.logger.error(error)
        sys.exit(ClickupClient.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == '__main__':
    main()

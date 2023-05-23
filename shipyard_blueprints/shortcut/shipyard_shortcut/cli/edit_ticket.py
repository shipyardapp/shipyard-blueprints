import argparse
import sys

from shipyard_shortcut import ShortcutClient
from shipyard_shortcut.error_handler import handle_error

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument('--story-id', dest='story_id', required=True)
    parser.add_argument('--story-name', dest='story_name', required=False, default=None)
    parser.add_argument('--workflow-state-id', dest='workflow_state_id', required=False, default=None)
    parser.add_argument('--description', dest='description', required=False, default=None)
    parser.add_argument('--story-type', dest='story_type', choices=['feature', 'bug', 'chore'], required=True)
    parser.add_argument('--labels', dest='labels', required=False, default=None)
    parser.add_argument('--deadline', dest='deadline', required=False, default=None)
    parser.add_argument('--tasks', dest='tasks', required=False, default=None)
    parser.add_argument('--verbose', dest='verbose', default=False, action='store_true')
    return parser.parse_args()


def main():
    args = get_args()
    args_dict = vars(args)
    shortcut = ShortcutClient(access_token=args.access_token, verbose=args.verbose)
    args_dict.pop('access_token')
    args_dict.pop('verbose')

    tasks = None
    if args_dict['labels']:
        labels = args_dict['labels'].split(',')
        args_dict['labels'] = [{"name": label} for label in labels]

    if args_dict['tasks']:
        tasks = args_dict['tasks'].split(',')
        args_dict.pop('tasks')

    # Filter out blank values from update_ticket_args to avoid sending them to the Shortcut API
    # and inadvertently overwriting valid ticket data.
    update_ticket_args = {key: value for key, value in args_dict.items() if value not in (None, '')}

    try:
        shortcut.update_ticket(**update_ticket_args)
        if tasks:
            for task in tasks:
                shortcut.add_task(story_id=args.story_id, task_name=task)
    except Exception as error:
        shortcut.logger.error(error)
        handle_error(shortcut, error)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()

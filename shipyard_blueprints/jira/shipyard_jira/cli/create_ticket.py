import argparse
import sys
import logging
from shipyard_jira import JiraClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--email", dest="email", required=True)
    parser.add_argument('--domain', dest='domain', required=True)
    parser.add_argument('--project-key', dest='project_key', required=True)
    parser.add_argument('--parent-ticket-key', dest='parent_ticket_id', required=False, default=None)
    parser.add_argument('--summary', dest='summary', required=True)
    parser.add_argument('--description', dest='description', required=False, default=None)
    parser.add_argument('--issue-type', dest='issue_type', required=True)
    parser.add_argument('--assignee', dest='assignee', required=False, default=None)
    parser.add_argument('--labels', dest='labels', required=False, default=None)
    parser.add_argument('--components', dest='components', required=False, default=None)
    parser.add_argument('--due-date', dest='duedate', required=False, default=None)
    parser.add_argument('--priority', dest='priority', required=False, default=None)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    args_dict = vars(args)
    jira = JiraClient(access_token=args.access_token,
                      domain=args.domain,
                      email_address=args.email)
    args_dict.pop('access_token')
    args_dict.pop('email')
    args_dict.pop('domain')
    if args_dict['labels']:
        args_dict['labels'] = args_dict['labels'].split(',')
    # Filter out blank values from update_ticket_args to avoid sending them to the Jira API
    # and inadvertently overwriting valid ticket data.
    create_ticket_args = {key: value for key, value in args_dict.items() if value not in (None, '')}

    if args.parent_ticket_id:
        try:
            jira.create_subtask(**create_ticket_args)
        except Exception as e:
            logging.error(e)
            sys.exit(1)
        else:
            sys.exit(0)
    else:
        try:
            jira.create_ticket(**create_ticket_args)
        except Exception:
            sys.exit(1)
        else:
            sys.exit(0)


if __name__ == '__main__':
    main()

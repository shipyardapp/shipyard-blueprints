import argparse
import sys

from shipyard_jira import JiraClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--domain", dest="domain", required=True)
    parser.add_argument("--email", dest="email", required=True)
    parser.add_argument("--ticket-key", dest="ticket_key", required=True)
    parser.add_argument("--comment", dest="comment", required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    jira = JiraClient(access_token=args.access_token,
                      domain=args.domain,
                      email_address=args.email)

    try:
        jira.add_comment(ticket_key=args.ticket_key, comment=args.comment)
    except Exception as error:
        jira.logger.error(error)
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()

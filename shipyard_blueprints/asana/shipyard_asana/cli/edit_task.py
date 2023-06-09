import asana
import json
import argparse
import logging
import sys

EXIT_CODE_BAD_REQUEST = 201
EXIT_CODE_TICKET_NOT_FOUND = 202

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token',type=str, required = True, dest = 'access_token')
    parser.add_argument('--ticket-id', type = str, required = True, dest = 'ticket_id')
    parser.add_argument('--name', type = str, dest = 'name', required = False, default = '')
    parser.add_argument('--resource-subtype', type = str, dest = 'resource_subtype', required = False, default = 'default_task', choices = {'default_task', 'milestone','section','approval'})
    parser.add_argument('--approval_status', type = str, dest = 'approval_status', required = False, default = 'pending', choices = {'pending','approved','rejected','changes_requested'})
    parser.add_argument("--assignee", type = str, dest = 'assignee', required = False, default = '')
    parser.add_argument('--due-on', type = str, dest = 'due_on', required = False, default = '')
    parser.add_argument('--notes', type = str, dest = 'notes', required = False, default = '')

    return parser.parse_args()

def get_logger():
    logger = logging.getLogger("Shipyard")
    logger.setLevel(logging.DEBUG)
    # Add handler for stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # add specific format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s -%(lineno)d: %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)
    return logger

def main():
    args = get_args()
    logger = get_logger()

    client = asana.Client.access_token(args.access_token)
    client.headers['Asana-Disable'] = 'new_user_task_lists,new_goal_memberships' # suppress warnings
    params = {}
    if args.name != '':
        params['name'] = args.name
    if args.resource_subtype != '':
        params['resource_subtype'] = args.resource_subtype
    if args.approval_status != '': 
        params['approval_status'] = args.approval_status
    if args.due_on != '':
        params['due_on'] = args.due_on
    if args.assignee != '':
        params['assignee'] = args.assignee
    if args.notes != '':
        params['notes'] = args.notes

    try:
        logger.info(f"Updating task {args.ticket_id} with params: {json.dumps(params)}")
        client.tasks.update_task(args.ticket_id, params)
        logger.info(f"Task successfully updated")
    except Exception as e:
        if "Not Found: task" in str(e):
            logger.error(f"Task {args.ticket_id} not found. Ensure that the task exists and that you have access to it.")
            sys.exit(EXIT_CODE_TICKET_NOT_FOUND)
        logger.error(f"Error updating task: {e}")
        sys.exit(EXIT_CODE_BAD_REQUEST)

if __name__ == '__main__':
    main()

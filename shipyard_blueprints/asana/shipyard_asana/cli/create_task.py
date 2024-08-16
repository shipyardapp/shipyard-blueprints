import asana
import json
import argparse
import logging
import sys

EXIT_CODE_BAD_REQUEST = 201
EXIT_CODE_INVALID_USER = 202


def get_logger():
    logger = logging.getLogger("Shipyard")
    logger.setLevel(logging.DEBUG)
    # Add handler for stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # add specific format
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s -%(lineno)d: %(message)s"
    )
    console.setFormatter(formatter)
    logger.addHandler(console)
    return logger


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", required=True, dest="access_token")
    parser.add_argument("--workspace-id", required=True, dest="workspace_id")
    parser.add_argument("--project-id", dest="project_id", required=False, default="")
    parser.add_argument("--name", dest="name", required=True)
    parser.add_argument(
        "--resource-subtype",
        dest="resource_subtype",
        required=False,
        default="default_task",
        choices={"default_task", "milestone", "section", "approval"},
    )
    parser.add_argument(
        "--approval-status",
        dest="approval_status",
        required=False,
        default="pending",
        choices={"pending", "approved", "rejected", "changes_requested"},
    )
    parser.add_argument("--assignee", dest="assignee", required=False, default="")
    parser.add_argument("--due-on", dest="due_on", required=False, default="")
    parser.add_argument("--notes", dest="notes", required=False, default="")

    return parser.parse_args()


def main():
    logger = get_logger()
    args = get_args()
    client = asana.Client.access_token(args.access_token)
    client.headers[
        "Asana-Disable"
    ] = "new_user_task_lists,new_goal_memberships"  # suppress warnings
    workspace_id = args.workspace_id
    project_id = args.project_id
    name = args.name
    resource_subtype = args.resource_subtype
    approval_status = args.approval_status
    due_on = args.due_on

    params = {
        "name": name,
        "resource_subtype": resource_subtype,
        "approval_status": approval_status,
        "due_on": None if due_on == "" else due_on,
        "workspace": workspace_id,
        "projects": [] if project_id == "" else [project_id],
        "assignee": None if args.assignee == "" else args.assignee,
        "notes": None if args.notes == "" else args.notes,
    }

    logger.info("Creating task with params: {}".format(json.dumps(params)))
    try:
        if project_id != "":
            logger.info(f"Attempting to create task within project {project_id}")
        else:
            logger.info(
                f"No project id provided, creating task without project in workspace {workspace_id}"
            )
        result = client.tasks.create_task(params)
        logger.info(
            f"Task successfully created with gid: {result['gid']} located at {result['permalink_url']}'"
        )

    except Exception as e:
        if "Not an email" in str(e):
            logger.error(f"Invalid user provided: {args.assignee}")
            sys.exit(EXIT_CODE_INVALID_USER)
        else:
            logger.error(f"Error creating task: {e}")
            sys.exit(EXIT_CODE_BAD_REQUEST)


if __name__ == "__main__":
    main()

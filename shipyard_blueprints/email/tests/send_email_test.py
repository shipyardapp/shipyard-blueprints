import os
import pytest
import subprocess
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="module")
def credential_group():
    return {
        "--smtp-host": os.getenv("EMAIL_SMTP_HOST"),
        "--smtp-port": os.getenv("EMAIL_SMTP_PORT"),
        "--username": os.getenv("EMAIL_USERNAME"),
        "--password": os.getenv("EMAIL_PASSWORD"),
    }


base_args = {
    "--to": "",
    "--subject": "",
    "--message": "",
    "--sender-address": "",
    "--send-method": "",
    "--source-file-name": "",
    "--source-folder-name": "",
    "--conditional-send": "",
    "--source-file-name-match-type": "",
    "--file-upload": "",
    "--include-shipyard-footer": "",
}


def run_script(args):
    run_command = [
        "poetry",
        "run",
        "python3",
        "shipyard_email/cli/send_email.py",
    ] + args
    return subprocess.run(run_command, capture_output=True, text=True, cwd=os.getcwd())


@pytest.mark.parametrize(
    "specific_args, expected_output",
    [
        (  # Test Case: Single recipient
            {
                "--to": os.environ["VALID_RECIPIENT_1"],
                "--subject": "Test",
                "--message": "Test",
                "--sender-address": os.environ["EMAIL_USERNAME"],
            },
            0,
        ),
        (  # Test Case: Single bcc recipient
            {
                "--bcc": os.environ["VALID_RECIPIENT_1"],
                "--subject": "Test",
                "--message": "Test",
                "--sender-address": os.environ["EMAIL_USERNAME"],
            },
            0,
        ),
        (  # Test Case: Single cc recipient
            {
                "--cc": os.environ["VALID_RECIPIENT_1"],
                "--subject": "Test",
                "--message": "Test",
                "--sender-address": os.environ["EMAIL_USERNAME"],
            },
            0,
        ),
        (  # Test Case: to bcc cc recipient
            {
                "--to": os.environ["VALID_RECIPIENT_1"],
                "--bcc": os.environ["VALID_RECIPIENT_1"],
                "--cc": os.environ["VALID_RECIPIENT_1"],
                "--subject": "Test",
                "--message": "Test",
                "--sender-address": os.environ["EMAIL_USERNAME"],
            },
            0,
        ),
        (  # Test Case: No Subject
            {
                "--to": os.environ["VALID_RECIPIENT_1"],
                "--message": "Test",
                "--sender-address": os.environ["EMAIL_USERNAME"],
            },
            0,
        ),
        (  # Test Case: Missing Recipient
            {
                "--sender-address": os.environ["EMAIL_USERNAME"],
            },
            2,
        ),
    ],
)
def test_send_email(specific_args, expected_output, credential_group):
    final_args = {**base_args, **credential_group, **specific_args}
    args_list = [item for pair in final_args.items() for item in pair if pair[1]]
    result = run_script(args_list)

    assert result.returncode == expected_output

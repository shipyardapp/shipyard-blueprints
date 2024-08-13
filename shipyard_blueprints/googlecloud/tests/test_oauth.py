import pytest
import os
import subprocess
from copy import deepcopy
from dotenv import load_dotenv


load_dotenv(".env.gcs.oauth")


@pytest.fixture(scope="module")
def up_cmd():
    return [
        "python3",
        "./shipyard_googlecloud/cli/upload.py",
        "--bucket",
        "shipyard_demo_bucket",
    ]


def test_upload_oauth(up_cmd):
    cmd = deepcopy(up_cmd)
    cmd.extend(
        [
            "--source-file-name",
            "single.csv",
            "--destination-file-name",
            "oauth_file.csv",
        ]
    )
    process = subprocess.run(cmd, check=True)

    assert process.returncode == 0

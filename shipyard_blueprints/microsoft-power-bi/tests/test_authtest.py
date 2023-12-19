import os
import pytest
import subprocess
from dotenv import load_dotenv


@pytest.fixture(autouse=True)
def check_env_file():
    if not os.path.exists(".env"):
        pytest.skip(".env file not found, all tests skipped.")
    else:
        load_dotenv()

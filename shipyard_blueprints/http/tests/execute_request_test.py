import subprocess
import pytest


# Function to run the script with subprocess and return the output
def run_script(args):
    base_command = ["python3", "shipyard_http/cli/execute_request.py"]
    return subprocess.run(base_command + args, capture_output=True, text=True)


# Test cases for different HTTP methods
@pytest.mark.parametrize(
    "method, url, message, content_type",
    [
        ("GET", "https://httpbin.org/get", None, None),
        (
            "POST",
            "https://httpbin.org/post",
            '{"name": "John", "age": 30}',
            "application/json",
        ),
        (
            "PUT",
            "https://httpbin.org/put",
            '{"name": "Jane", "age": 25}',
            "application/json",
        ),
        ("PATCH", "https://httpbin.org/patch", '{"name": "Alice"}', "application/json"),
    ],
)
def test_http_methods(method, url, message, content_type):
    args = ["--method", method, "--url", url, "--print-response", "TRUE"]
    if message:
        args += ["--message", message]
    if content_type:
        args += ["--content-type", content_type]

    result = run_script(args)

    assert (
        result.returncode == 0
    ), f"Script exited with {result.returncode}, stderr: {result.stderr}"

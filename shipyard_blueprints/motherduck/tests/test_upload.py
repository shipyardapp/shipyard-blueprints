import pytest
import os
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy
import subprocess
from shipyard_motherduck import MotherDuckClient

if env_exists := find_dotenv():
    load_dotenv()


csv_replace = "pytest_csv_upload"
single_file = "test.csv"
parquet_replace = "pytest_parquet_upload"

csv_multiple_upload = "pytest_csv_multiple_upload"
parquet_multiple_upload = "pytest_parquet_multiple_upload"

new_database = "newdb"


@pytest.fixture(scope="module")
def up_cmd():
    return [
        "python3",
        "./shipyard_motherduck/cli/upload.py",
        "--token",
        os.getenv("MOTHERDUCK_TOKEN"),
    ]


@pytest.fixture(scope="module")
def exec_cmd():
    return [
        "python3",
        "./shipyard_motherduck/cli/execute.py",
        "--token",
        os.getenv("MOTHERDUCK_TOKEN"),
    ]


@pytest.fixture(scope="module")
def duck() -> MotherDuckClient:
    return MotherDuckClient(os.getenv("MOTHERDUCK_TOKEN"))


def read_all_files(directory: str, file_type: str):
    """
    Reads all files in a directory and returns a single dataframe. The file type can either be a csv or parquet
    """
    files = os.listdir(directory)
    data = []
    for file in files:
        if file.endswith(file_type):
            path = os.path.join(directory, file)
            if file_type == "csv":
                data.append(pd.read_csv(path))
            elif file_type == "parquet":
                data.append(pd.read_parquet(path))
    return pd.concat(data)


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_upload_csv_replace(up_cmd):
    cmd = deepcopy(up_cmd)
    cmd.extend(
        [
            "--table-name",
            "pytest_csv_upload",
            "--file-name",
            "test.csv",
            "--insert-method",
            "replace",
            "--match-type",
            "exact_match",
        ]
    )
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_upload_parquet_replace(up_cmd):
    cmd = deepcopy(up_cmd)
    cmd.extend(
        [
            "--table-name",
            parquet_replace,
            "--file-name",
            "test.parquet",
            "--insert-method",
            "replace",
            "--match-type",
            "exact_match",
        ]
    )
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_fetch_csv_replace_data(duck):
    results = duck.fetch(f"SELECT * FROM  {csv_replace}").to_df()
    orignal_data = pd.read_csv("test.csv")
    assert results.shape[0] == orignal_data.shape[0]


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_fetch_parquet_replace_data(duck):
    results = duck.fetch(f"SELECT * FROM  {parquet_replace}").to_df()
    orignal_data = pd.read_parquet("test.parquet")
    assert results.shape[0] == orignal_data.shape[0]


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_upload_csv_append(up_cmd):
    cmd = deepcopy(up_cmd)
    cmd.extend(
        [
            "--table-name",
            "pytest_csv_upload",
            "--file-name",
            "test.csv",
            "--insert-method",
            "append",
            "--match-type",
            "exact_match",
        ]
    )
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_upload_parquet_append(up_cmd):
    cmd = deepcopy(up_cmd)
    cmd.extend(
        [
            "--table-name",
            parquet_replace,
            "--file-name",
            "test.parquet",
            "--insert-method",
            "append",
            "--match-type",
            "exact_match",
        ]
    )
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_fetch_parquet_append_data(duck):
    results = duck.fetch(f"SELECT * FROM  {parquet_replace}").to_df()
    orignal_data = pd.read_parquet("test.parquet")
    assert results.shape[0] == orignal_data.shape[0] * 2


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def fetch_csv_append_data(duck):
    results = duck.fetch(f"SELECT * FROM  {csv_replace}").to_df()
    orignal_data = pd.read_csv("test.csv")
    assert results.shape[0] == orignal_data.shape[0] * 2


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_upload_multiple_csv_replace(up_cmd):
    cmd = deepcopy(up_cmd)
    cmd.extend(
        [
            "--table-name",
            csv_multiple_upload,
            "--file-name",
            "*.csv",
            "--insert-method",
            "replace",
            "--directory",
            "mult",
            "--match-type",
            "glob_match",
        ]
    )
    result = subprocess.run(cmd, capture_output=True)
    print(result.stderr)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_fetch_multiple_csv_replace(duck):
    results = duck.fetch(f"SELECT * FROM  {csv_multiple_upload}").to_df()
    orignal_data = read_all_files("mult", "csv")
    assert results.shape[0] == orignal_data.shape[0]


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_upload_multiple_csv_append(up_cmd):
    cmd = deepcopy(up_cmd)
    cmd.extend(
        [
            "--table-name",
            csv_multiple_upload,
            "--file-name",
            "*.csv",
            "--insert-method",
            "append",
            "--directory",
            "mult",
            "--match-type",
            "glob_match",
        ]
    )
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_fetch_multiple_csv_append(duck):
    results = duck.fetch(f"SELECT * FROM  {csv_multiple_upload}").to_df()
    orignal_data = read_all_files("mult", "csv")
    assert results.shape[0] == orignal_data.shape[0] * 2


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_upload_multiple_parquet_replace(up_cmd):
    cmd = deepcopy(up_cmd)
    cmd.extend(
        [
            "--table-name",
            parquet_multiple_upload,
            "--file-name",
            "*.parquet",
            "--insert-method",
            "replace",
            "--directory",
            "mult",
            "--match-type",
            "glob_match",
        ]
    )
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_fetch_multiple_parquet_replace(duck):
    results = duck.fetch(f"SELECT * FROM  {parquet_multiple_upload}").to_df()
    orignal_data = read_all_files("mult", "parquet")
    assert results.shape[0] == orignal_data.shape[0]


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_upload_multiple_parquet_append(up_cmd):
    cmd = deepcopy(up_cmd)
    cmd.extend(
        [
            "--table-name",
            parquet_multiple_upload,
            "--file-name",
            "*.parquet",
            "--insert-method",
            "append",
            "--directory",
            "mult",
            "--match-type",
            "glob_match",
        ]
    )
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_fetch_multiple_parquet_append(duck):
    results = duck.fetch(f"SELECT * FROM  {parquet_multiple_upload}").to_df()
    orignal_data = read_all_files("mult", "parquet")
    assert results.shape[0] == orignal_data.shape[0] * 2


# other database upload
@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_upload_csv_to_other_databse(up_cmd):
    cmd = deepcopy(up_cmd)
    cmd.extend(
        [
            "--table-name",
            "pytest_csv_upload",
            "--file-name",
            "test.csv",
            "--insert-method",
            "replace",
            "--match-type",
            "exact_match",
            "--database",
            new_database,
        ]
    )
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0


# cleanup
@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_drop_csv_upload(exec_cmd):
    cmd = deepcopy(exec_cmd)
    cmd.extend(["--query", f"DROP TABLE {csv_replace}"])
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_drop_parquet_upload(exec_cmd):
    cmd = deepcopy(exec_cmd)
    cmd.extend(["--query", f"DROP TABLE {parquet_replace}"])
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_drop_csv_multiple_upload(exec_cmd):
    cmd = deepcopy(exec_cmd)
    cmd.extend(["--query", f"DROP TABLE {csv_multiple_upload}"])
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_drop_parquet_multiple_upload(exec_cmd):
    cmd = deepcopy(exec_cmd)
    cmd.extend(["--query", f"DROP TABLE {parquet_multiple_upload}"])
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_drop_new_database(exec_cmd):
    cmd = deepcopy(exec_cmd)
    cmd.extend(["--query", f"DROP DATABASE {new_database}"])
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0

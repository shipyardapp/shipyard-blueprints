import sys
import subprocess


def get_changed_dirs(before_sha: str, after_sha: str):
    """
    Retrieve the list of directories that have changed between two Git SHAs.
    This has been created to be used in GitHub Actions to determine which blueprints have changed.

    Args:
        before_sha (str): The starting SHA.
        after_sha (str): The ending SHA.

    Returns:
        list: A sorted list of changed directories within "shipyard_blueprints".
    """
    cmd = ["git", "diff", "--name-only", before_sha, after_sha]
    result = subprocess.run(cmd, capture_output=True, text=True)
    changed_files = result.stdout.splitlines()

    changed_dirs = set()

    for file in changed_files:
        if file.startswith("shipyard_blueprints"):
            directories = file.split("/")
            vendor = directories[1]
            changed_dirs.add(vendor)

    return sorted(changed_dirs)


def run_pytest_for_vendors(vendors):
    """
    Run pytest for each specified vendor.

    Args:
        vendors (list): List of vendor names.
    """
    for vendor in vendors:
        vendor_dir = f"shipyard_blueprints/{vendor}"
        # Installing dependencies
        subprocess.run(["poetry", "install"], cwd=vendor_dir)
        # Running pytest
        subprocess.run(["poetry", "run", "pytest"], cwd=vendor_dir)
if __name__ == "__main__":
    before_sha = sys.argv[1]
    after_sha = sys.argv[2]
    vendors = get_changed_dirs(before_sha, after_sha)
    run_pytest_for_vendors(vendors)

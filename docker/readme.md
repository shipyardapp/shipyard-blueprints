# README: Using the LocalTest Script

## Overview
The script will build a Docker image and run it in a container. The script will also mount the current directory as a volume inside the container. This allows you to edit your code locally and run it inside the container.

You my need to run `chmod +x localtest.sh` to make the script executable.

<b> Environment Variables </b>

Loads the environment variables from the .env file in the vendor directory. This is useful for storing sensitive information like API keys and access tokens.
## Command-Line Arguments Breakdown

| Argument                 | Description                                                                                                                                                      | Example                                                   |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|
| `-t`, `--tag`            | Specifies a custom tag for the Docker image.                                                                                                                     | `./docker/localtest --tag custom_tag`                         |
| `-v`, `--vendor`         | Sets the vendor name used as a build argument for the Docker image.                                                                                              | `./docker/localtest --vendor my_vendor`                       |
| `-p`, `--packages`       | Specifies additional Python packages to be installed in the Docker image. This is helpful for CLI commands that have separate dependencies than the python class | `./docker/localtest --packages "requests numpy"`              |
| `-s`, `--script`         | Determines which Python script to run inside the Docker container.                                                                                               | `./docker/localtest --script my_script.py`                    |
| `--force-rebuild`        | Forces a rebuild of the Docker image if an image with the specified tag already exists.                                                                          | `./docker/localtest --force-rebuild`                          |

### Script Arguments

Any arguments provided after the recognized options will be passed as arguments to the specified script inside the Docker container. For example:

| Argument                 | Description                                                                               | Example                                                   |
|--------------------------|-------------------------------------------------------------------------------------------|-----------------------------------------------------------|
| `--` followed by args    | Differentiates between arguments intended for the `localtest` script and the Python script.| `./docker/localtest --script my_script.py -- --access-token $ACCESS_TOKEN --verbose=TRUE` |

## Examples
* `./docker/localtest --vendor hubspot --script authtest.py`
* `./docker/localtest --vendor clickup --script add_comment.py --access-token $CLICKUP_ACCESS_TOKEN --task-id 123 --comment "this is a test"`

## Conclusion
The `localtest` script offers a comprehensive set of command-line arguments for building and running Docker containers. By understanding and leveraging these arguments, you can customize the Docker process to suit your requirements.


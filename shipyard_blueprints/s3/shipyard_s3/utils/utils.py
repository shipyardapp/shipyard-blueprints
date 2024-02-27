from typing import Optional, Dict, Any, List


def list_objects(
    s3_conn, bucket_name: str, prefix: str, continuation_token: Optional[str] = None
) -> Dict:
    """Helper function that returns the first 1000 of objects from a given bucket

    Args:
        s3_conn (): The S3 connection
        bucket_name: The name of  the bucket to scan
        prefix: The prefix (folder path) to use
        continuation_token: The refresh token

    Returns:

    """
    kwargs = {"Bucket": bucket_name, "Prefix": prefix}
    if continuation_token:
        kwargs["ContinuationToken"] = continuation_token

    response = s3_conn.list_objects_v2(**kwargs)
    return response


def get_files(response: dict) -> List[Any]:
    return [obj["Key"] for obj in response.get("Contents", [])]

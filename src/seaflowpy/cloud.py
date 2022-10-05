import boto3
import botocore

from . import errors

def get_s3_file_list(bucket_name, prefix):
    """Get list of S3 object keys for one folder"""
    while prefix.endswith("/"):
        prefix = prefix[:-1]
    prefix = prefix + "/"
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    exists = True
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False
    if not exists:
        raise errors.S3Error(f"S3 bucket {bucket_name} does not exist")

    files = []
    for obj in bucket.objects.filter(Prefix=prefix):
        files.append(obj.key)
    return files

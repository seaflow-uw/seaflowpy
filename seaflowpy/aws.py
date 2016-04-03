import boto3
import botocore
import io
import random
import time


def get_s3_bucket(s3, bucket_name):
    """Get a boto3 S3 Bucket object"""
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
        raise IOError("S3 bucket %s does not exist" % bucket_name)
    return bucket


def get_s3_files(folder, bucket_name):
    """Get list of S3 object keys for one cruise"""
    while folder.endswith("/"):
        folder = folder[:-1]
    folder = folder + "/"
    s3 = boto3.resource("s3")
    bucket = get_s3_bucket(s3, bucket_name)
    files = []
    for obj in bucket.objects.filter(Prefix=folder):
        files.append(obj.key)
    return files


def download_s3_file_memory(key_str, bucket_name, retries=5):
    """Return S3 file contents in io.BytesIO file-like object"""
    tries = 0
    while True:
        try:
            s3 = boto3.resource("s3")
            obj = s3.Object(bucket_name, key_str)
            resp = obj.get()
            data = io.BytesIO(resp["Body"].read())
            return data
        except:
            tries += 1
            if tries == retries:
                raise
            sleep = (2**(tries-1)) + random.random()
            time.sleep(sleep)

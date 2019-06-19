"""
This module contains classes for interfacing with various cloud providers. Each
class should provide at least the following public methods:

start() - Starts cloud instances
cleanup() - Frees cloud resources managed by this class
get_files() - Get a list of files within a folder in cloud storage
download_file_memory() - Download a file from cloud storage to memory

These methods are intended to be independent of any specific cloud provider,
making it simple to replace one provider for another.
"""
import io
import random
import time
import boto3
import botocore


class AWS:
    """
    A class for high-level operations in EC2 and S3.
    """

    def __init__(self, config_items):
        # Store IDs and public IPs instances
        self.state = {
            "InstanceIds": [],
            "hosts": []
        }
        # Make config options accessible as object attributes
        for k, v in config_items:
            setattr(self, k, v)

    def cleanup(self):
        if self.state["InstanceIds"]:
            print("Terminating {} instances...".format(len(self.state["InstanceIds"])))
            client = boto3.client('ec2')
            client.terminate_instances(InstanceIds=self.state["InstanceIds"])
            waiter = client.get_waiter("instance_terminated")
            waiter.wait(InstanceIds=self.state["InstanceIds"])
            print("Instances terminated")

    def start(self, instance_type="c3.4xlarge", count=1):
        client = boto3.client('ec2')
        resp = client.run_instances(
            ImageId=getattr(self, "image-id"),
            MinCount=count,
            MaxCount=count,
            SecurityGroups=[getattr(self, "security-group")],
            InstanceType=instance_type,
            KeyName=getattr(self, "ssh-private-key-name")
        )
        ids = self._get_instance_ids(self._get_instances(resp))
        self.state["InstanceIds"] = ids

        waiter = client.get_waiter("instance_running")
        waiter.wait(InstanceIds=ids)
        resp = client.describe_instances(InstanceIds=ids)
        ips = self._get_publicips(self._get_instances(resp))
        self.state["hosts"].extend(ips)

        return {
            "InstanceIds": ids,
            "publicips": ips
        }

    def get_files(self, folder):
        """Get list of S3 object keys for one folder"""
        while folder.endswith("/"):
            folder = folder[:-1]
        folder = folder + "/"
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(getattr(self, "s3-bucket"))
        exists = True
        try:
            s3.meta.client.head_bucket(Bucket=getattr(self, "s3-bucket"))
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False
        if not exists:
            raise IOError("S3 bucket %s does not exist" % getattr(self, "s3-bucket"))

        files = []
        for obj in bucket.objects.filter(Prefix=folder):
            files.append(obj.key)
        return files

    def download_file_memory(self, key_str, retries=5):
        """Return S3 file contents in io.BytesIO file-like object"""
        tries = 0
        while True:
            try:
                s3 = boto3.resource("s3")
                obj = s3.Object(getattr(self, "s3-bucket"), key_str)
                resp = obj.get()
                data = io.BytesIO(resp["Body"].read())
                return data
            except Exception:
                tries += 1
                if tries == retries:
                    raise
                sleep = (2**(tries-1)) + random.random()
                time.sleep(sleep)

    @staticmethod
    def _get_instances(resp):
        try:
            return resp["Instances"]
        except KeyError:
            return resp["Reservations"][0]["Instances"]

    @staticmethod
    def _get_instance_ids(instances):
        return [x["InstanceId"] for x in instances]

    @staticmethod
    def _get_publicips(instances):
        return [x["NetworkInterfaces"][0]["Association"]["PublicIp"] for x in instances]

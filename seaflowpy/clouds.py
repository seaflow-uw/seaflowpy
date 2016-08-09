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

import boto3
import botocore
import io
import random
import time


class AWS(object):
    """
    A class for high-level operations in EC2 and S3.
    """

    block_device_mapping = [
        {
            "DeviceName": "/dev/sdf",
            "VirtualName": "ephemeral0"
        },
        {
            "DeviceName": "/dev/sdg",
            "VirtualName": "ephemeral1"
        }
    ]

    user_data_text = """#!/bin/bash -e
# Create a RAID0 ext4 scratch volume from two local SSD drives on an EC2 c3
# machine and mount it at /mnt/raid

ephemerals=()

function check_mount {
  mounted=$(mount | gawk '$3 == "/mnt" {print $1}')
  if [[ -n "$mounted" ]]; then
    echo "Unmounting $mounted"
    umount /mnt
  else
    echo "No instance store device mounted"
  fi
}

function find_ephemerals {
  for ephname in xvdf xvdg xvdh xvdi; do
    devname=/dev/$ephname
    if [[ -e $devname ]]; then
      echo "Found $devname"
      ephemerals+=($devname)
    fi
  done
}

function create_raid {
  if [[ ${#ephemerals[@]} -ne 0 ]]; then
    if [[ ! -e /dev/md0 ]]; then
      mdadm --create --run --verbose /dev/md0 -c256 --name=MY_RAID --level=0 --raid-devices=${#ephemerals[@]} ${ephemerals[@]}
      echo DEVICE ${ephemerals[@]} | tee /etc/mdadm/mdadm.conf
      mdadm --detail --scan | tee -a /etc/mdadm/mdadm.conf
      blockdev --setra 65536 /dev/md0
      echo "Created RAID0 from ${ephemerals[@]} at /dev/md0"
      mkfs -t ext4 -L MY_RAID /dev/md0
      echo "Created ext4 filesystem on /dev/md0"
      mkdir /mnt/raid
      mount -t ext4 -o noatime LABEL=MY_RAID /mnt/raid
      chown ubuntu:ubuntu /mnt/raid
      echo "Mounted /dev/md0 at /mnt/raid"
      grep -v '/dev/xvd' /etc/fstab >/etc/fstab.new
      echo "LABEL=MY_RAID /mnt/raid ext4 defaults,nofail 0 2" >> /etc/fstab.new
      mv /etc/fstab /etc/fstab.orig
      mv /etc/fstab.new /etc/fstab
    fi
  fi
}


check_mount
find_ephemerals
create_raid
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
            UserData=self.user_data_text,
            InstanceType=instance_type,
            KeyName=getattr(self, "key-name"),
            BlockDeviceMappings=self.block_device_mapping
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
            except:
                tries += 1
                if tries == retries:
                    raise
                sleep = (2**(tries-1)) + random.random()
                time.sleep(sleep)

    def _get_instances(self, resp):
        try:
            return resp["Instances"]
        except KeyError:
            return resp["Reservations"][0]["Instances"]

    def _get_instance_ids(self, instances):
        return [x["InstanceId"] for x in instances]

    def _get_publicips(self, instances):
        return [x["NetworkInterfaces"][0]["Association"]["PublicIp"] for x in instances]

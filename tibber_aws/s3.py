import datetime
import logging

import boto3
import botocore

_LOGGER = logging.getLogger(__name__)

STATE_NOT_EXISTING = "not_existing"
STATE_OK = "ok"
STATE_PRECONDITION_FAILED = "precondition_failed"


class S3Bucket:
    def __init__(self, bucket_name, region_name='eu-west-1'):
        self._bucket_name = bucket_name
        self._region_name = region_name
        self.s3 = None
        self._bucket = None

    def get_bucket(self):
        self.s3 = boto3.resource(
            's3',
            region_name=self._region_name,
        )
        try:
            self._bucket = self.s3.create_bucket(Bucket=self._bucket_name,
                                                 CreateBucketConfiguration={'LocationConstraint':
                                                                            self._region_name}
                                                 )
        except self.s3.meta.client.exceptions.BucketAlreadyOwnedByYou:
            self._bucket = self.s3.Bucket(self._bucket_name)

    def load_json(self, key, if_unmodified_since=None):
        if if_unmodified_since is None:
            if_unmodified_since = datetime.datetime(1900, 1, 1)
        try:
            res = self._bucket.Object(key=key).get(IfUnmodifiedSince=if_unmodified_since)["Body"].read()
            return res.decode('utf-8'), STATE_OK
        except self.s3.meta.client.exceptions.NoSuchKey:
            return None, STATE_NOT_EXISTING
        except botocore.exceptions.ClientError as exp:
            if "PreconditionFailed" in str(exp):
                return None, STATE_PRECONDITION_FAILED
            raise

    def store_json(self, key, data):
        self._bucket.Object(key=key).put(Body=data)

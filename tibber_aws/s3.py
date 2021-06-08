import datetime
import logging
import zlib

import botocore

from .aws_base import AwsBase

_LOGGER = logging.getLogger(__name__)

STATE_NOT_EXISTING = "not_existing"
STATE_OK = "ok"
STATE_PRECONDITION_FAILED = "precondition_failed"


class S3Bucket(AwsBase):
    def __init__(self, bucket_name, region_name="eu-west-1"):
        self._bucket_name = bucket_name
        super().__init__("s3", region_name)

    async def load_data(self, key, if_unmodified_since=None):
        await self._init_client_if_required()
        if if_unmodified_since is None:
            if_unmodified_since = datetime.datetime(1900, 1, 1)
        try:
            res = await (
                await self._client.get_object(
                    Bucket=self._bucket_name,
                    Key=key,
                    IfUnmodifiedSince=if_unmodified_since,
                )
            )["Body"].read()
        except self._client.exceptions.NoSuchKey:
            return None, STATE_NOT_EXISTING
        except botocore.exceptions.ClientError as exp:
            if "PreconditionFailed" in str(exp):
                return None, STATE_PRECONDITION_FAILED
            raise
        if len(key) > 3 and key[-3:] == ".gz":
            content = zlib.decompressobj(zlib.MAX_WBITS | 16).decompress(res)
            return content, STATE_OK
        return res.decode("utf-8"), STATE_OK

    async def store_data(self, key, data, retry=1):
        await self._init_client_if_required()
        if len(key) > 3 and key[-3:] == ".gz":
            compressor = zlib.compressobj(wbits=zlib.MAX_WBITS | 16)
            body = compressor.compress(data)
            body += compressor.flush()
        else:
            body = data
        try:
            resp = await self._client.put_object(
                Bucket=self._bucket_name, Key=key, Body=body
            )
        except self._client.exceptions.NoSuchBucket:
            if retry > 0:
                await self._client.create_bucket(
                    Bucket=self._bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self._region_name},
                )
                return await self.store_data(key, data, retry - 1)
        return resp

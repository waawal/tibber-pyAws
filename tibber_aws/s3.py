import datetime
import logging

import aiobotocore
import botocore

_LOGGER = logging.getLogger(__name__)

STATE_NOT_EXISTING = "not_existing"
STATE_OK = "ok"
STATE_PRECONDITION_FAILED = "precondition_failed"


class S3Bucket:
    def __init__(self, bucket_name, region_name="eu-west-1"):
        self._bucket_name = bucket_name
        session = aiobotocore.get_session()
        self.client = session.create_client("s3", region_name=region_name)

    async def load_data(self, key, if_unmodified_since=None):
        if if_unmodified_since is None:
            if_unmodified_since = datetime.datetime(1900, 1, 1)
        try:
            res = await (
                await self.client.get_object(
                    Bucket=self._bucket_name,
                    Key=key,
                    IfUnmodifiedSince=if_unmodified_since,
                )
            )["Body"].read()
        except self.client.exceptions.NoSuchKey:
            return None, STATE_NOT_EXISTING
        except botocore.exceptions.ClientError as exp:
            if "PreconditionFailed" in str(exp):
                return None, STATE_PRECONDITION_FAILED
            raise
        return res.decode("utf-8"), STATE_OK

    async def store_data(self, key, data):
        resp = await self.client.put_object(
            Bucket=self._bucket_name, Key=key, Body=data
        )
        return resp

    async def close(self):
        await self.client.close()

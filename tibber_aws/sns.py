import contextlib
import json

import aiobotocore
import boto3


class Topic:
    def __init__(self, topic_name, region_name="eu-west-1"):
        self._client = None
        self._context_stack = contextlib.AsyncExitStack()
        self._region_name = region_name
        self._topic_arn = (
            boto3.resource("sns", region_name).create_topic(Name=topic_name).arn
        )

    async def publish(self, subject, message):
        if self._client is None:
            self._client = await self._context_stack.enter_async_context(
                aiobotocore.get_session().create_client(
                    "sns", region_name=self._region_name, verify=False
                )
            )
        await self._client.publish(
            TargetArn=self._topic_arn,
            Message=json.dumps({"default": json.dumps(message)}),
            Subject=subject,
            MessageStructure="json",
        )

    async def close(self):
        await self._context_stack.aclose()

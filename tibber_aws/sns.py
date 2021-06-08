import json

import boto3

from .aws_base import AwsBase


class Topic(AwsBase):
    def __init__(self, topic_name, region_name="eu-west-1"):
        super().__init__("sns", region_name)
        self._topic_arn = (
            boto3.resource(self._service_name, region_name)
            .create_topic(Name=topic_name)
            .arn
        )

    async def publish(self, subject, message):
        await self._init_client_if_required()
        await self._client.publish(
            TargetArn=self._topic_arn,
            Message=json.dumps({"default": json.dumps(message)}),
            Subject=subject,
            MessageStructure="json",
        )

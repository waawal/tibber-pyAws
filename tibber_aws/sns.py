import aiobotocore
import boto3
import json


class SNS:
    def __init__(self, topic_name, region_name='eu-west-1'):
        self._session = aiobotocore.get_session()
        self._client = self._session.create_client('sns', region_name=region_name, verify=False)
        self._topic_arn = boto3.resource('sns', region_name).create_topic(Name=topic_name).arn

    async def publish(self, subject, message):
        await self._client.publish(
            TargetArn=self._topic_arn,
            Message=json.dumps(
                {
                    'default': json.dumps(message)
                }
            ),
            Subject=subject,
            MessageStructure='json',
        )

    async def close(self):
        await self._client.close()

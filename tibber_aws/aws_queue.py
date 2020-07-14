import json
import logging
import time

import aiobotocore

_LOGGER = logging.getLogger(__name__)


class Queue:
    def __init__(self, queue_name, region_name="eu-west-1"):
        self._queue_name = queue_name
        self._region_name = region_name
        self._session = aiobotocore.get_session()
        self._client = self._session.create_client("sqs", region_name=region_name)
        self.queue_url = None

    async def subscribe_topic(self, topic_name):

        response = await self._client.create_queue(QueueName=self._queue_name)
        self.queue_url = response["QueueUrl"]
        attr_response = await self._client.get_queue_attributes(
            QueueUrl=self.queue_url, AttributeNames=["All"]
        )

        queue_attributes = attr_response.get("Attributes")
        queue_arn = queue_attributes.get("QueueArn")

        # Set up a policy to allow SNS access to the queue
        if "Policy" in queue_attributes:
            policy = json.loads(queue_attributes["Policy"])
        else:
            policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "Sid" + str(int(time.time())),
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": ["sqs:SendMessage", "sqs:ReceiveMessage"],
                    }
                ],
            }

        statement = policy.get("Statement", [{}])[0]
        statement["Resource"] = statement.get("Resource", queue_arn)
        statement["Condition"] = statement.get("Condition", {})

        statement["Condition"]["StringLike"] = statement["Condition"].get(
            "StringLike", {}
        )
        source_arn = statement["Condition"]["StringLike"].get("aws:SourceArn", [])
        if not isinstance(source_arn, list):
            source_arn = [source_arn]

        sns = self._session.create_client("sns", region_name=self._region_name)
        response = await sns.create_topic(Name=topic_name)
        topic_arn = response["TopicArn"]

        if topic_arn not in source_arn:
            source_arn.append(topic_arn)
            statement["Condition"]["StringLike"]["aws:SourceArn"] = source_arn
            policy["Statement"] = statement
            await self._client.set_queue_attributes(
                QueueUrl=self.queue_url, Attributes={"Policy": json.dumps(policy)}
            )

        await sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
        await sns.close()

    async def send(self, subject, message, delay_seconds=0):
        if self.queue_url is None:
            _LOGGER.error("No subscribed queue")
            return None

        return await self.queue.send_message(
            QueueUrl=self.queue.url,
            DelaySeconds=delay_seconds,
            MessageBody=json.dumps(
                {"Subject": subject, "Message": json.dumps(message)}
            ),
        )

    async def receive_message(self, num_msgs=1):
        if self.queue_url is None:
            _LOGGER.error("No subscribed queue")
            return [None]
        response = await self._client.receive_message(
            QueueUrl=self.queue_url, MaxNumberOfMessages=num_msgs
        )
        res = []
        for msg in response.get("Messages", []):
            res.append(MessageHandle(msg))
        return res

    async def delete_message(self, msg_handle):
        await self._client.delete_message(
            QueueUrl=self.queue_url, ReceiptHandle=msg_handle.receipt_handle
        )

    async def close(self):
        await self._client.close()


class MessageHandle:
    def __init__(self, msg):
        self._msg = msg

    @property
    def body(self):
        return self._msg["Body"]

    @property
    def receipt_handle(self):
        return self._msg["ReceiptHandle"]

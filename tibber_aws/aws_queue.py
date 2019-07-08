import json
import logging
import time

import boto3

_LOGGER = logging.getLogger(__name__)


class Queue:
    def __init__(self, queue_name, region_name='eu-west-1'):
        self._queue_name = queue_name
        self._region_name = region_name
        self._sns = boto3.resource('sns', self._region_name)
        self._sqs = boto3.resource('sqs', self._region_name)

        self.queue = None
        self.queue_arn = None

    def subscribe_topic(self, topic_name):

        self.queue = self._sqs.create_queue(QueueName=self._queue_name)
        self.queue_arn = self.queue.attributes['QueueArn']

        # Set up a policy to allow SNS access to the queue
        if 'Policy' in self.queue.attributes:
            policy = json.loads(self.queue.attributes['Policy'])
        else:
            policy = {
                "Version": "2012-10-17",
                "Statement": [{
                    "Sid": "Sid" + str(int(time.time())),
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "*"
                    },
                    "Action": ["sqs:SendMessage", "sqs:ReceiveMessage"]
                }
                ]
            }

        statement = policy.get('Statement', [{}])[0]
        statement['Resource'] = statement.get('Resource', self.queue_arn)
        statement['Condition'] = statement.get('Condition', {})

        statement['Condition']['StringLike'] = statement['Condition'].get('StringLike', {})
        source_arn = statement['Condition']['StringLike'].get('aws:SourceArn', [])
        if not isinstance(source_arn, list):
            source_arn = [source_arn]
        topic_arn = self._sns.create_topic(Name=topic_name).arn
        if topic_arn not in source_arn:
            source_arn.append(topic_arn)
            statement['Condition']['StringLike']['aws:SourceArn'] = source_arn
            policy['Statement'] = statement
            self.queue.set_attributes(Attributes={
                'Policy': json.dumps(policy)
            })

        topic = self._sns.Topic(topic_arn)
        for subscription in topic.subscriptions.all():
            if subscription.attributes['Endpoint'] == self.queue_arn:
                return
        _LOGGER.debug("Subscribing to %s", topic_name)
        topic.subscribe(Protocol='sqs', Endpoint=self.queue_arn)

    def send(self, subject, message, delay_seconds=0):
        if self.queue is None:
            _LOGGER.error("No subscribed queue")
            return None

        return self.queue.send_message(
            QueueUrl=self.queue.url,
            DelaySeconds=delay_seconds,
            MessageBody=json.dumps({'Subject': subject, 'Message': json.dumps(message)})
        )

    def receive_message(self, num_msgs=1):
        if self.queue is None:
            _LOGGER.error("No subscribed queue")
            return None
        return self.queue.receive_messages(MaxNumberOfMessages=num_msgs)

    def delete_message(self, msg_handle):
        return msg_handle.delete()

import json

import boto3


class Topic:
    def __init__(self, topic_name, subject, region_name='eu-west-1'):
        self._topic_name = topic_name
        self._subject = subject
        self._region_name = region_name
        self._sns = boto3.client('sns')
        self._topic = boto3.resource('sns', self._region_name).create_topic(Name=topic_name)

    def push(self, message, subject=None):
        subject = subject if subject else self._subject
        self._sns.publish(TargetArn=self._topic.arn,
                          Message=json.dumps({'default': json.dumps(message)}),
                          Subject=subject,
                          )

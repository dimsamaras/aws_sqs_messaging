# -*- coding: utf-8 -*-

import boto3


class SqsManager:

	def __init__(self, session, sqs_cfg):
		"""Create a SqsManager object."""
		self.sqs = session.resource('sqs', **sqs_cfg)
		self.session = session
		self.queue = ""

	def get_queue(self, queue_name):
		self.queue = self.sqs.get_queue_by_name(QueueName=queue_name)

	def receive_messages(self, max_messages, delay):	
		return self.queue.receive_messages(MaxNumberOfMessages=max_messages, WaitTimeSeconds=delay)	

	def delete_messages(self, messages):	
		return self.queue.delete_messages(Entries=messages)  
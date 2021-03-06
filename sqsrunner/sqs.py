# -*- coding: utf-8 -*-

import boto3
from botocore.exceptions import EndpointConnectionError
import logging

logger = logging.getLogger('receiverLogger')

class SqsManager:
	"""boto3 sqs implementation."""
	
	def __init__(self, session, cfg):
		"""Create a SqsManager object."""
		self.session 	= session
		try:
			self.sqs 		= session.resource('sqs', **cfg)
		except ClientError as e:
			logger.error("Unexpected error: %s" % e)
			exit()
		self.queue 		= ""
		self.queueName 	= ""

	def get_queue(self, queue_name):
		self.queueName 	= queue_name
		try:
			self.queue 		= self.sqs.get_queue_by_name(QueueName=queue_name)
		except EndpointConnectionError as e:
			logger.error("Error connecting to queue: %s" % e)
			exit()

	def receive_messages(self, max_messages, delay):	
		return self.queue.receive_messages(MaxNumberOfMessages=max_messages, WaitTimeSeconds=delay)	

	def delete_messages(self, messages):	
		return self.queue.delete_messages(Entries=messages)  

	def get_queue_name(self):
		return self.queueName

	def send_messages(self, messages):
		return self.queue.send_messages(Entries=messages)

	def get_queue_visibility_timeout(self):
		attributes = self.queue.attributes
		return attributes['VisibilityTimeout']

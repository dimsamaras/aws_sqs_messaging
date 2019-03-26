# -*- coding: utf-8 -*-

import boto3


class CloudwatchManager:
	"""boto3 Cloudatch implementation."""

	def __init__(self, session, cfg):
		"""Create a Cloudwatch object."""
		
		try:
			self.cw = session.client('cloudwatch',**cfg)
		except ClientError as e:
			print("Unexpected error: %s" % e)
		self.session = session

	def put_metric_data(self, namespace, metrics):
		"""Put metric data."""

		cw_args 				= {}
		cw_args['Namespace'] 	= namespace
		cw_args['MetricData'] 	= metrics
		print cw_args

		try:
			metrics = self.cw.put_metric_data(**cw_args)
			print metrics
		except self.cw.exceptions.InvalidParameterValueException as e:
			print("Parameter validation error: %s" % e)
		except TypeError as e:
			print("Type error: %s" % e)
		except:
			print("Error during put metric data")

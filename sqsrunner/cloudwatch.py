# -*- coding: utf-8 -*-

import boto3


class CloudwatchManager:

	def __init__(self, session):
		"""Create a SqsManager object."""
		try:
			self.cw = session.client('cloudwatch')
		except ClientError as e:
			print("Unexpected error: %s" % e)
		self.session = session

	def put_metric_data(self, namespace, metrics):
		cw_args 				= {}
		cw_args['Namespace'] 	= namespace
		cw_args['MetricData'] 	= metrics
		try:
			metrics = self.cw.put_metric_data(**cw_args)
			print metrics
		except ParamValidationError as e:
			print("Parameter validation error: %s" % e)
		except TypeError as e:
			print("Type error: %s" % e)
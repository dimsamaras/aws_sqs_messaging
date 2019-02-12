# -*- coding: utf-8 -*-

import boto3


class CloudwatchManager:

	def __init__(self, session, sqs_cfg):
		"""Create a SqsManager object."""
		self.cw = session.client('cloudwatch')
		self.session = session

	def put_metric_data(self, namespace, metrics):
		cw_args['Namespace'] = namespace
		cw_args['Metrics'] = metrics
		return self.cw.put_metric_data(**cw_args)

# -*- coding: utf-8 -*-

import threading
import logger
from datetime import datetime

class ackThread(threading.Thread):

	def __init__(self, threadID, SQS_MANAGER, CW_MANAGER, DELETE_BATCH_MAX, CW_BATCH_MAX, ackQueue, event):
		threading.Thread.__init__(self)
		self.threadID   		= threadID
		self.SqsManager   	    = SQS_MANAGER
		self.CWManager          = CW_MANAGER
		self.delete_batch_max	= DELETE_BATCH_MAX
		self.metrics_batch_max  = CW_BATCH_MAX
		self.ackQueue   		= ackQueue
		# A flag to notify the thread that it should finish up and exit
		self.stopEvent  		= event

	def run(self):
		logger.logging.info('Starting acknowledger: %s', self.name)
		ack_messages(self)
		logger.logging.info('Exiting acknowledger: %s', self.name)
			
def ack_messages(thread):
	"""Pull messages from intemediate queue and acknowledge them."""

	delete_batch = []
	metrics = []
	while not thread.stopEvent.isSet():
		if not thread.ackQueue.empty():
			delete_batch.append(thread.ackQueue.get_nowait())
			thread.ackQueue.task_done()
		event_is_set = thread.stopEvent.wait(0.3)
		if event_is_set:
			# logger.logging.info('stop received, messages in queue {messages} '.format(messages=thread.ackQueue.qsize()))
			while not thread.ackQueue.empty() or (len(delete_batch) > thread.delete_batch_max) :
				delete_batch.append(thread.ackQueue.get_nowait())
				thread.ackQueue.task_done()

				if len(delete_batch) == thread.delete_batch_max:
					logger.logging.info('Will delete on stop = ' + " ".join(str(x) for x in delete_batch))
					delete_batch = send_ack(thread.SqsManager, delete_batch)
					metrics = calculate_metrics(metrics, delete_batch, thread.metrics_batch_max)
			if delete_batch:
				logger.logging.info('Will delete on stop final remaining = ' + " ".join(str(x) for x in delete_batch))
				delete_batch = send_ack(thread.SqsManager, delete_batch)
				metrics = calculate_metrics(metrics, delete_batch, thread.metrics_batch_max, true)
		elif len(delete_batch) == thread.delete_batch_max:
			logger.logging.info('Will delete = ' + " ".join(str(x) for x in delete_batch))
			delete_batch = send_ack(thread.SqsManager, delete_batch)
			metrics = calculate_metrics(metrics, delete_batch, thread.metrics_batch_max)

def send_ack(SqsManager, messages):
	"""Send message acknowledgement  in batches."""

	SqsManager.delete_messages(messages) 

	return []

def send_metrics(CWManager, metrics):
	CWManager.put_metric_data('Schoox', [
											{
												'MetricName': 'ExecutionTime',
												'Dimensions': [
													{
														'Name': 'AutoScalingGroup',
														'Value': 'ProductionWorkerVPC'
													},
													{
														'Name': 'QueueName',
														'Value': SqsManager.get_queue_name()
													},
												],
												'Timestamp'	: datetime.now(),
												'Unit'   	: 'Seconds',
												'StorageResolution' : 60,
												'StatisticValues': {
													'Maximum':metrics['max'],
													'Minimum':metrics['min'],
													'SampleCount':metrics['count'],
													'Sum':metrics['sum']
												}
											},
										])
	 

def calculate_metrics(metrics, messages, metrics_batch_max, force=false):
	"""Calculate message processing metrics"""

	for msg in messages:
		entryExec = message['ProcTime']
		metrics['max'] = entryExec if metrics['count'] == 0 else entryExec if metrics['max'] < entryExec else metrics['max']
		metrics['min'] = entryExec if metrics['count'] == 0 else entryExec if metrics['min'] > entryExec else metrics['min']
		metrics['count'] += 1
		metrics['sum']   += entryExec
	
		if $metrics['count'] == metrics_batch_max:
			send_metrics(CWManager, metrics)
			metrics = []

		logger.logging.info('Check metrics = {}' + " ".format(metrics))
	
	if force:
		send_metrics(CWManager, metrics)

	return metrics



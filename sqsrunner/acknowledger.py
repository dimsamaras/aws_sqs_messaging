# -*- coding: utf-8 -*-

import threading
import logger
from datetime import datetime, timedelta

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
	metrics = reset_metrics_dict()
	timer = datetime.now()
	timeAck = datetime.now()
	ackTimeout = int(thread.SqsManager.get_queue_visibility_timeout()) - 30
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
					# logger.logging.info('Will delete on stop = ' + " ".join(str(x) for x in delete_batch))
					delete_batch, metrics, timer = calculate_metrics(metrics, delete_batch, timer, thread.CWManager, thread.SqsManager, thread.metrics_batch_max)
					delete_batch = send_ack(thread.SqsManager, delete_batch)					

			logger.logging.info('Will delete on stop final remaining = ' + " ".join(str(x) for x in delete_batch))
			delete_batch, metrics, timer = calculate_metrics(metrics, delete_batch, timer, thread.CWManager, thread.SqsManager, thread.metrics_batch_max, True)
			delete_batch = send_ack(thread.SqsManager, delete_batch)
		elif (len(delete_batch) == thread.delete_batch_max) or (len(delete_batch)>0 and (datetime.now() - timeAck) > timedelta(seconds=ackTimeout)) :
			logger.logging.info('Batch delete = ' + " ".join(str(x) for x in delete_batch))
			delete_batch, metrics, timer  = calculate_metrics(metrics, delete_batch, timer, thread.CWManager, thread.SqsManager, thread.metrics_batch_max)
			delete_batch = send_ack(thread.SqsManager, delete_batch)
			timeAck = datetime.now()

def send_ack(SqsManager, messages):
	"""Send message acknowledgement  in batches."""
	if messages:
		SqsManager.delete_messages(messages) 

	return []

def send_metrics(CWManager, SqsManager, metrics):
	logger.logging.info('Metrics to send = ' + str(metrics))

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
	 
def calculate_metrics(metrics, messages, timer, CWManager, SqsManager, metrics_batch_max, force=False):
	"""Calculate message processing metrics"""

	for msg in messages:
		entryExec = msg['ProcTime']
		try:
			del msg['ProcTime']
		except KeyError:
			pass
		metrics['max'] = entryExec if metrics['count'] == 0 else entryExec if metrics['max'] < entryExec else metrics['max']
		metrics['min'] = entryExec if metrics['count'] == 0 else entryExec if metrics['min'] > entryExec else metrics['min']
		metrics['count'] += 1
		metrics['sum'] += entryExec
	
	elapsed = datetime.now() - timer

	logger.logging.info('Calculated metrics = ' + str(metrics))

	if (elapsed > timedelta(minutes=1)) and (metrics['count'] > 0):
		send_metrics(CWManager, SqsManager, metrics)
		metrics = reset_metrics_dict()

	if force and (metrics['count']>0):
		send_metrics(CWManager, SqsManager, metrics)

	return messages, metrics, timer 


def reset_metrics_dict():
	return {'max':0, 'sum':0, 'min':0, 'count':0}
# -*- coding: utf-8 -*-

import threading
import logger

class ackThread(threading.Thread):

	def __init__(self, threadID, SQS_MANAGER, CW_MANAGER, ackQueue, event):
		threading.Thread.__init__(self)
		self.threadID   		= threadID
		self.SqsManager   	    = SQS_MANAGER
		self.CWManager          = CW_MANAGER
		self.ackQueue   		= ackQueue
		# A flag to notify the thread that it should finish up and exit
		self.stopEvent  		= event

	def run(self):
		logger.logging.info('Starting acknowledger: %s', self.name)
		ack_messages(self)
		logger.logging.info('Exiting acknowledger: %s', self.name)
			
def ack_messages(thread):
	delete_batch = []
	while not thread.stopEvent.isSet():
		if not thread.ackQueue.empty():
			delete_batch.append(thread.ackQueue.get_nowait())
			thread.ackQueue.task_done()
		event_is_set = thread.stopEvent.wait(0.3)
		if event_is_set:
			logger.logging.info('stop received, messages in queue {messages} '.format(messages=thread.ackQueue.qsize()))
			while not thread.ackQueue.empty() or (len(delete_batch) > delete_batch_max) :
				delete_batch.append(thread.ackQueue.get_nowait())
				thread.ackQueue.task_done()

				if len(delete_batch) == delete_batch_max:
					logger.logging.info('Will delete on stop = ' + " ".join(str(x) for x in delete_batch))
					delete_batch = send_ack(thread.SqsManager, thread.CWManager, delete_batch)
			if delete_batch:
				logger.logging.info('Will delete on stop final remaining = ' + " ".join(str(x) for x in delete_batch))
				delete_batch = send_ack(thread.SqsManager, thread.CWManager, delete_batch)
		elif len(delete_batch) == delete_batch_max:
			logger.logging.info('Will delete = ' + " ".join(str(x) for x in delete_batch))
			delete_batch = send_ack(thread.SqsManager, thread.CWManager, delete_batch)

def send_ack(SqsManager, CWManager, messages):
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
									            'Value': len(messages)
									        },
									    ])
	SqsManager.delete_messages(messages)         
	return []

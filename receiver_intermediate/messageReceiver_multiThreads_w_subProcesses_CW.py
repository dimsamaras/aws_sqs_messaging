from subprocess import Popen, PIPE
import shlex
import boto3
import time
from datetime import datetime
import threading
from Queue import Queue
import logging

import config #config.py 
from sqs import SqsManager
from cloudwatch import CloudwatchManager

logging.basicConfig(level=logging.INFO,
					format='(%(threadName)-9s) %(message)s',)

env                 = 'DEV_3'
max_processes       = config.SQS_CONFIG[env]['max_processes']
max_q_messages      = config.SQS_CONFIG['general']['max_messages_received']
queue_name          = config.SQS_CONFIG[env]['queue_name']
endpoint_url        = config.SQS_CONFIG[env]['endpoint_url']
profile_name        = config.SQS_CONFIG[env]['profile_name']
region_name         = config.SQS_CONFIG[env]['region_name']
delete_batch_max    = config.SQS_CONFIG['general']['delete_batch_max']
delay_max           = config.SQS_CONFIG['general']['delay_max']

class workerThread(threading.Thread):

	def __init__(self, threadID, message, ackQueue):
		threading.Thread.__init__(self)
		self.threadID       = threadID
		self.message        = message
		self.ackQueue       = ackQueue

	def run(self):
		process_message(self)
		
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
		logging.info('Starting acknowledger: %s', self.name)
		ack_messages(self)
		logging.info('Exiting acknowledger: %s', self.name)

def process_message(thread):
	cmd = "php " + thread.message.body
	process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
	stdout, stderr = process.communicate()
	if (stderr):
		logging.info('Processing error, {id}, {body}, with error: {error}'.format(body=thread.message.body, id=thread.message.message_id, error= stderr))
	else:   
		logging.info('Processing ok, {body}'.format(body=thread.message.body))
		thread.ackQueue.put({'Id': thread.message.message_id, 'ReceiptHandle': thread.message.receipt_handle})
			
def ack_messages(thread):
	delete_batch = []
	while not thread.stopEvent.isSet():
		if not thread.ackQueue.empty():
			delete_batch.append(thread.ackQueue.get_nowait())
			thread.ackQueue.task_done()
		event_is_set = thread.stopEvent.wait(0.3)
		if event_is_set:
			logging.info('stop received, messages in queue {messages} '.format(messages=thread.ackQueue.qsize()))
			while not thread.ackQueue.empty() or (len(delete_batch) > delete_batch_max) :
				delete_batch.append(thread.ackQueue.get_nowait())
				thread.ackQueue.task_done()

				if len(delete_batch) == delete_batch_max:
					logging.info('Will delete on stop = ' + " ".join(str(x) for x in delete_batch))
					delete_batch = send_ack(thread.SqsManager, thread.CWManager, delete_batch)
			if delete_batch:
				logging.info('Will delete on stop final remaining = ' + " ".join(str(x) for x in delete_batch))
				delete_batch = send_ack(thread.SqsManager, thread.CWManager, delete_batch)
		elif len(delete_batch) == delete_batch_max:
			logging.info('Will delete = ' + " ".join(str(x) for x in delete_batch))
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

def main():
	session_cfg         = {}
	sqs_cfg             = {}
	if profile_name:
		session_cfg['profile_name'] = profile_name
	if region_name:
		session_cfg['region_name']  = region_name
	if endpoint_url:
		sqs_cfg['endpoint_url']     = endpoint_url  

	session             = boto3.Session(**session_cfg)
	SQS_MANAGER 		= SqsManager(session, sqs_cfg)
	SQS_MANAGER.get_queue(queue_name)
	CW_MANAGER			= CloudwatchManager(session)
	delay               = delay_max 
	ackQueue            = Queue(maxsize=0)
	nonPhpMessages      = []
	stopEvent = threading.Event()

	try:	
		a = ackThread('acknowledger', SQS_MANAGER, CW_MANAGER, ackQueue, stopEvent)
		a.daemon = True
		a.setName('acknowledger')
		a.start()

		while True:
			start       = time.time()
			messages    = SQS_MANAGER.receive_messages(max_q_messages, delay_max)

			logging.info('Received ' + str(len(messages)) + ' messages')
			for message in messages:
				args = shlex.split(message.body)
				if args[0].endswith(".php"): 
					logging.info('Running Threads ' + str(threading.active_count()))
					while threading.active_count() >= max_processes + 1:
						logging.info('Delaying for threads to get free 3 secs')
						time.sleep(2)
					t = workerThread(message.receipt_handle, message, ackQueue)
					t.daemon = True
					t.setName('worker ' + message.receipt_handle)
					t.start()
				else:
					nonPhpMessages.append({'Id': message.message_id, 'ReceiptHandle': message.receipt_handle})

					if len(nonPhpMessages) == delete_batch_max:
						logging.info('Will delete non php messages= ' + " ".join(str(x) for x in nonPhpMessages))
						SQS_MANAGER.delete_messages(nonPhpMessages)   
						nonPhpMessages = []     

			# delay = int(delay_max - (time.time() - start))
			# logging.info('Delaying ' + str(delay) + ' secs')
			# if delay>0:
			# 	time.sleep(delay)       

	except KeyboardInterrupt:
		logging.info("Ctrl-c received! Stop receiving...")
		
		# Wait for threads to complete
		# Filter out threads which have been joined or are None
		logging.info('Before join() on threads: threads={}'.format(threading.enumerate()))
		for t in threading.enumerate():
			if t.name.startswith('worker'):
				t.join()
		logging.info('After join() on threads: threads={}'.format(threading.enumerate()))

		logging.info('Close acknowledger thread: {}'.format(a))
		a.stopEvent.set()
		a.join()

		if nonPhpMessages:
			logging.info('Messages left by main' + str(len(nonPhpMessages)))
			SQS_MANAGER.delete_messages(nonPhpMessages)   
		
	logging.info('main() execution is now finished...')

if __name__ == '__main__':
	main()
 #    args = docopt.docopt(__doc__)
 #    src_queue_url = args['--src']
 #    dst_queue_url = args['--dst']



# https://www.bogotobogo.com/python/Multithread/python_multithreading_Event_Objects_between_Threads.php
# https://pythontic.com/multithreading/thread/join
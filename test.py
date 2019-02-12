from subprocess import Popen, PIPE
import multiprocessing
import shlex
import boto3
import time
import json
import config #config.py 
import threading
from Queue import Queue
import logging
import os 

logging.basicConfig(level=logging.INFO,
					format='(%(threadName)-9s) %(message)s',)

exitFlag            = 0
env                 = 'DEV'
max_processes       = config.SQS_CONFIG[env]['max_processes']
max_q_messages      = config.SQS_CONFIG[env]['max_messages_received']
queue_name          = config.SQS_CONFIG[env]['queue_name']
endpoint_url        = config.SQS_CONFIG[env]['endpoint_url']
profile_name        = config.SQS_CONFIG[env]['profile_name']
region_name         = config.SQS_CONFIG[env]['region_name']
delete_batch_max    = config.SQS_CONFIG['general']['delete_batch_max']
delay_max           = config.SQS_CONFIG['general']['delay_max']

class ackThread(threading.Thread):

	def __init__(self, threadID, QeueuResource, ackQueue, event):
		threading.Thread.__init__(self)
		self.threadID   		= threadID
		self.QeueuResource   	= QeueuResource
		self.ackQueue   		= ackQueue
		# A flag to notify the thread that it should finish up and exit
		self.stopEvent  		= event

	def run(self):
		logging.info('Starting: %s', self.name)
		ack_messages(self)
		logging.info('Exiting: %s', self.name)

def process_message(data):
	logging.info(str(os.getpid()) + ' working')
	message = data[0]
	message_id = data[1]
	message_receipt = data[2]
	ackQueue = data[3]

	logging.info('Got message = ' + str(message))

	cmd = "php " + message
	process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
	stdout, stderr = process.communicate()
	if (stderr):
		logging.info('Processing error, {id}, {body}, with error: {error}'.format(body=message, id=message_id, error= stderr))
	else:   
		logging.info('Processing ok')
		logging.info('Processed message = ' + str(message) + ' will sleep for 10 ')
 		time.sleep(10)
		ackQueue.put({'Id': message_id, 'ReceiptHandle': message_receipt})
			
def ack_messages(thread):
	delete_batch = []
	while not thread.stopEvent.isSet():
		if not thread.ackQueue.empty():
			delete_batch.append(thread.ackQueue.get_nowait())
			thread.ackQueue.task_done()
		event_is_set = thread.stopEvent.wait(0.3)
		if event_is_set:
			logging.info('stop received')
			while not thread.ackQueue.empty():
				delete_batch.append(thread.ackQueue.get_nowait())
				thread.ackQueue.task_done()

				if len(delete_batch) == delete_batch_max:
					delete_batch = send_ack(delete_batch, thread.QeueuResource )
			if delete_batch:
					delete_batch = send_ack(delete_batch, thread.QeueuResource)
		elif len(delete_batch) == delete_batch_max:
			delete_batch = send_ack(delete_batch, thread.QeueuResource)

def send_ack(messages, queueResource):
	logging.info('Will delete messages = ' + " ".join(str(x) for x in messages))
	queueResource.delete_messages(Entries=messages)         
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
	sqs                 = session.resource('sqs',**sqs_cfg)
	resourceQueue       = sqs.get_queue_by_name(QueueName=queue_name)
	delay               = delay_max 
	manager   			= multiprocessing.Manager()
	ackQueue            = manager.Queue(maxsize=10)
	stopEvent 			= threading.Event()

	try:	
		a = ackThread('acknowledger', resourceQueue, ackQueue, stopEvent)
		a.daemon = True
		a.setName('acknowledger')
		a.start()

		workerProcesses = multiprocessing.Pool(max_processes)

		while True:
			start       = time.time()
			messages    = resourceQueue.receive_messages(MaxNumberOfMessages=max_q_messages, WaitTimeSeconds=delay_max)

			logging.info('Received ' + str(len(messages)) + ' messages')
			data = [(message.body, message.message_id, message.receipt_handle, ackQueue) for message in messages]
			res = workerProcesses.map_async(process_message, data)
			
			if ackQueue.qsize() > max_processes/2:
			   time.sleep(delay)   

	except KeyboardInterrupt:
		logging.info("Ctrl-c received! Stop receiving...")
		
		# Wait for threads to complete
		# Filter out threads which have been joined or are None
		workerProcesses.close()
		workerProcesses.join()

		logging.info('Close acknowledger thread: {}'.format(a))
		a.stopEvent.set()
		a.join()

	logging.info('main() execution is now finished...')

if __name__ == '__main__':
	main()
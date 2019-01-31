from subprocess import Popen, PIPE
import shlex
import boto3
import time
import json
import config #config.py 
import threading
from Queue import Queue
import logging

logging.basicConfig(level=logging.DEBUG,
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

class workerThread(threading.Thread):

	def __init__(self, threadID, message, ackQueue):
		threading.Thread.__init__(self)
		self.threadID       = threadID
		self.message        = message
		self.ackQueue       = ackQueue

	def run(self):
		logging.debug('Starting: %s', self.message.message_id)
		process_message(self)
		logging.debug('Exiting: %s', self.message.message_id)
		
class ackThread(threading.Thread):

	def __init__(self, threadID, QeueuResource, ackQueue, event):
		threading.Thread.__init__(self)
		self.threadID   		= threadID
		self.QeueuResource   	= QeueuResource
		self.ackQueue   		= ackQueue
		# A flag to notify the thread that it should finish up and exit
		self.stopEvent  		= event

	def run(self):
		logging.debug('Starting: %s', self.name)
		ack_messages(self)
		logging.debug('Exiting: %s', self.name)

def process_message(thread):

	cmd = "php " + thread.message.body
	process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
	stdout, stderr = process.communicate()
	if (stderr):
		logging.debug('Processing error, {id}, {body}, with error: {error}'.format(body=thread.message.body, id=thread.message.message_id, error= stderr))
	else:   
		# logging.debug( "%s: %s" % (self.getName(), message.body))
		# logging.debug('Processing ok, out = ' + stdout)   
		logging.debug('Processing ok')
		thread.ackQueue.put({'Id': thread.message.message_id, 'ReceiptHandle': thread.message.receipt_handle})
			
def ack_messages(thread):
	delete_batch = []
	while not thread.stopEvent.isSet():
		delete_batch.append(thread.ackQueue.get())
		thread.ackQueue.task_done()
		event_is_set = thread.stopEvent.wait(1)
		if event_is_set:
			logging.debug('stop received')
			while not thread.ackQueue.empty():
				delete_batch.append(thread.ackQueue.get())
				thread.ackQueue.task_done()

				if len(delete_batch) == delete_batch_max:
					logging.debug('Will delete= ' + " ".join(str(x) for x in delete_batch))
					thread.QeueuResource.delete_messages(Entries=delete_batch) 
					delete_batch = []
			if delete_batch:
				logging.debug('Will delete remaining= ' + " ".join(str(x) for x in delete_batch))
				thread.QeueuResource.delete_messages(Entries=delete_batch)         
		elif len(delete_batch) == delete_batch_max:
			logging.debug('Will delete= ' + " ".join(str(x) for x in delete_batch))
			thread.QeueuResource.delete_messages(Entries=delete_batch) 
			delete_batch = []   

def main():
	session_cfg         = {}
	sqs_cfg             = {}
	if profile_name:
		session_cfg['profile_name'] = profile_name
	if region_name:
		session_cfg['region_name']  = region_name
	if endpoint_url:
		sqs_cfg['endpoint_url']     = endpoint_url  
	# session             = boto3.Session(**session_cfg)
	# sqs                 = session.resource('sqs',**sqs_cfg)
	# resourceQueue       = sqs.get_queue_by_name(QueueName=queue_name)
	resourceQueue		= []
	delay               = delay_max 
	ackQueue            = Queue(maxsize=0)
	nonPhpMessages      = []
	threads             = []
	stopEvent = threading.Event()

	try:	
		a = ackThread('acknowledger', resourceQueue, ackQueue, stopEvent)
		a.daemon = True
		a.setName('acknowledger')
		a.start()

		while True:
			start       = time.time()
			# messages    = resourceQueue.receive_messages(MaxNumberOfMessages=max_q_messages, WaitTimeSeconds=delay_max)

			# # logging.debug('Received ' + str(len(messages)) + ' messages')
			# for message in messages:
			# 	args = shlex.split(message.body)
			# 	if args[0].endswith(".php"): 
			# 		logging.debug('Running Threads ' + str(threading.active_count()))
			# 		while threading.active_count() >= max_processes + 1:
			# 			logging.debug('Delaying for threads to get free 3 secs')
			# 			time.sleep(3)
			# 		t = workerThread(message.receipt_handle, message, ackQueue)
			# 		t.daemon = True
			# 		threads.append(t)
			# 		t.setName('worker ' + message.receipt_handle)
			# 		t.start()
			# 	else:
			# 		nonPhpMessages.append({'Id': message.message_id, 'ReceiptHandle': message.receipt_handle})

			# 		if len(nonPhpMessages) == delete_batch_max:
			# 			logging.debug('Will delete non php messages= ' + " ".join(str(x) for x in nonPhpMessages))
			# 			resourceQueue.delete_messages(Entries=nonPhpMessages)   
			# 			nonPhpMessages = []     

			delay = int(delay_max - (time.time() - start))
			logging.debug('Delaying ' + str(delay) + ' secs')
			if delay>0:
				time.sleep(delay)       

	except KeyboardInterrupt:
		logging.debug("Ctrl-c received! Stop receiving...")
		
		# # Wait for threads to complete
		# # Filter out threads which have been joined or are None
		# logging.debug('Before join() on threads: threads={}'.format(threads))
		# threads = [t.join() for t in threads if t is not None and t.isAlive()]
		# logging.debug('After join() on threads: threads={}'.format(threads))

		logging.debug('Close acknowledger thread: {}'.format(a))
		a.stopEvent.set()
		a.join()

		if nonPhpMessages:
			logging.debug('Messages left by main' + str(len(nonPhpMessages)))
			resourceQueue.delete_messages(Entries=nonPhpMessages)   
		
		# EMpty the rest of the queue


	logging.debug('main() execution is now finished...')

if __name__ == '__main__':


 #    args = docopt.docopt(__doc__)
 #    src_queue_url = args['--src']
 #    dst_queue_url = args['--dst']

	main()


# https://www.bogotobogo.com/python/Multithread/python_multithreading_Event_Objects_between_Threads.php
# https://pythontic.com/multithreading/thread/join
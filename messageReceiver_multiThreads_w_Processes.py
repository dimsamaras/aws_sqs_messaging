from subprocess import Popen, PIPE
import shlex
import boto3
import time
import json
import config #config.py 
import threading
from Queue import Queue

exitFlag 			= 0
env             	= 'DEV'
max_processes   	= config.SQS_CONFIG[env]['max_processes']
max_q_messages  	= config.SQS_CONFIG[env]['max_messages_received']
queue_name      	= config.SQS_CONFIG[env]['queue_name']
endpoint_url    	= config.SQS_CONFIG[env]['endpoint_url']
profile_name    	= config.SQS_CONFIG[env]['profile_name']
region_name     	= config.SQS_CONFIG[env]['region_name']
delete_batch_max 	= config.SQS_CONFIG['general']['delete_batch_max']
delay_max		 	= config.SQS_CONFIG['general']['delay_max']

class workerThread(threading.Thread):

	def __init__(self, threadID, message, resource, ackQueue):
		threading.Thread.__init__(self)
		self.threadID 		= threadID
		self.message 		= message
		self.resource 		= resource
		self.ackQueue   	= ackQueue
		# A flag to notify the thread that it should finish up and exit
		self.kill_received 	= False

	def run(self):
		print "Starting " + self.message.message_id
		process_message(self, self.message, self.resource, self.ackQueue)
		print "Exiting " + self.message.message_id
		
class ackThread(threading.Thread):

	def __init__(self, threadID, resource, ackQueue):
		threading.Thread.__init__(self)
		self.threadID 	= threadID
		self.resource 	= resource
		self.ackQueue   = ackQueue
		# A flag to notify the thread that it should finish up and exit
		self.kill_received = False

	def run(self):
		print "Starting " + self.name
		ack_messages(self, self.resource, self.ackQueue)
		print "Exiting " + self.name

def process_message(thread, message, resourceQueue, ackQueue):

	cmd = "php " + message.body
	process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
	stdout, stderr = process.communicate()
	if (stderr):
		print('Processing error, {id}, {body}, with error: {error}'.format(body=message.body, id=message.message_id, error= stderr))
	else:	
		# print "%s: %s" % (self.getName(), message.body)
		# print('Processing ok, out = ' + stdout)	
		print('Processing ok')
		ackQueue.put({'Id': message.message_id, 'ReceiptHandle': message.receipt_handle})
			
def ack_messages(thread, resourceQueue, ackQueue):

	delete_batch = []

	while not thread.kill_received: # not ackQueue.empty():
		if 
	    delete_batch.append(ackQueue.get_nowait())
	    ackQueue.task_done()

	    if len(delete_batch) == delete_batch_max:
			print('Will delete= ' + " ".join(str(x) for x in delete_batch))
			resourceQueue.delete_messages(Entries=delete_batch)	
			delete_batch = []

		# if thread.kill_received:
		# 	print('Messages left by acknowledger ' + str(len(delete_batch)))
		# 	if delete_batch:
		# 		resourceQueue.delete_messages(Entries=delete_batch)
		# 	break		

def main():
	session_cfg     	= {}
	sqs_cfg         	= {}
	if profile_name:
		session_cfg['profile_name'] = profile_name
	if region_name:
		session_cfg['region_name'] 	= region_name
	if endpoint_url:
		sqs_cfg['endpoint_url'] 	= endpoint_url  
	session         	= boto3.Session(**session_cfg)
	sqs             	= session.resource('sqs',**sqs_cfg)
	resourceQueue       = sqs.get_queue_by_name(QueueName=queue_name)
	delay               = delay_max 
	ackQueue 			= Queue(maxsize=0)
	nonPhpMessages      = []
	threads 			= []

	try:
		a = ackThread('acknowledger', resourceQueue, ackQueue)
		a.daemon = True
		a.setName('acknowledger')
		a.start()

		while True:
			start 		= time.time()
			messages 	= resourceQueue.receive_messages(MaxNumberOfMessages=max_q_messages, WaitTimeSeconds=delay_max)

			print('Received ' + str(len(messages)) + ' messages')
			for message in messages:
				args = shlex.split(message.body)
				if args[0].endswith(".php"): 
					print('Running Threads ' + str(threading.active_count()))
					while threading.active_count() >= max_processes + 1:
						print('Delaying for threads to get free 3 secs')
						time.sleep(3)
					t = workerThread(message.receipt_handle, message, resourceQueue, ackQueue)
					t.daemon = True
					threads.append(t)
					t.setName(message.receipt_handle)
					t.start()
				else:
					nonPhpMessages.append({'Id': message.message_id, 'ReceiptHandle': message.receipt_handle})

					if len(nonPhpMessages) == delete_batch_max:
						print('Will delete non php messages= ' + " ".join(str(x) for x in nonPhpMessages))
						resourceQueue.delete_messages(Entries=nonPhpMessages)	
						nonPhpMessages = []		

			delay = int(delay_max - (time.time() - start))
			print('Delaying ' + str(delay) + ' secs')
			if delay>0:
				time.sleep(delay)		

	except KeyboardInterrupt:
		print("Ctrl-c received! Stop receiving...")
		
		a.kill_received = True

		# Wait for threads to complete
		# Filter out threads which have been joined or are None
		print('Before join() on threads: threads={}'.format(threads))
		threads = [t.join() for t in threads if t is not None and t.isAlive()]
		print('After join() on threads: threads={}'.format(threads))

		print('Close acknowledger thread: {}'.format(a))
		a.join()

		if nonPhpMessages:
			print('Messages left by main' + str(len(nonPhpMessages)))
			resourceQueue.delete_messages(Entries=nonPhpMessages)	
		
		# EMpty the rest of the queue


	print('main() execution is now finished...')

if __name__ == '__main__':


 #    args = docopt.docopt(__doc__)
 #    src_queue_url = args['--src']
 #    dst_queue_url = args['--dst']

	main()

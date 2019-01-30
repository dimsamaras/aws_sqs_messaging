from subprocess import Popen, PIPE
import shlex
import boto3
import time
import json
import config #config.py 
import threading

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
delete_batch        = []
threads 			= []

class workerThread(threading.Thread):

	def __init__(self, threadID, message, resource):
		threading.Thread.__init__(self)
		self.threadID 	= threadID
		self.message 	= message
		self.resource 	= resource
		# A flag to notify the thread that it should finish up and exit
		self.kill_received = False

	def run(self):
		print "Starting " + self.name
		process_message(self, self.getName(), self.message, self.resource)
		print "Exiting " + self.name
		threads.remove(self)
		

def process_message(thread, threadName, message, queue):
	global delete_batch 

	doRun = True if not thread.kill_received else False
	while doRun:
		cmd = "php " + message.body
		process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
		stdout, stderr = process.communicate()
		if (stderr):
			print('Processing error, {id}, {body}, with error: {error}'.format(body=message.body, id=message.message_id, error= stderr))
		else:	
			print "%s: %s" % (threadName, message.body)
			print('Processing ok, out = ' + stdout)	
			lock.acquire()

			delete_batch.append({'Id': message.message_id, 'ReceiptHandle': message.receipt_handle})
			print('Messages to delete= ' + str(len(delete_batch)))
			if len(delete_batch) == delete_batch_max:
				print('Will delete= ' + " ".join(str(x) for x in delete_batch))
				queue.delete_messages(Entries=delete_batch)	
				delete_batch = []

			lock.release()

		doRun = False
			

def main():
	global threads
	global delete_batch
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
	queue           	= sqs.get_queue_by_name(QueueName=queue_name)
	delay               = delay_max 

	try:
		while True:
			start 		= time.time()
			messages 	= queue.receive_messages(MaxNumberOfMessages=max_q_messages, WaitTimeSeconds=delay_max)

			print('Received ' + str(len(messages)) + ' messages')
			for message in messages:
				args = shlex.split(message.body)
				if args[0].endswith(".php"): 
					print('Running Threads ' + str(len(threads)))
					while len(threads) >= max_processes:
						time.sleep(2)
					t = workerThread(message.receipt_handle, message, queue)
					# lock.acquire()
					threads.append(t)
					# lock.acquire()
					t.setName(message.receipt_handle)
					t.start()
				else:
					# lock.acquire()
					delete_batch.append({'Id': message.message_id, 'ReceiptHandle': message.receipt_handle})
					# lock.release()

			delay = int(delay_max - (time.time() - start))
			if delay>0:
				time.sleep(delay)

	except KeyboardInterrupt:
		print("Ctrl-c received! Stop receiving...")
		
		for t in threads:
			t.kill_received = True

		# Wait for threads to complete
		# Filter out threads which have been joined or are None
		print('Before join() on threads: threads={}'.format(threads))
		threads = [t.join() for t in threads if t is not None and t.isAlive()]
		print('After join() on threads: threads={}'.format(threads))

		if delete_batch:
			print('Messages left ' + str(len(delete_batch)))
			queue.delete_messages(Entries=delete_batch)	
				

	print('main() execution is now finished...')

if __name__ == '__main__':


	args = docopt.docopt(__doc__)
    src_queue_url = args['--src']
    dst_queue_url = args['--dst']
    
	main()

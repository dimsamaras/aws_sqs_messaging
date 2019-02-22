from subprocess import Popen, PIPE
"""
Subprocess spawns new processes, but aside from stdin/stdout and whatever other APIs the other program may implement you have no means to communicate with them. Its main purpose is to launch processes that are completely separate from your own program.
Multiprocessing also spawns new processes, but they run your code, and are designed to communicate with each other. You use it to divide tasks within your own program across multiple CPU cores.
"""
import shlex
import boto3
import time
import json
import config #config.py 

env                 = 'DEV'
max_processes       = config.SQS_CONFIG[env]['max_processes']
max_q_messages      = config.SQS_CONFIG['general']['max_messages_received']
queue_name          = config.SQS_CONFIG[env]['queue_name']
endpoint_url        = config.SQS_CONFIG[env]['endpoint_url']
profile_name        = config.SQS_CONFIG[env]['profile_name']
region_name         = config.SQS_CONFIG[env]['region_name']
delete_batch_max    = config.SQS_CONFIG['general']['delete_batch_max']
delay_max           = config.SQS_CONFIG['general']['delay_max']

session_cfg     	= {}
if profile_name:
    session_cfg['profile_name'] = profile_name
if region_name:
    session_cfg['region_name'] = region_name

sqs_cfg         	= {}
if endpoint_url:
    sqs_cfg['endpoint_url'] = endpoint_url  

session         	= boto3.Session(**session_cfg)
sqs             	= session.resource('sqs',**sqs_cfg)
queue           	= sqs.get_queue_by_name(QueueName=queue_name)

def worker(cmd):
	pass

## Get messages until finished
delete_batch = []

while True:
	start = time.time()
	messages = queue.receive_messages(MaxNumberOfMessages=max_q_messages)
	print('Received ' + str(len(messages)) + ' messages')

	for message in messages:
		# print('{0}, {1}, {2}'.format(message.body, message.message_id, message.receipt_handle))
		args = shlex.split(message.body)
		if args[0].endswith(".php"): 
			cmd = "php " + message.body
			process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
			# while proc.returncode is None:
			# 	proc.poll()
			stdout, stderr = process.communicate()
			if (stderr):
				## Move to dead letter queue, with the stdError data as metadata!
				"""
				The message attributes of an SQS message are immutable once the message has been sent to the queue. The SQS Query API (used by all client libraries) has no support for modifying a message in the queue, other than to change its visibility timeout.
				"""
				# receipt = message.receipt_handle
				print('Processing error, {id}, {body}, with error: {error}'.format(body=message.body, id=message.message_id, error= stderr))
			else:	
				print('Processing ok, out = ' + stdout)	
				## Let the queue know that the message is processed
				delete_batch.append({'Id': message.message_id, 'ReceiptHandle': message.receipt_handle})
		else:
			delete_batch.append({'Id': message.message_id, 'ReceiptHandle': message.receipt_handle})

		if len(delete_batch) == delete_batch_max:
			queue.delete_messages(Entries=delete_batch)	

	delay = int(delay_max - (time.time() - start))
	time.sleep(delay)

if delete_batch:
    	queue.delete_messages(Entries=delete_batch)	


# #!/usr/bin/env python
# import os
# import subprocess
# from multiprocessing.pool import ThreadPool

# def run(i):
#     working_dir = "dir/Run/" + str(i + 1)
#     return i, subprocess.call(os.path.join(working_dir, 'mej'), cwd=working_dir)

# results = ThreadPool().map(run, range(n))		
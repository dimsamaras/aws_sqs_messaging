from subprocess import Popen, PIPE
"""
Subprocess spawns new processes, but aside from stdin/stdout and whatever other APIs the other program may implement you have no means to communicate with them. Its main purpose is to launch processes that are completely separate from your own program.
Multiprocessing also spawns new processes, but they run your code, and are designed to communicate with each other. You use it to divide tasks within your own program across multiple CPU cores.
"""
import shlex
import boto3
import time

import config #config.py confgiuration file

env             = 'DEV'
max_q_messages  = config.SQS_CONFIG[env]['max_messages_received']
queue_name      = config.SQS_CONFIG[env]['queue_name']
endpoint_url    = config.SQS_CONFIG[env]['endpoint_url']
profile_name    = config.SQS_CONFIG[env]['profile_name']
region_name     = config.SQS_CONFIG[env]['region_name']

session_cfg     = {}
if profile_name:
    session_cfg['profile_name'] = profile_name
if region_name:
    session_cfg['region_name'] = region_name

sqs_cfg         = {}
if endpoint_url:
    sqs_cfg['endpoint_url'] = endpoint_url  

session         = boto3.Session(**session_cfg)
sqs             = session.resource('sqs',**sqs_cfg)
queue           = sqs.get_queue_by_name(QueueName=queue_name)

# Get messages until finished
while True:
    messages  = queue.receive_messages(MaxNumberOfMessages = max_q_messages)
    for message in messages:
	    # Process messages by printing out body
		print('Hello, {0}'.format(message.body))
 		args = shlex.split(message.body)
 		if args[0].endswith(".php"): 
			cmd = "php " + message.body
			process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
			stdout, stderr = process.communicate()
			print("stdout = " + stdout)
			print("stderr = " + stderr)

			if (sterrd):
				# Move to dead letter queue, with the stdError data as metadata!
				

		# Let the queue know that the message is processed
		message.delete()
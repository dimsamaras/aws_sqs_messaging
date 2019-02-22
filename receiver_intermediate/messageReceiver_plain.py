import boto3
import time

import config #config.py confgiuration file

env             = 'DEV'
max_q_messages  = config.SQS_CONFIG['general']['max_messages_received']
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
while true:
    messages  = queue.receive_messages(MaxNumberOfMessages = max_q_messages)
    for message in messages:
	    # Process messages by printing out body
	    print('Hello, {0}'.format(message.body))

	    # Let the queue know that the message is processed
	    message.delete()
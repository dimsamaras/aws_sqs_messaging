import boto3
import time
import random

import config #config.py confgiuration file

env             = 'DEV'
queue_name      = config.SQS_CONFIG[env]['queue_name']
endpoint_url    = config.SQS_CONFIG[env]['endpoint_url']
profile_name    = config.SQS_CONFIG[env]['profile_name']
region_name     = config.SQS_CONFIG[env]['region_name']

session_cfg = {}
if profile_name:
    session_cfg['profile_name'] = profile_name
if region_name:
    session_cfg['region_name'] = region_name

sqs_cfg = {}
if endpoint_url:
    sqs_cfg['endpoint_url'] = endpoint_url  

session         = boto3.Session(**session_cfg)
sqs             = session.resource('sqs',**sqs_cfg)
queue           = sqs.get_queue_by_name(QueueName=queue_name)

start           = time.time()
for i in range(0,5):
    body = "/var/www/devscripts/dimsamQueueTest.php " + str(random.randint(0,3)) + " dimsam";
    if queue_name.endswith('.fifo'):
        response = queue.send_message(
            MessageBody=body,
            MessageGroupId='messageGroup'+str(random.randint(1, 4)) #Create different groups
            )
    else:
        response = queue.send_message(
            MessageBody=body
            )

    # The response is NOT a resource, but gives you a message ID and MD5
    print(response.get('MessageId'))
    # print(response.get('MessageGroupId'))
    print(response.get('MD5OfMessageBody'))


end             = time.time()
print ('Time started: ' + str(start) + '  and ended: ' + str(end) + '. Total time elapsed: ' + str(end - start))

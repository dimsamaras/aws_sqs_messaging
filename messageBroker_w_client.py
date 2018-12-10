import boto3
import time
import random

import config #config.py confgiuration file

queue_url = config.SQS_CONFIG['queue_url']
profile_name = config.SQS_CONFIG['profile_name']
region_name = config.SQS_CONFIG['region_name']

session = boto3.Session(profile_name = profile_name)
queue = session.client('sqs', region_name = region_name)

start = time.time()
for i in range(0,200):

    if queue_url.endswith('.fifo'):
        response = queue.send_message(QueueUrl=queue_url,
            MessageBody='Counting: ' + str(i),
            MessageGroupId='messageGroup'+str(random.randint(1, 4)) #Create different groups
            )
    else:
        response = queue.send_message(QueueUrl=queue_url,
            MessageBody='Counting: ' + str(i)
            )

    # The response is NOT a resource, but gives you a message ID and MD5
    print(response.get('MessageId'))
    # print(response.get('MessageGroupId'))
    print(response.get('MD5OfMessageBody'))


end = time.time()

print ('Time started: ' + str(start) + '  and ended: ' + str(end) + '. Total time elapsed: ' + str(end - start))

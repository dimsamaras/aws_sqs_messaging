import boto3
import time


# Get the service resource
qName = 'schoox2TestQ.fifo'
suffix = '.fifo'
region_name = 'us-west-2'
profile_name = 'schoox2'

session = boto3.Session(profile_name = profile_name)
sqs = session.resource('sqs', region_name = region_name)
# client = session.client('sqs', region_name = 'us-west-2')
# Get the queue
queue = sqs.get_queue_by_name(QueueName=qName)

start = time.time()
for i in range(0,100):

    if qName.endswith(suffix):
        response = queue.send_message(
            MessageBody='Counting: ' + str(i),
            MessageGroupId='messageGroup1' #Create different groups
            )
    else:
        response = queue.send_message(
            MessageBody='Counting: ' + str(i)
            )

    # The response is NOT a resource, but gives you a message ID and MD5
    print(response.get('MessageId'))
    # print(response.get('MessageGroupId'))
    print(response.get('MD5OfMessageBody'))


end = time.time()

print ('Time started: ' + str(start) + '  and ended: ' + str(end) + '. Total time elapsed: ' + str(end - start))

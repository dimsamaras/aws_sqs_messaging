import boto3
import time

session = boto3.Session(profile_name = 'schoox2')

# Get the service resource
sqs = session.resource('sqs', region_name = 'us-west-2')
client = session.client('sqs', region_name = 'us-west-2')
# Get the queue
queue = sqs.get_queue_by_name(QueueName='schoox2TestQ.fifo')

start = time.time()
# Get messages untill finished
all_messages=[]
messages  = queue.receive_messages(MaxNumberOfMessages = 10):
while len(messages)>10:
    all_messages.extend(messages)
    messages  = queue.receive_messages(MaxNumberOfMessages = 10):

end = time.time()

print ('Time started: ' + str(start) + '  and ended: ' + str(end) + '. Total time elapsed: ' + str(end - start))
# Process messages by printing out body
for message in all_messages:
    # Print out the body of the message
    print('Hello, {0}'.format(message.body))

    # Let the queue know that the message is processed
    message.delete()

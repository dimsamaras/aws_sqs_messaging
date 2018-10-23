import boto3
import threading
import time

exitFlag = 0
max_queue_messages = 10
region_name = 'us-west-2'
queue_name = 'schoox2TestQ.fifo'
suffix = '.fifo'
profile_name = 'schoox2'

class myThread (threading.Thread):import boto3
import threading
import os
import time
import sys

exitFlag = 0
max_queue_messages = 10
region_name = 'us-west-2'
queue_name = 'schoox2TestQ.fifo'
suffix = '.fifo'
profile_name = 'schoox2'

class myThread (threading.Thread):

    def __init__(self, threadID, delay, resource):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.delay = delay
        self.resource = resource
        # A flag to notify the thread that it should finish up and exit
        self.kill_received = False

    def run(self):
        print "Starting " + self.name
        get_the_messages(self,self.getName(), self.delay, self.resource)
        print "Exiting " + self.name

def get_the_messages(thread, threadName, delay, resourceQueue):
    session = boto3.Session(profile_name = profile_name)
    qClient = session.client('sqs', region_name = region_name)

    # Get the queue
    queueUrl = qClient.get_queue_url(QueueName=resourceQueue)
    queueUrl=queueUrl['QueueUrl']
    response  = qClient.receive_message(QueueUrl = queueUrl, MaxNumberOfMessages = max_queue_messages, AttributeNames=['ApproximateFirstReceiveTimestamp','SentTimestamp','MessageGroupId'])
    messages = response['Messages']

    print(messages)
    while not thread.kill_received:
        # TODO: resend msgs to queue
        if exitFlag:
           threadName.exit()
        Entries = []
        for message in messages:
            # Print out the body of the message
            print "%s: %s , %s" % (threadName, message['body'], message['message_id'])
            Entries.append({'Id': message['message_id'],'ReceiptHandle':message['ReceiptHandle']})
        # Let the queue know that the messages are processed in batches
        response = qClient.delete_message_batch(QueueUrl = queueUrl,Entries = messages)
        response  = qClient.receive_message(QueueUrl = queueUrl, MaxNumberOfMessages = max_queue_messages, AttributeNames=['ApproximateFirstReceiveTimestamp','SentTimestamp','MessageGroupId'])
        messages = response['Messages']

def main(args):
    threads = []
    for i in range(10):
        t = myThread(i, 1, queue_name)
        threads.append(t)
        t.setName('Thread-'+str(i))
        t.start()
        print('thread {} started'.format(i))

    print('Before joining')
    try:
        # threads = [t.join() for t in threads if t is not None and t.isAlive()]
        # print('After join() on threads: threads={}'.format(threads))
        while 1:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Ctrl-c received! Sending kill to threads...")
        for t in threads:
            t.kill_received = True
        # Join all thread
        # Filter out threads which have been joined or are None
        threads = [t.join() for t in threads if t is not None and t.isAlive()]
        print('After join() on threads: threads={}'.format(threads))
        threads =[]
    print('main() execution is now finished...')

if __name__ == '__main__':
    main(sys.argv)

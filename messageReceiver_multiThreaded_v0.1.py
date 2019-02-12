import boto3
import threading
import time

import config #config.py confgiuration file


exitFlag = 0
env                 = 'DEV'
max_processes       = config.SQS_CONFIG[env]['max_processes']
max_q_messages      = config.SQS_CONFIG['general']['max_messages_received']
queue_name          = config.SQS_CONFIG[env]['queue_name']
endpoint_url        = config.SQS_CONFIG[env]['endpoint_url']
profile_name        = config.SQS_CONFIG[env]['profile_name']
region_name         = config.SQS_CONFIG[env]['region_name']
delete_batch_max    = config.SQS_CONFIG['general']['delete_batch_max']
delay_max           = config.SQS_CONFIG['general']['delay_max']

class myThread (threading.Thread):

   def __init__(self, threadID, delay, resource):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.delay = delay
      self.resource = resource

   def run(self):
      print "Starting " + self.name
      get_the_messages(self.getName(), self.delay, self.resource)
      print "Exiting " + self.name

def get_the_messages(threadName, delay, resourceQueue):

    while 1:
        if exitFlag:
           threadName.exit()
        for message in messages:
            # Print out the body of the message
            print "%s: %s" % (threadName, message.body)
            # Let the queue know that the message is processed
            message.delete()

        # time.sleep(delay)
        messages  = resourceQueue.receive_messages(MaxNumberOfMessages = max_queue_messages)
    end = time.time()

    print ('Time started: ' + str(start) + '  and ended: ' + str(end) + '. Total time elapsed: ' + str(end - start))

session = boto3.Session(profile_name = profile_name)

# Get the service resource
sqs = session.resource('sqs', region_name = region_name)
# Get the service client
# client = session.client('sqs', region_name = region_name)

# Get the queue
queue = sqs.get_queue_by_name(QueueName=queue_name)
# queue2 = sqs.get_queue_by_name(QueueName=queue_name)

# Create new threads
thread1 = myThread(1, 1, queue)
thread2 = myThread(2, 2, queue)
thread3 = myThread(3, 4, queue)
thread4 = myThread(4, 8, queue)

# Start Threads
thread1.setName('Thread-1')
thread1.start()

thread2.setName('Thread-2')
thread2.start()

thread3.setName('Thread-3')
thread3.start()

thread4.setName('Thread-4')
thread4.start()

print "Exiting Main Thread"

import boto3
import threading
import os
import time
import sys

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
        # A flag to notify the thread that it should finish up and exit
        self.kill_received = False

    def run(self):
        print "Starting " + self.name
        get_the_messages(self,self.getName(), self.delay, self.resource)
        print "Exiting " + self.name

def get_the_messages(thread, threadName, delay, resourceQueue):
    # start = time.time()
    # Get messages untill finished
    messages  = resourceQueue.receive_messages(MaxNumberOfMessages = max_q_messages)
    # while len(messages) > 0:
    while not thread.kill_received:
        # TODO: resend msgs to queue
        if exitFlag:
           threadName.exit()
        for message in messages:
            # Print out the body of the message
            print "%s: %s" % (threadName, message.body)
            # Let the queue know that the message is processed
            message.delete()

        # time.sleep(delay)
        messages  = resourceQueue.receive_messages(MaxNumberOfMessages = max_q_messages)
    # end = time.time()
    # print ('Time started: ' + str(start) + '  and ended: ' + str(end) + '. Total time elapsed: ' + str(end - start))

def main(args):

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

    threads = []
    for i in range(4):
        t = myThread(i, 1, queue)
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

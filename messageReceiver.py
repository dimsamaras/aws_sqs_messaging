# -*- coding: utf-8 -*-

"""
Receive messages from sqs and process them parallely.
"""
import shlex
import boto3
import time
import threading
from Queue import Queue
import click
# custom dependencies
import config
import logger
from sqs import SqsManager
from cloudwatch import CloudwatchManager
from worker.worker import workerThread
from acknowledger.acknowledger import ackThread

# global variables
SQS_MANAGER = None
CW_MANAGER = None
MAX_PROCESSES = None
MAX_Q_MESSAGES = None
QUEUE = None
QUEUE_ENDPOINT = None
PROFILE = None
REGION_NAME = None
DELETE_BATCH_MAX = None
CW_BATCH_MAX = None
DELAY_MAX = None

@click.group()
@click.option('--env', default='DEV_3', help="Use an enviroment profile for the worker to run, DEV, DEV_2, DEV_3, DEV_4")
def cli(env):
	"""Worker consumes sqs messages"""

	global SQS_MANAGER, CW_MANAGER, MAX_PROCESSES, MAX_Q_MESSAGES, QUEUE, QUEUE_ENDPOINT, PROFILE, REGION_NAME, DELETE_BATCH_MAX,CW_BATCH_MAX, DELAY_MAX

	MAX_PROCESSES       = config.SQS_CONFIG[env]['max_processes']
	MAX_Q_MESSAGES      = config.SQS_CONFIG['general']['max_messages_received']
	QUEUE          		= config.SQS_CONFIG[env]['queue_name']
	QUEUE_ENDPOINT      = config.SQS_CONFIG[env]['endpoint_url']
	PROFILE       		= config.SQS_CONFIG[env]['profile_name']
	REGION_NAME         = config.SQS_CONFIG[env]['region_name']
	DELETE_BATCH_MAX    = config.SQS_CONFIG['general']['delete_batch_max']
	CW_BATCH_MAX		= config.SQS_CONFIG['general']['cloudwatch_metric_limit']
	DELAY_MAX           = config.SQS_CONFIG['general']['delay_max']

	session_cfg         = {}
	sqs_cfg             = {}
	if PROFILE:
		session_cfg['profile_name'] = PROFILE
	if REGION_NAME:
		session_cfg['region_name']  = REGION_NAME
	if QUEUE_ENDPOINT:
		sqs_cfg['endpoint_url']     = QUEUE_ENDPOINT  

	session             = boto3.Session(**session_cfg)
	SQS_MANAGER 		= SqsManager(session, sqs_cfg)
	SQS_MANAGER.get_queue(QUEUE)
	CW_MANAGER			= CloudwatchManager(session)

@cli.command('work')
def work():
	"""Worker execution logic"""
	global SQS_MANAGER, CW_MANAGER, MAX_PROCESSES, MAX_Q_MESSAGES, QUEUE, QUEUE_ENDPOINT, PROFILE, REGION_NAME, DELETE_BATCH_MAX,CW_BATCH_MAX, DELAY_MAX

	ackQueue = Queue(maxsize=0)
	nonPhpMessages = []
	stopEvent = threading.Event()

	try:	
		a = ackThread('acknowledger', SQS_MANAGER, CW_MANAGER, DELETE_BATCH_MAX,CW_BATCH_MAX, ackQueue, stopEvent)
		a.daemon = True
		a.setName('acknowledger')
		a.start()

		while True:
			start       = time.time()
			messages    = SQS_MANAGER.receive_messages(MAX_Q_MESSAGES, DELAY_MAX)

			logger.logging.info('Received ' + str(len(messages)) + ' messages')
			for message in messages:
				args = shlex.split(message.body)
				if args[0].endswith(".php"): 
					logger.logging.info('Running Threads ' + str(threading.active_count()))
					while threading.active_count() >= MAX_PROCESSES + 1:
						logger.logging.info('Delaying for threads to get free 3 secs')
						time.sleep(2)
					t = workerThread(message.receipt_handle, message, ackQueue)
					t.daemon = True
					t.setName('worker ' + message.receipt_handle)
					t.start()
				else:
					nonPhpMessages.append({'Id': message.message_id, 'ReceiptHandle': message.receipt_handle})

					if len(nonPhpMessages) == DELETE_BATCH_MAX:
						logger.logger.logging.info('Will delete non php messages= ' + " ".join(str(x) for x in nonPhpMessages))
						SQS_MANAGER.delete_messages(nonPhpMessages)   
						nonPhpMessages = []     

	except KeyboardInterrupt:
		logger.logging.info("Ctrl-c received! Stop receiving...")
		
		# Wait for threads to complete
		# Filter out threads which have been joined or are None
		logger.logging.info('Before join() on threads: threads={}'.format(threading.enumerate()))
		for t in threading.enumerate():
			if t.name.startswith('worker'):
				t.join()
		logger.logging.info('After join() on threads: threads={}'.format(threading.enumerate()))

		logger.logging.info('Close acknowledger thread: {}'.format(a))
		a.stopEvent.set()
		a.join()

		if nonPhpMessages:
			logger.logging.info('Messages left by main' + str(len(nonPhpMessages)))
			SQS_MANAGER.delete_messages(nonPhpMessages)   
		
	logger.logging.info('main() execution is now finished...')

@cli.command('info')
def info():	
	"""Worker congi enviroment info"""

	global MAX_PROCESSES, MAX_Q_MESSAGES, QUEUE, QUEUE_ENDPOINT, PROFILE, REGION_NAME, DELETE_BATCH_MAX, DELAY_MAX

	logger.logging.info('Enviroment: {env} setup:{setup}'.format(env=env, setup=[{'concurent processes':MAX_PROCESSES, 
		'max messages to receive': MAX_Q_MESSAGES,
		'queue name': QUEUE,
		'queue endopoint url': QUEUE_ENDPOINT,
		'queue delivery delay': DELAY_MAX,
		'max messages to acknowledge': DELETE_BATCH_MAX,
		'aws profile name': PROFILE,
		'aws region': REGION_NAME
		}]))

if __name__ == '__main__':
	cli()
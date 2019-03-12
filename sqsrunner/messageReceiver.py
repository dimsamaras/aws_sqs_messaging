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
import json
import signal
# custom dependencies
import sqsrunner.logger as logger
from sqsrunner.sqs import SqsManager
from sqsrunner.cloudwatch import CloudwatchManager
from sqsrunner.worker import workerThread
from sqsrunner.acknowledger import ackThread
from sqsrunner.gracefulkiller import GracefulKiller


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
@click.option('--config', required=True,  type=click.Path(exists=True), help="The configuration file")
@click.option('--env', required=True, help="Use an enviroment profile for the worker to run, e.g DEV, for the DEV object to be used")
def cli(config, env):
	"""Worker consumes sqs messages."""

	global  SQS_MANAGER, CW_MANAGER, MAX_PROCESSES, MAX_Q_MESSAGES, QUEUE, QUEUE_ENDPOINT, PROFILE, REGION_NAME, DELETE_BATCH_MAX,CW_BATCH_MAX, DELAY_MAX

	with open(config) as f:
		config = json.load(f)

	MAX_PROCESSES       = config['env'][env]['max_processes']
	MAX_Q_MESSAGES      = config['worker']['max_messages_received']
	QUEUE          		= config['env'][env]['queue_name']
	QUEUE_ENDPOINT      = config['env'][env]['endpoint_url']
	PROFILE       		= config['env'][env]['profile_name']
	REGION_NAME         = config['env'][env]['region_name']
	DELETE_BATCH_MAX    = config['worker']['delete_batch_max']
	CW_BATCH_MAX		= config['worker']['cloudwatch_metric_limit']
	DELAY_MAX           = config['worker']['delay_max']

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
	"""Worker executed. Consumes sqs messages, acknowledges and produces metric data."""
	global SQS_MANAGER, CW_MANAGER, MAX_PROCESSES, MAX_Q_MESSAGES, QUEUE, QUEUE_ENDPOINT, PROFILE, REGION_NAME, DELETE_BATCH_MAX,CW_BATCH_MAX, DELAY_MAX

	sighandler = GracefulKiller()
	ackQueue = Queue(maxsize=0)
	nonPhpMessages = []
	stopEvent = threading.Event()

	a = ackThread('acknowledger', SQS_MANAGER, CW_MANAGER, DELETE_BATCH_MAX,CW_BATCH_MAX, ackQueue, stopEvent)
	a.daemon = True
	a.setName('acknowledger')
	a.start()

	while True:
		if sighandler.receivedTermSignal:
			logger.logging.warning("Gracefully exiting due to receipt of signal {}".format(sighandler.lastSignal))
			signal_term_handler(nonPhpMessages)

		start       = time.time()
		messages    = SQS_MANAGER.receive_messages(MAX_Q_MESSAGES, DELAY_MAX)

		logger.logging.info('Received ' + str(len(messages)) + ' messages')
		for message in messages:
			args = shlex.split(message.body)
			if args[0].endswith(".php"): 
				logger.logging.info('Running Threads ' + str(threading.active_count()))
				while threading.active_count() >= MAX_PROCESSES + 1:
					logger.logging.info('Delaying for threads to get free 3 secs')
					time.sleep(3)
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

@cli.command('info')
def info():	
	"""Worker configured enviroment info."""

	global MAX_PROCESSES, MAX_Q_MESSAGES, QUEUE, QUEUE_ENDPOINT, PROFILE, REGION_NAME, DELETE_BATCH_MAX, DELAY_MAX

	logger.logging.info('Enviroment setup:{setup}'.format(setup=[{'concurent processes':MAX_PROCESSES, 
		'max messages to receive': MAX_Q_MESSAGES,
		'queue name': QUEUE,
		'queue endopoint url': QUEUE_ENDPOINT,
		'queue delivery delay': DELAY_MAX,
		'max messages to acknowledge': DELETE_BATCH_MAX,
		'aws profile name': PROFILE,
		'aws region': REGION_NAME
		}]))

def signal_term_handler(nonPhpMessages):
	logger.logging.info('Before join() on threads: threads={}'.format(threading.enumerate()))

	for t in threading.enumerate():
		if t.name.startswith('worker'):
			logger.logging.info('Close worker thread: {}'.format(t))  
			t.join()

	if nonPhpMessages:
		logger.logging.info('Messages left by main' + str(len(nonPhpMessages)))
		SQS_MANAGER.delete_messages(nonPhpMessages)  

	for a in threading.enumerate():
		if a.name.startswith('acknowledger'):
			logger.logging.info('Close acknowledger thread: {}'.format(a))  
			a.stopEvent.set()
			a.join()

	logger.logging.info('Done, After join() on threads: threads={}'.format(threading.enumerate()))
	exit()

if __name__ == '__main__':
	cli()
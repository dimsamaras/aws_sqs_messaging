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
import logging
import logging.handlers
from datetime import date
import hashlib

# custom dependencies
from sqsrunner.sqs import SqsManager
from sqsrunner.cloudwatch import CloudwatchManager
from sqsrunner.worker import workerThread
from sqsrunner.acknowledger import ackThread
from sqsrunner.gracefulkiller import GracefulKiller
from sqsrunner.assumerole import RoleManager
from sqsrunner.redisManager import RedisManager


# global variables
LOGGER = None
PROCESS_LOGGER = None
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
EXECUTOR = None
WORKING_DIR = None
LOG_DIRECTORY = None
LOG_LEVEL = 'DEBUG'
REDIS_MANAGER = None

@click.group()
@click.option('--config', required=True,  type=click.Path(exists=True), help="The configuration file")
@click.option('--env', required=True, help="Use an enviroment profile for the worker to run, e.g DEV, for the DEV object to be used")
def cli(config, env):
	"""Worker consumes sqs messages."""

	global REDIS_MANAGER, PROCESS_LOGGER, LOGGER, SQS_MANAGER, CW_MANAGER, MAX_PROCESSES, MAX_Q_MESSAGES, QUEUE, QUEUE_ENDPOINT, PROFILE, REGION_NAME, DELETE_BATCH_MAX,CW_BATCH_MAX, DELAY_MAX, EXECUTOR, WORKING_DIR, LOG_DIRECTORY, LOG_LEVEL

	with open(config) as f:
		config = json.load(f)

	MAX_PROCESSES       	= config['env'][env]['max_processes']
	MAX_Q_MESSAGES      	= config['worker']['max_messages_received']
	QUEUE          			= config['env'][env]['queue_name']
	QUEUE_ENDPOINT      	= config['env'][env]['endpoint_url']
	PROFILE       			= config['env'][env]['profile_name']
	REGION_NAME         	= config['env'][env]['region_name']
	DELETE_BATCH_MAX    	= config['worker']['delete_batch_max']
	CW_BATCH_MAX			= config['worker']['cloudwatch_metric_interval']
	DELAY_MAX           	= config['worker']['delay_max']
	EXECUTOR  				= config['env'][env]['executor']
	WORKING_DIR  			= config['env'][env]['working_dir']
	LOG_DIRECTORY  			= config['worker']['log_directory']
	LOG_LEVEL  				= config['worker']['log_level'].upper()
	LOG_ROLLOVER_WHEN   	= config['worker']['logging_rollover_when']
	LOG_ROLLOVER_INTERVAL 	= config['worker']['logging_rollover_interval']
	DUPLICATES_CACHING 		= config['env'][env]['clear_duplicates']

	logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")

	LOGGER = logging.getLogger('receiverLogger')
	fileHandler = logging.handlers.TimedRotatingFileHandler("{0}/{1}.log".format(LOG_DIRECTORY, env + '_worker'), LOG_ROLLOVER_WHEN, LOG_ROLLOVER_INTERVAL)
	fileHandler.setFormatter(logFormatter)
	fileHandler.setLevel(LOG_LEVEL)
	LOGGER.addHandler(fileHandler)

	PROCESS_LOGGER = logging.getLogger('processLogger')
	fileHandler_p = logging.handlers.TimedRotatingFileHandler("{0}/{1}.log".format(LOG_DIRECTORY, env + '_worker_processed'), LOG_ROLLOVER_WHEN, LOG_ROLLOVER_INTERVAL)
	fileHandler_p.setFormatter(logFormatter)
	PROCESS_LOGGER.addHandler(fileHandler_p)

	# consoleHandler = logging.StreamHandler()
	# consoleHandler.setFormatter(logFormatter)
	# LOGGER.addHandler(consoleHandler)

	session_cfg         = {}
	client_cfg          = {}
	if PROFILE:
		session_cfg['profile_name'] = PROFILE
	if REGION_NAME:
		session_cfg['region_name']  = REGION_NAME
	if QUEUE_ENDPOINT:
		client_cfg['endpoint_url']  = QUEUE_ENDPOINT 

	if not session_cfg:
		roleManager = RoleManager()
		tempCredentials = roleManager.get_credentials()
		session_cfg['aws_access_key_id'] = tempCredentials['AccessKeyId']
		session_cfg['aws_secret_access_key'] = tempCredentials['SecretAccessKey']
		session_cfg['aws_session_token'] = tempCredentials['SessionToken']

	session             = boto3.Session(**session_cfg)
	SQS_MANAGER 		= SqsManager(session, client_cfg)
	SQS_MANAGER.get_queue(QUEUE)
	CW_MANAGER			= CloudwatchManager(session, client_cfg)

	redis_cfg = {}
	if DUPLICATES_CACHING:
		if config['env'][env]['redis_host']:
			redis_cfg['host'] = config['env'][env]['redis_host']
		else:
			redis_cfg['host'] = '127.0.0.1'
		if config['env'][env]['redis_port']:
			redis_cfg['port'] = config['env'][env]['redis_port']
		else:
			redis_cfg['port'] = 6379	
		if config['env'][env]['redis_password']:
			redis_cfg['password'] = config['env'][env]['redis_password']

		REDIS_MANAGER = RedisManager(redis_cfg)		

@cli.command('work')
def work():
	"""Worker executed. Consumes sqs messages, acknowledges and produces metric data."""

	global QUEUE, REDIS_MANAGER, LOGGER, SQS_MANAGER, CW_MANAGER, MAX_PROCESSES, MAX_Q_MESSAGES, DELETE_BATCH_MAX, CW_BATCH_MAX, DELAY_MAX, EXECUTOR, WORKING_DIR

	sighandler = GracefulKiller()
	ackQueue = Queue(maxsize=0)
	nonExecutorMessages = [] # store messages that cannot be handled by the predefined process executor
	stopEvent = threading.Event()

	a = ackThread('acknowledger', SQS_MANAGER, CW_MANAGER, DELETE_BATCH_MAX, CW_BATCH_MAX, ackQueue, stopEvent)
	a.daemon = True
	a.setName('acknowledger')
	a.start()

	while True:
		if sighandler.receivedTermSignal:
			LOGGER.warning("Gracefully exiting due to receipt of signal {}".format(sighandler.lastSignal))
			signal_term_handler(nonExecutorMessages)

		start       = time.time()
		messages    = SQS_MANAGER.receive_messages(MAX_Q_MESSAGES, DELAY_MAX)

		LOGGER.info('Received ' + str(len(messages)) + ' messages')
		for message in messages:
			LOGGER.info('Running Threads ' + str(threading.active_count()))
			while threading.active_count() >= MAX_PROCESSES + 1:
				LOGGER.info('Delaying for threads to get free 3 secs')
				time.sleep(3)

			# add message to working set, remove from pending
			if REDIS_MANAGER:
				redis = REDIS_MANAGER.get_manager()
				command_digested = hashlib.md5(message.body).hexdigest()	
				redis.sadd(QUEUE + ":" + QUEUE + "_working", command_digested);
				redis.srem(QUEUE + ":" + QUEUE, command_digested)
				redis.hset(QUEUE + ":" + command_digested, 'time_arrived_to_worker', time.time());
			t = workerThread(message.receipt_handle, message, ackQueue, EXECUTOR, WORKING_DIR, QUEUE, redis)
			t.daemon = True
			t.setName('worker ' + message.receipt_handle)
			t.start()					

@cli.command('info')
def info():	
	"""Worker configured enviroment info."""
	global LOGGER, MAX_PROCESSES, MAX_Q_MESSAGES, QUEUE, QUEUE_ENDPOINT, PROFILE, REGION_NAME, DELETE_BATCH_MAX, DELAY_MAX, EXECUTOR, WORKING_DIR, LOG_DIRECTORY, LOG_LEVEL, CW_BATCH_MAX

	LOGGER.info('Enviroment setup:{setup}'.format(setup=[{
		'executor':EXECUTOR,
		'working directory':WORKING_DIR,
		'concurent processes':MAX_PROCESSES, 
		'max messages to receive': MAX_Q_MESSAGES,
		'queue name': QUEUE,
		'queue endopoint url': QUEUE_ENDPOINT,
		'queue delivery delay': DELAY_MAX,
		'max messages to acknowledge': DELETE_BATCH_MAX,
		'interval to send metrics': CW_BATCH_MAX,
		'aws profile name': PROFILE,
		'aws region': REGION_NAME,
		'logging directory': LOG_DIRECTORY,
		'logging severity level': LOGGER.getEffectiveLevel()
		}])
	)

def signal_term_handler(nonExecutorMessages):
	"""On signal termination."""
	global LOGGER

	LOGGER.info('Before join() on threads: threads={}'.format(threading.enumerate()))

	for t in threading.enumerate():
		if t.name.startswith('worker'):
			LOGGER.info('Close worker thread: {}'.format(t))
			t.join()

	if nonExecutorMessages:
		LOGGER.info('Messages left unprocessed by main' + str(len(nonExecutorMessages)))
		SQS_MANAGER.delete_messages(nonExecutorMessages)  

	for a in threading.enumerate():
		if a.name.startswith('acknowledger'):
			LOGGER.info('Close acknowledger thread: {}'.format(a))
			a.stopEvent.set()
			a.join()

	LOGGER.info('Done, After join() on threads: threads={}'.format(threading.enumerate()))
	exit()

if __name__ == '__main__':
	cli()
else:
	print __name__
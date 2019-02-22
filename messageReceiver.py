# -*- coding: utf-8 -*-

"""
Receive messages from sqs and process them parallely.
"""
import shlex
import boto3
import time
from datetime import datetime
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


SQS_MANAGER = None
CW_MANAGER = None

@click.group()
@click.option('--env', default='DEV_3', help="Use an enviroment profile for the worker to run, DEV, DEV_2, DEV_3, DEV_4")
def cli(env):
	"""Worker consumes sqs messages"""

	print env
	global SQS_MANAGER, CW_MANAGER

	max_processes       = config.SQS_CONFIG[env]['max_processes']
	max_q_messages      = config.SQS_CONFIG['general']['max_messages_received']
	queue_name          = config.SQS_CONFIG[env]['queue_name']
	endpoint_url        = config.SQS_CONFIG[env]['endpoint_url']
	profile_name        = config.SQS_CONFIG[env]['profile_name']
	region_name         = config.SQS_CONFIG[env]['region_name']
	delete_batch_max    = config.SQS_CONFIG['general']['delete_batch_max']
	delay_max           = config.SQS_CONFIG['general']['delay_max']

	session_cfg         = {}
	sqs_cfg             = {}
	if profile_name:
		session_cfg['profile_name'] = profile_name
	if region_name:
		session_cfg['region_name']  = region_name
	if endpoint_url:
		sqs_cfg['endpoint_url']     = endpoint_url  

	session             = boto3.Session(**session_cfg)
	SQS_MANAGER 		= SqsManager(session, sqs_cfg)
	SQS_MANAGER.get_queue(queue_name)
	CW_MANAGER			= CloudwatchManager(session)

@cli.command('work')
def work():
	"""Worker execution logic"""
	delay               = delay_max 
	ackQueue            = Queue(maxsize=0)
	nonPhpMessages      = []
	stopEvent = threading.Event()

	try:	
		a = ackThread('acknowledger', SQS_MANAGER, CW_MANAGER, ackQueue, stopEvent)
		a.daemon = True
		a.setName('acknowledger')
		a.start()

		while True:
			start       = time.time()
			messages    = SQS_MANAGER.receive_messages(max_q_messages, delay_max)

			logger.logging.info('Received ' + str(len(messages)) + ' messages')
			for message in messages:
				args = shlex.split(message.body)
				if args[0].endswith(".php"): 
					logger.logging.info('Running Threads ' + str(threading.active_count()))
					while threading.active_count() >= max_processes + 1:
						logger.logging.info('Delaying for threads to get free 3 secs')
						time.sleep(2)
					t = workerThread(message.receipt_handle, message, ackQueue)
					t.daemon = True
					t.setName('worker ' + message.receipt_handle)
					t.start()
				else:
					nonPhpMessages.append({'Id': message.message_id, 'ReceiptHandle': message.receipt_handle})

					if len(nonPhpMessages) == delete_batch_max:
						logger.logger.logging.info('Will delete non php messages= ' + " ".join(str(x) for x in nonPhpMessages))
						SQS_MANAGER.delete_messages(nonPhpMessages)   
						nonPhpMessages = []     

			# delay = int(delay_max - (time.time() - start))
			# logger.logging.info('Delaying ' + str(delay) + ' secs')
			# if delay>0:
			# 	time.sleep(delay)       

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
@click.argument('env')
def info(env):	
	"""Worker congi enviroment info"""

	max_processes       = config.SQS_CONFIG[env]['max_processes']
	max_q_messages      = config.SQS_CONFIG['general']['max_messages_received']
	queue_name          = config.SQS_CONFIG[env]['queue_name']
	endpoint_url        = config.SQS_CONFIG[env]['endpoint_url']
	profile_name        = config.SQS_CONFIG[env]['profile_name']
	region_name         = config.SQS_CONFIG[env]['region_name']
	delete_batch_max    = config.SQS_CONFIG['general']['delete_batch_max']
	delay_max           = config.SQS_CONFIG['general']['delay_max']

	logger.logging.info('Enviroment: {env} setup:{setup}'.format(env=env, setup=[{'max processes to be used':max_processes, 
		'max messages to receive': max_q_messages,
		'queue name': queue_name,
		'queue endopoint url': endpoint_url}]))

if __name__ == '__main__':
	cli()
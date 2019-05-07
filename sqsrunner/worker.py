# -*- coding: utf-8 -*-

import logging
import threading
from subprocess import Popen, PIPE
import time
from datetime import date
import json
import hashlib

logger = logging.getLogger('receiverLogger')
process_logger = logging.getLogger('processLogger')

class workerThread(threading.Thread):

	def __init__(self, threadID, message, ackQueue, executor, working_dir, queue,cache):
		"""
		threadId = thread id, 
		message = command for the worker to execute,
		ackQueue = queue used for the acknowledgement of the messages,
		executor = type of processes to work on,
		working_dir = working directory,
		cache = the messages are checked for duplicacy
		"""

		threading.Thread.__init__(self)
		self.threadID       = threadID
		self.message        = message
		self.ackQueue       = ackQueue
		self.executor		= executor
		self.working_dir	= working_dir
		self.queue  		= queue
		self.cache     		= cache

	def run(self):
		process_message(self)

def process_message(thread):
	"""Spawn ne wprocesses to execute commands."""

	if not thread.working_dir:
		thread.working_dir = "."

	cmd 			= thread.executor + " " + thread.message.body
	timeStarted 	= time.time()
	process 		= Popen(cmd, shell=True, cwd=thread.working_dir, stdout=PIPE, stderr=PIPE)
	stdout, stderr 	= process.communicate()
	rc 				= process.returncode
	timeDelta 		= time.time() - timeStarted

	if (stderr or rc != 0):
		logger.info('Processing error with return code: {rc}, {id}, {body}, with out: {out} and error: {error}'.format(rc=rc, body=thread.message.body, id=thread.message.message_id, out= stdout, error= stderr))
		# send this command to the dlq according to the redrive policy
		# dead letter queues must be set manually 
	else:   
		logger.info('Processing ok with return code: {rc}, {body}'.format(rc=rc, body=thread.message.body))
		thread.ackQueue.put({'Id': thread.message.message_id, 'ReceiptHandle': thread.message.receipt_handle, 'ProcTime': timeDelta})

	process_logger.info('Processed message with return code: {rc}, {body}'.format(rc=rc, body=json.dumps({'command':thread.message.body, 'executor':thread.executor, 'working_dir':thread.working_dir, 'output':stdout, 'error':stderr, 'execution_time':timeDelta})))

	if thread.cache :
		command_digested = hashlib.md5(thread.message.body).hexdigest()	
		# clear message to working set
		thread.cache.srem(thread.queue + ":" + thread.queue + "_working", command_digested)
		# remove command from queue
		thread.cache.delete(thread.queue + ":" + command_digested)
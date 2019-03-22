# -*- coding: utf-8 -*-

import logger
import threading
from subprocess import Popen, PIPE
import time
from datetime import date
import json

class workerThread(threading.Thread):

	def __init__(self, threadID, message, ackQueue, executor, working_dir):
		threading.Thread.__init__(self)
		self.threadID       = threadID
		self.message        = message
		self.ackQueue       = ackQueue
		self.executor		= executor
		self.working_dir	= working_dir

	def run(self):
		process_message(self)

def process_message(thread):
	"""Spawn ne wprocesses to execute commands."""

	if not thread.working_dir:
		thread.working_dir = "."
	cmd = thread.executor + " " + thread.message.body

	timeStarted = time.time() 

	process = Popen(cmd, shell=True, cwd=thread.working_dir, stdout=PIPE, stderr=PIPE)
	stdout, stderr = process.communicate()

	timeDelta = time.time() - timeStarted

	if (stderr or stdout):
		logger.logging.info('Processing error, {id}, {body}, with out: {out} and error: {error}'.format(body=thread.message.body, id=thread.message.message_id, out= stdout, error= stderr))
		# send this command to the dlq according to the redrive policy
		# dead letter queues must be set manually 
	else:   
		logger.logging.info('Processing ok, {body}'.format(body=thread.message.body))
		thread.ackQueue.put({'Id': thread.message.message_id, 'ReceiptHandle': thread.message.receipt_handle, 'ProcTime': timeDelta})

	log(json.dumps({'command':thread.message.body, 'executor':thread.executor, 'working_dir':thread.working_dir, 'output':stdout, 'error':stderr, 'execution_time':timeDelta}))	

def log(dump):
	"""Log the command execution."""

	today 	 = str(date.today())
	filename = '/var/log/'+today+'_worker.log'

	with open(filename, "a") as f:
		f.write(dump + " \n")	
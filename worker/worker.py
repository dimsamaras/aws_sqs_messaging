# -*- coding: utf-8 -*-

import logger
import threading
from subprocess import Popen, PIPE
import time

class workerThread(threading.Thread):

	def __init__(self, threadID, message, ackQueue):
		threading.Thread.__init__(self)
		self.threadID       = threadID
		self.message        = message
		self.ackQueue       = ackQueue

	def run(self):
		process_message(self)

def process_message(thread):
	cmd = "php " + thread.message.body
	timeStarted = time.time() 
	process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
	stdout, stderr = process.communicate()
	timeDelta = time.time() - timeStarted
	if (stderr):
		logger.logging.info('Processing error, {id}, {body}, with error: {error}'.format(body=thread.message.body, id=thread.message.message_id, error= stderr))
	else:   
		logger.logging.info('Processing ok, {body}'.format(body=thread.message.body))
		thread.ackQueue.put({'Id': thread.message.message_id, 'ReceiptHandle': thread.message.receipt_handle, 'ProcTime': timeDelta})
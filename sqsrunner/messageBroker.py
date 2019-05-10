import boto3
import time
import random
import click
import json
from sqsrunner.sqs import SqsManager
import logging
from sqsrunner.assumerole import RoleManager
import hashlib
from sqsrunner.redisManager import RedisManager

# global variables
SQS_MANAGER = None
QUEUE = None
QUEUE_ENDPOINT = None
PROFILE = None
REGION_NAME = None
REDIS_MANAGER = None

@click.group()
@click.option('--config', required=True, type=click.Path(exists=True), help="The configuration file")
@click.option('--env', required=True, help="Use an enviroment profile for the worker to run, e.g DEV, for the DEV object to be used")
def cli(config, env):
	"""Worker creates sqs messages."""

	global REDIS_MANAGER, SQS_MANAGER, QUEUE, QUEUE_ENDPOINT, PROFILE, REGION_NAME

	with open(config) as f:
		config = json.load(f)

	QUEUE               = config['env'][env]['queue_name']
	QUEUE_ENDPOINT      = config['env'][env]['endpoint_url']
	PROFILE             = config['env'][env]['profile_name']
	REGION_NAME         = config['env'][env]['region_name']
	DUPLICATES_CACHING 		= config['env'][env]['clear_duplicates']

	session_cfg         = {}
	sqs_cfg             = {}
	if PROFILE:
		session_cfg['profile_name'] = PROFILE
	if REGION_NAME:
		session_cfg['region_name']  = REGION_NAME
	if QUEUE_ENDPOINT:
		sqs_cfg['endpoint_url']     = QUEUE_ENDPOINT  

	if not session_cfg:
		roleManager = RoleManager()
		tempCredentials = roleManager.get_credentials()
		session_cfg['aws_access_key_id'] = tempCredentials['AccessKeyId']
		session_cfg['aws_secret_access_key'] = tempCredentials['SecretAccessKey']
		session_cfg['aws_session_token'] = tempCredentials['SessionToken']

	session             = boto3.Session(**session_cfg)
	SQS_MANAGER         = SqsManager(session, sqs_cfg)
	SQS_MANAGER.get_queue(QUEUE)

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
	"""Worker executed. Create new messages."""

	global REDIS_MANAGER, SQS_MANAGER, QUEUE

	start = time.time()

	# for i in range(0,50):
	#     body = "/var/www/devscripts/dimsamQueueTest.php " + str(random.randint(0,5)) + " dimsam";
	#     if queue_name.endswith('.fifo'):
	#         response = SQS_MANAGER.send_message(
	#             MessageBody=body,
	#             MessageGroupId='messageGroup'+str(random.randint(1, 4)) #Create different groups
	#             )
	#     else:
	#         response = SQS_MANAGER.send_message(
	#             MessageBody=body
	#             )

	#     # The response is NOT a resource, but gives you a message ID and MD5
	#     print(response.get('MessageId'))
	#     # print(response.get('MessageGroupId'))
	#     print(response.get('MD5OfMessageBody'))

	messages = []
	for i in range(0, 10):
		body = (
			"/var/www/devscripts/dimsamQueueTest.php "
			+ str(random.randint(0, 10))
			+ " dimsam"
		)
		if QUEUE.endswith(".fifo"):
			messages.append(
				{
					"Id": "randId_" + str(i),
					"MessageBody": body,
					"MessageGroupId": "messageGroup"
					+ str(random.randint(1, 4)),  # Create different groups
				}
			)
		else:
			messages.append({"Id": "randId_" + str(i), "MessageBody": body})

		if REDIS_MANAGER:
				redis = REDIS_MANAGER.get_manager()
				document = {
                        'message'           		: body,
                        'time_inserted_to_queue'	: time.time()
                    }
				command_digested = hashlib.md5(body).hexdigest()	
				redis.hmset(QUEUE + ":" + command_digested, document)
				redis.sadd(QUEUE + ":" + QUEUE, command_digested);
					
		if len(messages) == 10:
			response = SQS_MANAGER.send_messages(messages)
			print(response)
			messages = []

	# the remaining
	if messages:
		response = SQS_MANAGER.send_messages(messages)
		print(response)

	end = time.time()
	print(
		"Time started: "
		+ str(start)
		+ "  and ended: "
		+ str(end)
		+ ". Total time elapsed: "
		+ str(end - start)
	)

@cli.command('info')
def info(): 
	"""Worker congi enviroment info"""

	global QUEUE, QUEUE_ENDPOINT, PROFILE, REGION_NAME

	logging.info('Enviroment setup:{setup}'.format(setup=[{
		'queue name': QUEUE,
		'queue endopoint url': QUEUE_ENDPOINT,
		'aws profile name': PROFILE,
		'aws region': REGION_NAME
		}]))

if __name__ == '__main__':

	logging.basicConfig(level=logging.INFO,
					format='(%(threadName)-9s) %(message)s',)

	cli()
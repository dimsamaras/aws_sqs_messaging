# SEND AND RECEIVE MESSAGES TO SQS

python modules using boto3 and click libraries as dependencies.

# USAGE
	1. Create messages:
		```python -m sqsrunner.messageBroker --config config.json --env DEV work```

	2. Receive messages:
		```python -m sqsrunner.messageReceiver --config config.json --env DEV work```

	--config option accepts a file path with the configuration .json. Follow the config.json.example
	--env option accepts the enviroment object	
	
	* boto3 consumes either profile and credential definitions from aws configuration OR can assume role

## THE CONFIG 
	1. copy config.json.example to config.json
	2. Name your environments
	3. Fill the blanks:
	```
			"description": "Environament description",
			"executor":"<What processes are you going to consume : php, php56 php72 /usr/local/bin/php, python>",
			"working_dir":"<The project working directory path, is the commands path is relative>",		
			"queue_name": "<The queue name>",
			"profile_name": "<profile, If empty it will try to assume role for instance>",
			"region_name": "<profile_region, If empty it will try to assume role for instance>",
			"endpoint_url": "<The endopoint url>",
			"max_processes": Int, Conqurent processes
	```

	Use example for more 
## MESSAGE BROKERS
* messageBroker.py _sqs message broker using the resource_

## MESSAGE RECEIVERS
* messageReceiver.py _sqs message php consumer._

## MESSAGE RECEIVERS intermediate steps
* messageReceiver_multiProcesses_v0.1.py _multitprocessing implementation of the receiver_
* messageReceiver_multiProcesses.py _multitprocessing implementation of the receiver. Catched keyboard interaptions_
* messageReceiver_multiThreaded.py _multithreaded implementation using resource_
* messageReceiver_multiThreaded_w_client.py _multithreaded implementation using client_
* messageReceiver_multiThreaded_w_Processes.py _subprocesses run through threads in order to manage the messages flow_
and more
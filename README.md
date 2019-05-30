# SEND AND RECEIVE MESSAGES TO SQS

python modules using boto3 and click libraries as dependencies.

# USAGE
### 1. Create messages :
```
python -m sqsrunner.messageBroker --config config.json --env DEV work
```
### Default:
```
python -m sqsrunner.messageBroker --config config.json --env DEV work
```

### 2. Receive messages:
```
python -m sqsrunner.messageReceiver --config config.json --env DEV work
```

	--config option accepts a file path with the configuration .json. Follow the config.json.example
	--env option accepts the enviroment object	
	
	*note: boto3 consumes either profile and credential definitions from aws configuration OR can assume role

## THE CONFIG 

'env' object sets the consumer parameters, from where it will get the messages and how to execute them, also the parallelization of the execution

'worker' object sets the worker parameters, how many messages to receive woth everry call, how to acknoledge them and how to log everything.
	
### 1. copy config.json.example to config.json
### 2. Name your environments
### 3. Fill the blanks and/or choose one form the options inide < >:
```
"env": {
	"DEV": {
		"description": "use when running on development environment ie. from within the container",
		"executor":"php",	
		"working_dir":"",		
		"queue_name": "local_queue",
		"profile_name": "default",
		"region_name": "us-east-1",
		"endpoint_url": "http://localstack:4576",
		"max_processes": 2,
		"clear_duplicates":false,
		"redis_host":Choose one accordingly:<"redis","localhost","127.0.0.1">,
		"redis_port":6379,
		"redis_password":""
	},
	"PRODUCTION": {
		"description": "use when running for aws queue",
		"executor":Choose one accordingly:"<php, php56 php72 /usr/local/bin/php, python>",
		"working_dir":Setup your working dir:"<working directory path>",		
		"queue_name": "demo_queue",
		"profile_name": "",
		"region_name": "",
		"endpoint_url": "",
		"max_processes": 10,
		"clear_duplicates":If you have message caching in your code(Schoox project specific):<true,false>,
		"redis_host":Choose one accordingly:<"redis","localhost","127.0.0.1">,
		"redis_port":6379,
		"redis_password":""
	}
	},
"worker": {
	"max_messages_received": 10,
	"delete_batch_max": 10,
	"delay_max": 20,
	"cloudwatch_metric_limit": 100,
	"cloudwatch_metric_interval": 5,
	"log_directory":"/var/log/",
	"log_level":Choose a logging level:<"DEBUG","INFO","WARNING","ERROR","CRITICAL">
	"logging_rollover_when":Choose accordingly:<'S' Seconds,'M' Minutes,'H' Hours,'D' Days,'midnight'>,
	"logging_rollover_interval": 5
}
```

	
## MESSAGE BROKERS
* messageBroker.py _sqs message broker using the resource_

## MESSAGE RECEIVERS
* messageReceiver.py _sqs message php consumer._

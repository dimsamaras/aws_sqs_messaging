import boto3
import time
import random
import click
from sqsrunner.sqs import SqsManager
import sqsrunner.config as config # config.py configuration file

# global variables
SQS_MANAGER = None
QUEUE = None
QUEUE_ENDPOINT = None
PROFILE = None
REGION_NAME = None

@click.group()
@click.option('--env', default='DEV_3', help="Use an enviroment profile for the worker to run, DEV, DEV_2, DEV_3, DEV_4")
def cli(env):
    global SQS_MANAGER, QUEUE, QUEUE_ENDPOINT, PROFILE, REGION_NAME

    QUEUE               = config.SQS_CONFIG[env]['queue_name']
    QUEUE_ENDPOINT      = config.SQS_CONFIG[env]['endpoint_url']
    PROFILE             = config.SQS_CONFIG[env]['profile_name']
    REGION_NAME         = config.SQS_CONFIG[env]['region_name']

    session_cfg         = {}
    sqs_cfg             = {}
    if PROFILE:
        session_cfg['profile_name'] = PROFILE
    if REGION_NAME:
        session_cfg['region_name']  = REGION_NAME
    if QUEUE_ENDPOINT:
        sqs_cfg['endpoint_url']     = QUEUE_ENDPOINT  

    session             = boto3.Session(**session_cfg)
    SQS_MANAGER         = SqsManager(session, sqs_cfg)
    SQS_MANAGER.get_queue(QUEUE)


@cli.command('work')
def work(): 

    global SQS_MANAGER, QUEUE

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
    for i in range(0, 100):
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

        if len(messages) == 10:
            response = SQS_MANAGER.send_messages(messages)
            print(response)
            messages = []

    # the remaining
    if messages:
        response = SQS_MANAGER.send_messages(messages)
        print(response)

    # for i in range(0,5):
    #     body = "/var/www/devscripts/dimsamQueueTestFail2.php " + str(random.randint(0,3)) + " dimsam";
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

    # for i in range(0,5):
    #     body = "/var/www/devscripts/dimsamQueueTest.py " + str(random.randint(0,3)) + " dimsam";
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

    logger.logging.info('Enviroment: {env} setup:{setup}'.format(env=env, setup=[{
        'queue name': QUEUE,
        'queue endopoint url': QUEUE_ENDPOINT,
        'aws profile name': PROFILE,
        'aws region': REGION_NAME
        }]))

if __name__ == '__main__':
    cli()
import os 
import boto3

# https://docs.aws.amazon.com/code-samples/latest/catalog/python-sts-assume_role.py.html

class RoleManager:
	""" Assume role class """

	def __init__(self):
		self.awsrole = os.environ['AWS_ASSUME_ROLE']
		self.awskey  = os.environ['AWS_KEY_ID']
		self.awssecret = os.environ['AWS_KEY_SECRET']
		self.credentials = {}

	def get_credentials(self):

		if self.awsrole:
			sts_client = boto3.client('sts')

			assumed_role_object=sts_client.assume_role(
			    RoleArn=self.awsrole,
			    RoleSessionName="AssumeRoleSessionWorker"
			)

			self.credentials = assumed_role_object['Credentials']

			return self.credentials
		else:
			return {}	

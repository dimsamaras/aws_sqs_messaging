import os 
import boto3

# https://docs.aws.amazon.com/code-samples/latest/catalog/python-sts-assume_role.py.html

class RoleManager:
	""" Assume role class """

	def __init__(self):
		self.awsrole = os.environ['AWS_ASSUME_ROLE']
		self.awskey  = os.environ['AWS_KEY_ID']
		self.awssecret = os.environ['AWS_KEY_SECRET']
		self.credentials = None

	def get_credentials(self):

		if self.awsrole and self.awskey and self.awssecret:
			sts_client = boto3.client('sts')

			assumed_role_object=sts_client.assume_role(
			    RoleArn=self.awsrole,
			    RoleSessionName="AssumeRoleSessionWorker"
			)

		self.credentials = assumed_role_object['Credentials']

		return self.credentials

	# # Use the temporary credentials that AssumeRole returns to make a 
	# # connection to Amazon S3  
	# s3_resource=boto3.resource(
	#     's3',
	#     aws_access_key_id=credentials['AccessKeyId'],
	#     aws_secret_access_key=credentials['SecretAccessKey'],
	#     aws_session_token=credentials['SessionToken'],
	# )

	# # Use the Amazon S3 resource object that is now configured with the 
	# # credentials to access your S3 buckets. 
	# for bucket in s3_resource.buckets.all():
	#     print(bucket.name)
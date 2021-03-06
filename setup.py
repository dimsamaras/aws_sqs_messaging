from setuptools import setup

setup(
	name='schooxworker',
	version='0.10.2',
	author='Dimitris Samaras',
	author_email='',
	description='This is tool to read and execute php commands served from sqs',
	licence='GPLv3+',
	packages=['sqsrunner'],
	url='https://github.com/dimsamaras/aws_sqs_messaging',
	install_requires=[
		'click',
		'boto3',
		'redis'
	],

	entry_points="""
		[console_scripts]
		sqsrunner=sqsrunner.messageReceiver:cli
	"""
	)


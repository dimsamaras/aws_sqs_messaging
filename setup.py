from setuptools import setup

setup(
	name='schooxworker',
	version='0.1',
	author='Dimitris Samaras',
	author_email='',
	description='This is tool to read and execute php commands served from sqs',
	licene='GPLv3+',
	packages=['sqsrunner'],
	url='https://github.com/dimsamaras/aws_sqs_messaging',
	install_requieres=[
		'click',
		'boto3'
	],

	entry_points='''
		[console_scripts]
		sqsrunner=sqsrunner.messageReceiver:cli
	'''
	)


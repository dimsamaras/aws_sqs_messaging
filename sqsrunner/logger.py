import logging
# import sqsrunner.settings as settings
# from datetime import date

logging.basicConfig(level=logging.INFO,
					format='(%(threadName)-9s) %(message)s',)

# today 	 = str(date.today())
# filename = '/var/log/' + settings.settingsDict['worker']['env'] + '_' + today + '_worker.log'

# logging.basicConfig(filename=filename,
# 					level=logging.DEBUG,
# 					format='(%(levelname)s: %(asctime)s %(threadName)-9s) %(message)s',)

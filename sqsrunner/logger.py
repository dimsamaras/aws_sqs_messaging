import logging
from datetime import date

today 	 = str(date.today())
filename = '/var/log/' + today + '_worker.log'

logging.basicConfig(filename=filename,
					level=logging.INFO,
					format='(%(levelname)s: %(asctime)s %(threadName)-9s) %(message)s',)

import logging
import redis

logger = logging.getLogger('receiverLogger')

class RedisManager:
	"""Redis client class."""

	"""Create a Redis client object."""
	def __init__(self,  cfg):
		try:
			self.conn = redis.StrictRedis(**cfg)
			self.conn.ping()
		except Exception as ex:
			logger.error("Unexpected error connecting to Redis: %s" % ex)

	def get_manager(self):
		return self.conn
			
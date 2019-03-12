import signal
import sqsrunner.logger as logger
import threading

class GracefulKillerSimple:
  receivedTermSignal = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self,signum, frame):
    self.receivedTermSignal = True

class GracefulKiller:
    """Catch signals to allow graceful shutdown."""

    def __init__(self):
        self.receivedSignal = self.receivedTermSignal = False
        catchSignals = [
            1,
            2,
            3,
            10,
            12,
            15,
        ]
        for signum in catchSignals:
            signal.signal(signum, self.handler)

    def handler(self, signum, frame):
        self.lastSignal = signum
        self.receivedSignal = True
        if signum in [2, 3, 15]:
            self.receivedTermSignal = True
            

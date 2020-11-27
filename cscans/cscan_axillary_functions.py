'''
This file contains axillary functions for continuous scans scripts.

Author yury.matveev@desy.de
'''

import time
import threading
import sys

from cscan_constants import *

# ----------------------------------------------------------------------
#
# ----------------------------------------------------------------------

class EndMeasurementBarrier():

    def __init__(self, n_workers):
        self._n_reported_workers = 0
        self._n_workers = n_workers

    def wait(self):
        while self._n_reported_workers < self._n_workers:
            time.sleep(REFRESH_PERIOD)
        self._n_reported_workers = 0

    def report(self):
        self._n_reported_workers += 1

# ----------------------------------------------------------------------
class ExcThread(threading.Thread):

    # ----------------------------------------------------------------------
    def __init__(self, target, threadname, errorBucket,  *args):
        threading.Thread.__init__(self, target=target, name=threadname, args=args)
        self._stop_event = threading.Event()
        self.bucket = errorBucket
        self._name = threadname

    # ----------------------------------------------------------------------
    def run(self):
        try:
            if hasattr(self, '_Thread__target'):
                self.ret = self._Thread__target(*self._Thread__args, **self._Thread__kwargs)
            else:
                self.ret = self._target(*self._args, **self._kwargs)
        except Exception as exp:
            # traceback.print_tb(sys.exc_info()[2])
            self.bucket.put([self._name, sys.exc_info()[0], sys.exc_info()[2]])

    # ----------------------------------------------------------------------
    def stop(self):
        self._stop_event.set()

    # ----------------------------------------------------------------------
    def stopped(self):
        return self._stop_event.isSet()
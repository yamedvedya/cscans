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
        self._n_workers = n_workers
        self._workers_report = [0 for _ in range(n_workers)]

    # ----------------------------------------------------------------------
    def _check_barrier(self):
        prod = 1
        for value in self._workers_report:
            prod *= value
        return prod

    # ----------------------------------------------------------------------
    def wait(self):
        while not self._check_barrier():
            time.sleep(REFRESH_PERIOD)
        self._workers_report = [0 for _ in range(self._n_workers)]

    # ----------------------------------------------------------------------
    def report(self, ind):
        self._workers_report[ind] = 1

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
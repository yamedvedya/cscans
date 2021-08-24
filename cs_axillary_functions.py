'''
This file contains axillary cscans for continuous scans scripts.

Author yury.matveev@desy.de
'''

import time
import threading
import sys
import numpy as np
import PyTango
import traceback

from cs_constants import *

def get_tango_device(channel_info):
    device = PyTango.DeviceProxy(channel_info.full_name.strip('tango://'))
    return device.tangodevice

# ----------------------------------------------------------------------
#
# ----------------------------------------------------------------------

class EndMeasurementBarrier():

    def __init__(self, n_workers):
        self._n_workers = n_workers
        self._reports = np.zeros(n_workers)

    # ----------------------------------------------------------------------
    def wait(self, blocking=True):
        if blocking:
            while np.any(self._reports == 0):
                time.sleep(REFRESH_PERIOD)
            self._reports = np.zeros(self._n_workers)
        else:
            if np.any(self._reports == 0):
                return False
            else:
                self._reports = np.zeros(self._n_workers)
                return True

    # ----------------------------------------------------------------------
    def report(self, id):
        self._reports[id] = 1

# ----------------------------------------------------------------------
class ExcThread(threading.Thread):

    # ----------------------------------------------------------------------
    def __init__(self, target, threadname, errorBucket,  *args):
        threading.Thread.__init__(self, target=target, name=threadname, args=args)
        self._stop_event = threading.Event()
        self.bucket = errorBucket
        self._name = threadname
        self.status = 'idle'

    # ----------------------------------------------------------------------
    def run(self):
        self.status = 'running'
        try:
            if hasattr(self, '_Thread__target'):
                self.ret = self._Thread__target(*self._Thread__args, **self._Thread__kwargs)
            else:
                self.ret = self._target(*self._args, **self._kwargs)
        except Exception as exp:
            tr = sys.exc_info()[2]
            # while tr.tb_next is not None:
            #     tr = tr.tb_next
            self.bucket.put([self._name, sys.exc_info()[1], traceback.print_tb(tr)])
        finally:
            self.status = 'finished'

    # ----------------------------------------------------------------------
    def stop(self):
        self._stop_event.set()

    # ----------------------------------------------------------------------
    def stopped(self):
        return self._stop_event.isSet()

# ----------------------------------------------------------------------
class CannotDoPilc(Exception):
    pass
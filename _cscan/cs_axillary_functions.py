'''
This file contains axillary cscans for continuous scans scripts.

Author yury.matveev@desy.de
'''

import time
import threading
import sys
import numpy as np
import PyTango

from _cscan.cs_constants import *


# ----------------------------------------------------------------------
def get_channel_tango_device(channel_info):
    return PyTango.DeviceProxy(channel_info.full_name.strip('tango://'))


# ----------------------------------------------------------------------
def get_tango_device(channel_info):
    return get_channel_tango_device(channel_info).tangodevice


# ----------------------------------------------------------------------
def get_channel_source(channel_info):
    tockens = get_channel_tango_device(channel_info).TangoAttribute.split('/')
    return '/'.join(tockens[:-1]), tockens[-1]

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
        except:
            self.bucket.put([self._name, sys.exc_info()])
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
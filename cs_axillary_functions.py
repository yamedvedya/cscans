'''
This file contains axillary cscans for continuous scans scripts.

Author yury.matveev@desy.de
'''

import time
import threading
import sys
import numpy as np
import PyTango

from cs_constants import *

def get_tango_device(channel_info):
    device = PyTango.DeviceProxy(channel_info.full_name.strip('tango://'))
    return device.tangodevice

def send_movevvc_command(device, command_list):
    close_loop_state = None

    if hasattr(device, 'movevvc') and hasattr(device, 'conversion'):
        if device.FlagClosedLoop:
            close_loop_state = device.FlagClosedLoop
            device.FlagClosedLoop = 0

        device.movevvc(["slew: {}, position: {}".format(int(speed * device.conversion), position)
                        for speed, position in command_list])
    else:
        if len(command_list) > 1:
            raise RuntimeError(
                '{} does not have movevvc command, the variable speed move is not possible'.format(device.name()))

        if hasattr(device, 'slewrate') and hasattr(device, 'conversion'):
            device.slewrate = int(command_list[0][0] * device.conversion)
            device.position = command_list[0][1]

        elif hasattr(device, 'velocity'):
            device.velocity = command_list[0][0]
            device.position = command_list[0][1]

        else:
            raise RuntimeError('Could not move {}'.format(device.name()))

    return close_loop_state

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
            # traceback.print_tb(sys.exc_info()[2])
            self.bucket.put([self._name, sys.exc_info()[0], sys.exc_info()[2]])
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
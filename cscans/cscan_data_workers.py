'''
This file contains classes performing DDG timer operation and the acquisition of individual measurement channels.
There are 3 acquisition classes: for "instant" tango device, for the LambdaRoiAnalysis and fake Lambda answer generator

Author yury.matveev@desy.de
'''

# general python imports
import PyTango
import time
import sys

import numpy as np
from Queue import Queue
from Queue import Empty as empty_queue

# cscan imports
from cscan_axillary_functions import ExcThread, EndMeasurementBarrier
from cscan_constants import *

# ----------------------------------------------------------------------
#                       Timer class
# ----------------------------------------------------------------------


class TimerWorker(object):
    def __init__(self, timer_name, error_queue, triggers, data_collector_trigger,
                 workers_done_barrier, macro, motors_list, timing_logger):
        #parameters:
        # timer_name: tango name of timer
        # error_queue: queue to report about problems
        # worker_triggers: list of ordinary worker triggers
        # lambda_roi_workers: list of lambda roi workers
        # workers_done_barrier: barrier where all workers reporting

        self._device_proxy = PyTango.DeviceProxy(timer_name)

        self._point = 0
        self._triggers = triggers
        self._workers_done_barrier = workers_done_barrier
        self._macro = macro
        self._position_measurement = MovingGroupPosition(motors_list, macro, error_queue)
        # self._motors_devices = [PyTango.DeviceProxy(mot.TangoDevice) for mot in motors_list]

        self._data_collector_trigger = data_collector_trigger
        self._time_timer = []
        self._time_acq = []

        self.last_position = None
        self._timing_logger = timing_logger

        self._worker = ExcThread(self._main_loop, 'timer_worker', error_queue)

    def _main_loop(self):
        while not self._worker.stopped():

            if self._macro.debug_mode:
                self._macro.debug('Start timer point {}'.format(self._point))

            _start_time = time.time()
            self._device_proxy.StartAndWaitForTimer()
            self._timing_logger['Acquisition'].append(time.time() - _start_time)

            _start_time = time.time()
            # position = [device.Position for device in self._motors_devices]
            position = self._position_measurement.get_motors_position()
            self.last_position = position[0]
            self._timing_logger['Position_measurement'].append(time.time() - _start_time)

            _start_time = time.time()
            self._data_collector_trigger.put([self._point, time.time(), position])
            for trigger in self._triggers:
                trigger.put(self._point)
            self._workers_done_barrier.wait()
            self._timing_logger['Data_collection'].append(time.time() - _start_time)

            self._point += 1

        self._position_measurement.close()

    def stop(self):
        self._worker.stop()

    def start(self):
        self._worker.start()

    def set_new_period(self, period):
        self._device_proxy.SampleTime = period


# ----------------------------------------------------------------------
#                       "Instant" device data collector
# ----------------------------------------------------------------------

class DataSourceWorker(object):
    def __init__(self, source_info, trigger, workers_done_barrier, error_queue, macro, timing_logger):
        # arguments:
        # index = worker index
        # channel_info - channels information from measurement group
        # trigger_queue - threading queue to start measurement
        # workers_done_barrier - barrier to restart timer
        # error_queue - queue to report problems
        # macro - link to marco

        self._source_info = source_info
        self._trigger = trigger
        self._macro = macro
        self._workers_done_barrier = workers_done_barrier
        self._timing_logger = timing_logger

        self.data_buffer = {}
        tokens = source_info.source.split('/')
        self._device_proxy = PyTango.DeviceProxy('/'.join(tokens[2:-1]))
        self._device_attribute = tokens[-1]
        self.channel_name = source_info.full_name
        self.channel_label = source_info.label

        self._is_counter = False
        for counter_name, tango_name in counter_names.items():
            if counter_name in source_info.full_name:
                self._counter_proxy = PyTango.DeviceProxy('{}.{:02d}'.format(tango_name, int(source_info.full_name.split('/')[-1])))
                self._counter_proxy.Reset()
                self._is_counter = True

        self._worker = ExcThread(self._main_loop, source_info.name, error_queue)
        self._worker.start()

    def _main_loop(self):
        _timeit = []
        while not self._worker.stopped():
            try:
                index = self._trigger.get(block=False)
                _start_time = time.time()
                self.data_buffer['{:04d}'.format(index)] = getattr(self._device_proxy, self._device_attribute)
                if self._is_counter:
                    self._counter_proxy.Reset()
                    time.sleep(COUNTER_RESET_DELAY)
                self._workers_done_barrier.report()
                self._timing_logger[self.channel_label].append(time.time() - _start_time)

                if self._macro.debug_mode:
                    self._macro.debug('Worker {} was triggered, point {} with data {} in buffer'.format(
                                      self.channel_name, index, self.data_buffer['{:04d}'.format(index)]))
            except empty_queue:
                time.sleep(REFRESH_PERIOD)
            except Exception as err:
                self._macro.output('Error {} {}'.format(err, sys.exc_info()[2].tb_lineno))

    def stop(self):
        self._worker.stop()

# ----------------------------------------------------------------------
#                       Lambda ROI reader class
# ----------------------------------------------------------------------


class LambdaRoiWorker(object):
    def __init__(self, source_info, trigger, error_queue, macro, timing_logger):
        # arguments:
        # index = worker index
        # channel_info - channels information from measurement group
        # trigger_queue - threading queue to start measurement
        # workers_done_barrier - barrier to restart timer
        # error_queue - queue to report problems
        # macro - link to marco

        self._trigger = trigger
        self._macro = macro
        self._timing_logger = timing_logger

        self.data_buffer = {}
        self._device_proxy = PyTango.DeviceProxy(self._macro.getEnv('LambdaOnlineAnalysis'))

        if 'atten' in source_info.label:
            self._channel = int(source_info.label[-7])-1
            self._correction_needed = True
            self._attenuator_proxy = PyTango.DeviceProxy(self._macro.getEnv('AttenuatorProxy'))
        else:
            self._channel = int(source_info.label[-1])-1
            self._correction_needed = False

        self.channel_name = source_info.full_name
        self.channel_label = source_info.label

        self._worker = ExcThread(self._main_loop, source_info.name, error_queue)
        self._worker.start()

    def _main_loop(self):
        _timeit = []
        while not self._worker.stopped():
            try:
                index = self._trigger.get(block=False)
                _start_time = time.time()
                while time.time() - _start_time < TIMEOUT:
                    if self._device_proxy.lastanalyzedframe >= index + 1:

                        data = self._device_proxy.getroiforframe([self._channel, index + 1])
                        if self._correction_needed:
                            data *= self._attenuator_proxy.Position
                        self._timing_logger[self.channel_label].append(time.time() - _start_time)

                        self.data_buffer['{:04d}'.format(index)] = data
                        if self._macro.debug_mode:
                            self._macro.debug('Lambda RoI {} was triggered, point {} with data {} in buffer'.format(
                                                self._channel, index, self.data_buffer['{:04d}'.format(index)]))

                        break
                    else:
                        time.sleep(REFRESH_PERIOD)
            except empty_queue:
                time.sleep(REFRESH_PERIOD)

    def stop(self):
        self._worker.stop()

# ----------------------------------------------------------------------
#                       Lambda data class
# ----------------------------------------------------------------------


class LambdaWorker(object):
    def __init__(self, source_info, trigger, workers_done_barrier, error_queue, macro):
        # arguments:
        # channel_info - channels information from measurement group
        # trigger_queue - threading queue to start measurement
        # workers_done_barrier - barrier to restart timer
        # error_queue - queue to report problems
        # macro - link to marco

        self._trigger = trigger
        self._macro = macro
        self._workers_done_barrier = workers_done_barrier

        self.data_buffer = {}
        self.channel_name = source_info.full_name

        self._worker = ExcThread(self._main_loop, source_info.name, error_queue)
        self._worker.start()

    def _main_loop(self):
        _timeit = []
        while not self._worker.stopped():
            try:
                index = self._trigger.get(block=False)
                self.data_buffer['{:04d}'.format(index)] = -1
            except empty_queue:
                time.sleep(REFRESH_PERIOD)

    def stop(self):
        self._worker.stop()


# ----------------------------------------------------------------------
#                       Moving group position
# ----------------------------------------------------------------------

class MovingGroupPosition(object):
    def __init__(self, motors_list, macro, error_queue):

        self._macro = macro

        self._motors_reported_barrier = EndMeasurementBarrier(len(motors_list))
        self._motor_triggers = [Queue() for _ in motors_list]
        self._devices = [MotorPosition(motor, trigger, self._motors_reported_barrier, error_queue)
                         for motor, trigger in zip(motors_list, self._motor_triggers)]

    # ----------------------------------------------------------------------
    def get_motors_position(self):
        for ind, trigger in enumerate(self._motor_triggers):
            trigger.put(ind)
        self._motors_reported_barrier.wait()

        return [device.position for device in self._devices]

    # ----------------------------------------------------------------------
    def close(self):
        for device in self._devices:
            device.stop()
            while device.state != "stopped":
                time.sleep(REFRESH_PERIOD)

        if self._macro.debug_mode:
            self._macro.debug('MovingGroupPosition stopped')

# ----------------------------------------------------------------------
#                       Motor position
# ----------------------------------------------------------------------


class MotorPosition(object):

    def __init__(self, motor, trigger, motors_reported_barrier, error_queue):

        self._trigger = trigger
        self._device_proxy = PyTango.DeviceProxy(motor.TangoDevice)
        self._motors_reported_barrier = motors_reported_barrier

        self.position = None

        self.state = 'idle'

        self._worker = ExcThread(self._main_loop, self._device_proxy.name(), error_queue)
        self._worker.start()

    def _main_loop(self):
        self.state = 'running'
        while not self._worker.stopped():
            try:
                ind = self._trigger.get(block=False)
                self.position = self._device_proxy.Position
                self._motors_reported_barrier.report()
            except empty_queue:
                time.sleep(REFRESH_PERIOD)

        self.state = 'stopped'

    def stop(self):
        self._worker.stop()
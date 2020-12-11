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

if sys.version_info.major >= 3:
    from queue import Queue
    from queue import Empty as empty_queue
else:
    from Queue import Queue
    from Queue import Empty as empty_queue

# cscan imports
from cscans.cscan_axillary_functions import ExcThread, EndMeasurementBarrier, get_reciprocal_coordinates
from cscans.cscan_constants import *

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

        self._point = -1
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

    # ----------------------------------------------------------------------
    def _main_loop(self):
        while not self._worker.stopped():

            if self._macro.debug_mode:
                self._macro.debug('Start timer point {}'.format(self._point + 1))

            _start_position = None
            _position_time1 = None
            _end_position = None
            _position_time2 = None

            if MOTORS_POSITION_LOGIC in ['before', 'center']:
                _start_time = time.time()
                _start_position = self._position_measurement.get_motors_position()
                _position_time1 = time.time() - _start_time

            _start_time = time.time()
            self._device_proxy.StartAndWaitForTimer()
            self._timing_logger['Timer'].append(time.time() - _start_time)

            if MOTORS_POSITION_LOGIC in ['center', 'after']:
                _start_time = time.time()
                _end_position = self._position_measurement.get_motors_position()
                _position_time2 = time.time() - _start_time

            if MOTORS_POSITION_LOGIC == 'before':
                _position = _start_position
                _timing = _position_time1
            elif MOTORS_POSITION_LOGIC == 'center':
                _position = (_start_position + _end_position)/2
                _timing = _position_time1 + _position_time2
            else:
                _position = _end_position
                _timing = _position_time2

            self._timing_logger['Position_measurement'].append(_timing)

            if _position[0] == self.last_position:
                if self._macro.debug_mode:
                    self._macro.debug('Timer stops due to repeating positions {} == {}'.format(_position[0],
                                                                                               self.last_position))
                break

            self._point += 1
            self.last_position = _position[0]

            _start_time2 = time.time()
            self._data_collector_trigger.put([self._point, time.time(), _position])

            for trigger in self._triggers:
                trigger.put(self._point)
            self._workers_done_barrier.wait()

            self._timing_logger['Data_collection'].append(time.time() - _start_time2)
            self._timing_logger['Point_dead_time'].append(time.time() - _start_time)

        self._position_measurement.close()

    # ----------------------------------------------------------------------
    def set_start_position(self):
        self.last_position = self._position_measurement.get_motors_position()[0]

    # ----------------------------------------------------------------------
    def stop(self):
        self._worker.stop()
        while self._worker.status != 'finished':
            time.sleep(REFRESH_PERIOD)
        return self._point

    # ----------------------------------------------------------------------
    def start(self):
        self._worker.start()

    # ----------------------------------------------------------------------
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

        self.last_collected_point = -1

        self.data_buffer = {}
        tokens = source_info.source.split('/')

        self._device_proxy = PyTango.DeviceProxy('/'.join(tokens[2:-1]))
        self._device_attribute = tokens[-1]

        self.channel_name = source_info.full_name
        self._channel_label = source_info.label

        self._is_counter = False
        for counter_name, tango_name in counter_names.items():
            if counter_name in source_info.full_name:
                self._counter_proxy = PyTango.DeviceProxy('{}.{:02d}'.format(tango_name,
                                                                             int(source_info.full_name.split('/')[-1])))
                self._counter_proxy.Reset()
                self._is_counter = True

        self._worker = ExcThread(self._main_loop, source_info.name, error_queue)
        self._worker.start()

    # ----------------------------------------------------------------------
    def _main_loop(self):
        try:
            while not self._worker.stopped():
                try:
                    point_to_collect = self._trigger.get(block=False)
                    self.data_buffer[point_to_collect] = {self.channel_name: None}

                    _start_time = time.time()
                    data = getattr(self._device_proxy, self._device_attribute)
                    self.data_buffer[point_to_collect][self.channel_name] = data
                    if self._is_counter:
                        self._counter_proxy.Reset()
                    self._workers_done_barrier.report()
                    self.last_collected_point = point_to_collect

                    self._timing_logger[self._channel_label].append(time.time() - _start_time)

                    if self._macro.debug_mode:
                        self._macro.debug('{} point {} data {}'.format(self._channel_label, point_to_collect, data))

                except empty_queue:
                    time.sleep(REFRESH_PERIOD)

        except Exception as err:
            self._macro.error('{} error {} {}'.format(self._channel_label, err, sys.exc_info()[2].tb_lineno))
            raise err

    # ----------------------------------------------------------------------
    def get_data_for_point(self, point_num):
        return self.data_buffer[point_num]

    # ----------------------------------------------------------------------
    def stop(self):
        self._worker.stop()
        while self._worker.status != 'finished':
            time.sleep(REFRESH_PERIOD)

# ----------------------------------------------------------------------
#                       Lambda ROI reader class
# ----------------------------------------------------------------------


class LambdaRoiWorker(object):
    def __init__(self, trigger, error_queue, macro, timing_logger):
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

        self.last_collected_point = -1

        self._correction_needed = False
        self._attenuator_proxy = PyTango.DeviceProxy(self._macro.getEnv('AttenuatorProxy'))

        # channel_num, full_name, need correction
        self._channels = []
        self.channel_name = 'Lambda'

        self.data_buffer = {}
        self._device_proxy = PyTango.DeviceProxy(self._macro.getEnv('LambdaOnlineAnalysis'))

        self._worker = ExcThread(self._main_loop, 'lambda_worker', error_queue)
        self._worker.start()

    # ----------------------------------------------------------------------
    def add_channel(self, source_info):

        if 'atten' in source_info.label:
            self._channels.append([int(source_info.label[-7])-1, source_info.label, source_info.full_name, True])
            self._correction_needed = True

        elif source_info.label == 'lmbd':
            self._channels.append([-1, source_info.label, source_info.full_name, False])

        else:
            self._channels.append([int(source_info.label[-1])-1, source_info.label, source_info.full_name, False])

    # ----------------------------------------------------------------------
    def _main_loop(self):
        try:
            while not self._worker.stopped():
                try:
                    point_to_collect = self._trigger.get(block=False)
                    _start_time = time.time()
                    self.data_buffer[point_to_collect] = {}
                    _success = False
                    while time.time() - _start_time < TIMEOUT:
                        if self._device_proxy.lastanalyzedframe >= point_to_collect + 1:
                            _data_to_print = {}
                            if self._correction_needed:
                                atten = self._attenuator_proxy.Position
                            else:
                                atten = 1

                            for channel_num, label, full_name, need_correction in self._channels:
                                if channel_num == -1:
                                    data = -1
                                else:
                                    data = self._device_proxy.getroiforframe([channel_num, point_to_collect + 1])
                                    if need_correction:
                                        data *= atten

                                self.data_buffer[point_to_collect][full_name] = data
                                _data_to_print[label] = data

                            self._timing_logger['Lambda'].append(time.time() - _start_time)

                            if self._macro.debug_mode:
                                self._macro.debug('Lambda point {} data {}'.format(point_to_collect, _data_to_print))

                            self.last_collected_point = point_to_collect

                            _success = True
                            break

                        else:
                            time.sleep(REFRESH_PERIOD)

                    if not _success:
                        raise RuntimeError('No response from Lambda!')

                except empty_queue:
                    time.sleep(REFRESH_PERIOD)

        except Exception as err:
            self._macro.error('Lambda worker error {} {}'.format(err, sys.exc_info()[2].tb_lineno))
            raise err

    # ----------------------------------------------------------------------
    def get_data_for_point(self, point_num):
        return self.data_buffer[point_num]

    # ----------------------------------------------------------------------
    def stop(self):
        self._worker.stop()
        while self._worker.status != 'finished':
            time.sleep(REFRESH_PERIOD)

# ----------------------------------------------------------------------
#                       Moving group position
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
class Motion_Monitor(object):
    def __init__(self, motors_list, diffractometer, macro, error_queue):
        super(Motion_Monitor, self).__init__()

        self._position_measurement = MovingGroupPosition(motors_list, macro, error_queue)
        self._macro = macro
        self._real_positions = None
        self._reciprocal_positions = None
        self._worker = ExcThread(self._main_loop, 'motion_monitor', error_queue)

    def _main_loop(self):
        try:
            self._real_positions = self._position_measurement.get_motors_position()
            while not self._worker.stopped():
                positions = self._position_measurement.get_motors_position()
                self._real_positions.append(positions)

        except Exception as err:
            self._macro.error('Lambda worker error {} {}'.format(err, sys.exc_info()[2].tb_lineno))
            raise err


# ----------------------------------------------------------------------
class MovingGroupPosition(object):
    def __init__(self, motors_list, macro, error_queue):

        self._macro = macro

        if MOTORS_POSITION_MODE == 'sync':
            self._motors_reported_barrier = EndMeasurementBarrier(len(motors_list))
            self._motor_triggers = [Queue() for _ in motors_list]
            self._devices = [MotorPosition(motor, trigger, self._motors_reported_barrier, error_queue)
                             for motor, trigger in zip(motors_list, self._motor_triggers)]
        else:
            self._devices = []
            for motor in motors_list:
                try:
                    self._devices.append(PyTango.DeviceProxy(motor.TangoDevice))
                except:
                    self._devices.append(motor)

    # ----------------------------------------------------------------------
    def get_motors_position(self):
        if MOTORS_POSITION_MODE == 'sync':
            for ind, trigger in enumerate(self._motor_triggers):
                trigger.put(ind)
            self._motors_reported_barrier.wait()

        return np.array([device.position for device in self._devices])

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
        try:
            self._device_proxy = PyTango.DeviceProxy(motor.TangoDevice)
            name = self._device_proxy.name()
        except:
            self._device_proxy = motor
            name = self._device_proxy.name

        self._motors_reported_barrier = motors_reported_barrier

        self.position = None

        self.state = 'idle'

        self._worker = ExcThread(self._main_loop, name, error_queue)
        self._worker.start()

    # ----------------------------------------------------------------------
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

    # ----------------------------------------------------------------------
    def stop(self):
        self._worker.stop()
'''
This file contains classes performing DDG timer operation and the acquisition of individual measurement channels.
There are 2 acquisition classes: for "instant" tango device and for the Lambda

Author yury.matveev@desy.de
'''

# general python imports
import PyTango
import time
import sys

if sys.version_info.major >= 3:
    from queue import Empty as empty_queue
else:
    from Queue import Empty as empty_queue

# cscans imports
from cs_axillary_functions import ExcThread, get_tango_device
from cs_constants import *

# ----------------------------------------------------------------------
#                       Timer class
# ----------------------------------------------------------------------


class TimerWorker(object):
    def __init__(self, timer_name, error_queue, triggers, data_collector_trigger,
                 workers_done_barrier, macro, movement, timing_logger):
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
        self._movement = movement
        # self._motors_devices = [PyTango.DeviceProxy(mot.TangoDevice) for mot in motors_list]

        self._data_collector_trigger = data_collector_trigger
        self._time_timer = []
        self._time_acq = []

        self._paused = False

        self._timing_logger = timing_logger

        self._worker = ExcThread(self._main_loop, 'timer_worker', error_queue)

    # ----------------------------------------------------------------------
    def _main_loop(self):
        _acq_start_time = time.time()
        try:
            _timer_start_position = _last_position = self._movement.get_main_motor_position()
            while not self._worker.stopped():

                if not self._paused:
                    self._macro.report_debug('Start timer point {}'.format(self._point + 1))

                    _start_position = None
                    _position_time1 = None
                    _end_position = None
                    _position_time2 = None

                    if MOTORS_POSITION_LOGIC in ['before', 'center']:
                        _start_time = time.time()
                        _start_position = self._movement.get_motors_position()
                        _position_time1 = time.time() - _start_time

                    _start_time = time.time()
                    self._device_proxy.StartAndWaitForTimer()
                    self._timing_logger['Timer'].append(time.time() - _start_time)

                    if MOTORS_POSITION_LOGIC in ['center', 'after']:
                        _start_time = time.time()
                        _end_position = self._movement.get_motors_position()
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

                    if _position > _timer_start_position:
                        if _position == _last_position:
                            self._macro.report_debug('Timer stops due to repeating position {}'.format(_position))
                            break

                    self._point += 1
                    _last_position = _position

                    _start_time2 = time.time()
                    self._data_collector_trigger.put([self._point, time.time() - _acq_start_time, _position])

                    for trigger in self._triggers:
                        trigger.put(self._point)

                    while not self._workers_done_barrier.wait(False) and not self._worker.stopped():
                        time.sleep(REFRESH_PERIOD)

                    self._timing_logger['Data_collection'].append(time.time() - _start_time2)
                    self._timing_logger['Point_dead_time'].append(time.time() - _start_time)
                else:
                    time.sleep(REFRESH_PERIOD)

        except Exception as err:
            self._macro.error('Timer error: {}'.format(err))
            raise

    # ----------------------------------------------------------------------
    def pause(self):
        self._paused = True

    # ----------------------------------------------------------------------
    def resume(self):
        self._paused = False

    # ----------------------------------------------------------------------
    def stop(self):
        self._worker.stop()
        while self._worker.status == 'running':
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
    def __init__(self, my_id, source_info, trigger, workers_done_barrier, error_queue, macro, timing_logger):
        # arguments:
        # index = worker index
        # channel_info - channels information from measurement group
        # trigger_queue - threading queue to start measurement
        # workers_done_barrier - barrier to restart timer
        # error_queue - queue to report problems
        # macro - link to marco

        self._my_id = my_id
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
        for counter_name in COUNTER_NAMES:
            if counter_name in source_info.full_name:
                self._tango_proxy = PyTango.DeviceProxy(get_tango_device(source_info))
                self._tango_proxy.Reset()
                self._is_counter = True

        self._is_timer = False
        for timer_prefix in TIMER_PREFIXES:
            if timer_prefix in source_info.label:
                self._tango_proxy = PyTango.DeviceProxy(get_tango_device(source_info))
                self._timer_value = None
                self._is_timer = True

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
                    if self._is_timer:
                        if self._timer_value is None:
                            self._timer_value = self._tango_proxy.SampleTime
                        data = self._timer_value
                    else:
                        data = getattr(self._device_proxy, self._device_attribute)
                    self.data_buffer[point_to_collect][self.channel_name] = data
                    if self._is_counter:
                        self._tango_proxy.Reset()
                    self._workers_done_barrier.report(self._my_id)
                    self.last_collected_point = point_to_collect

                    self._timing_logger[self._channel_label].append(time.time() - _start_time)

                    self._macro.report_debug('{} point {} data {}'.format(self._channel_label, point_to_collect, data))

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
        while self._worker.status == 'running':
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
        self.timeout = 0

        self._correction_needed = False
        self._attenuator_proxy = PyTango.DeviceProxy(self._macro.getEnv('AttenuatorProxy'))

        # channel_num, full_name, need correction
        self._channels = []
        self.channel_name = 'Lambda'

        self.data_buffer = {}
        self._online_proxy = PyTango.DeviceProxy(self._macro.getEnv('LambdaOnlineAnalysis'))
        self._asapo_proxy = PyTango.DeviceProxy(self._macro.getEnv('LambdaASAPOAnalysis'))

        self._worker = ExcThread(self._main_loop, 'lambda_worker', error_queue)
        self._worker.start()

    # ----------------------------------------------------------------------
    def add_channel(self, source_info):

        if 'asapo' in source_info.label:
            self._channels.append([self._asapo_proxy, int(source_info.label[-1]) - 1,
                                   source_info.label, source_info.full_name, False])

        else:
            if 'atten' in source_info.label:
                self._channels.append([self._online_proxy, int(source_info.label[-7])-1,
                                       source_info.label, source_info.full_name, True])
                self._correction_needed = True

            elif source_info.label == 'lmbd':
                self._channels.append([None, None, source_info.label, source_info.full_name, False])

            else:
                self._channels.append([self._online_proxy, int(source_info.label[-1])-1,
                                       source_info.label, source_info.full_name, False])

    # ----------------------------------------------------------------------
    def _main_loop(self):
        try:
            while not self._worker.stopped():
                try:
                    point_to_collect = self._trigger.get(block=False)
                    self._macro.report_debug('Lambda trigger {}'.format(point_to_collect))
                    _start_time = time.time()
                    self.data_buffer[point_to_collect] = {}
                    _success = False
                    while time.time() - _start_time < TIMEOUT_LAMBDA:
                        if self._online_proxy.lastanalyzedframe >= point_to_collect + 1:
                            # self._macro.output('Got point after {}'.format(time.time() - _start_time))
                            _data_to_print = {}
                            if self._correction_needed:
                                atten = self._attenuator_proxy.Position
                            else:
                                atten = 1

                            for source, channel_num, label, full_name, need_correction in self._channels:
                                if channel_num is None:
                                    data = -1
                                else:
                                    data = source.getroiforframe([channel_num, point_to_collect + 1])
                                    if need_correction:
                                        data *= atten

                                self.data_buffer[point_to_collect][full_name] = data
                                _data_to_print[label] = data

                            self._timing_logger['Lambda'].append(time.time() - _start_time)

                            self._macro.report_debug('Lambda point {} data {}'.format(point_to_collect, _data_to_print))

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
        while self._worker.status == 'running':
            time.sleep(REFRESH_PERIOD)

    # ----------------------------------------------------------------------
    def set_timeout(self, timeout):
        self.timeout = timeout
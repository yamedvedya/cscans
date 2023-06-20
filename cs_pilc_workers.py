'''
This file contains classes performing DDG timer operation and the acquisition of individual measurement channels.
There are 2 acquisition classes: for "instant" tango device and for the Lambda

Author yury.matveev@desy.de
'''

# general python imports
import PyTango
import time
import os
import numpy as np

# cscans imports
from cs_axillary_functions import ExcThread, CannotDoPilc
from cs_constants import *

# ----------------------------------------------------------------------
#                       Timer class
# ----------------------------------------------------------------------


class PILCWorker(object):
    def __init__(self, macro, data_collector_trigger, detector_triggers, movables, experimental_channels, error_queue):

        self._error_queue = error_queue
        self._macro = macro

        self._time_scan = False
        self._external_trigger = False
        for motor in movables:
            if motor == DUMMY_MOTOR:
                self._time_scan = True
            elif motor == EXTERNAL_TRIGGER:
                self._external_trigger = True
            elif motor not in PILC_MOTORS_MAP.keys() or self._time_scan:
                raise CannotDoPilc()
        self._movables = movables

        self._channels = []
        for channel_info in experimental_channels:
            if 'eh_t' in channel_info.label:
                self._timer_name = channel_info.full_name
            elif 'lmbd' in channel_info.label or "1m" in channel_info.label:
                pass
            elif channel_info.label not in PILC_DETECTOR_MAP.keys():
                self._macro.warning(f'{channel_info.label} is not linked to any PiLC. The software scan will be performed')
                raise CannotDoPilc()
            else:
                self._channels.append([channel_info.label, channel_info.full_name])

        self._data_collector_trigger = data_collector_trigger
        self._detector_triggers = detector_triggers

        self._trigger_generators = [PyTango.DeviceProxy(device) for device in PILC_TRIGGERS]
        self._counter = PyTango.DeviceProxy(PILC_COUNTER)
        self._counter.manualmode = 0
        self._adc = PyTango.DeviceProxy(PILC_ADC)
        self._adc.manualmode = 0

        f_name = os.path.splitext(self._macro.getEnv('ScanFile')[0])[0] + \
                                '_{}_' + '{:05d}'.format(self._macro.getEnv('ScanID'))

        for idx, device in enumerate(self._trigger_generators):
            device.FileDir = self._macro.getEnv('ScanDir')
            device.FilePrefix = f_name.format('TG_{}'.format(idx))

        self._counter.FileDir = self._macro.getEnv('ScanDir')
        self._counter.FilePrefix = f_name.format('CT')

        self._adc.FileDir = self._macro.getEnv('ScanDir')
        self._adc.FilePrefix = f_name.format('ADC')

        self._main_trigger = None
        self._slave_trigger = None

        self._integration_time = None
        self.data_buffer = {}

        self.last_collected_point = 0
        self.npts = 0
        self._paused = True
        self._worker = ExcThread(self._main_loop, 'plic_worker', error_queue)

    # ----------------------------------------------------------------------
    def _main_loop(self):
        _start_time = time.time()
        try:
            while not self._worker.stopped():
                if not self._paused:
                    if self._trigger_generators[self._main_trigger].TriggerCounter == 1 and _start_time is None:
                        self._macro.report_debug('Timer: new start time saved')
                        _start_time = time.time()

                    while self._trigger_generators[self._main_trigger].TriggerCounter > self.last_collected_point + 1:
                        time.sleep(self._integration_time)
                        _new_point = self.last_collected_point + 1
                        if _new_point == 1:
                            time.sleep(1)  # TODO better solution

                        if _new_point >= self.npts:
                            break

                        self._macro.report_debug('Got point {}'.format(_new_point))

                        try:
                            if MOTORS_POSITION_LOGIC == 'before':
                                _position = self._get_positions_for_point(_new_point-1)

                            elif MOTORS_POSITION_LOGIC == 'center':
                                _position = (self._get_positions_for_point(_new_point-1) +
                                             self._get_positions_for_point(_new_point))/2
                            else:
                                _position = self._get_positions_for_point(_new_point)

                            _point_data = {self._timer_name: self._integration_time}
                            for name, full_name in self._channels:
                                _point_data[full_name] = self._get_point_for_detector(name, _new_point)
                        except IndexError:
                            self._macro.warning(f'Point {_new_point} was not read!')
                            break

                        self.data_buffer[_new_point] = _point_data
                        self.last_collected_point = _new_point
                        for trigger in self._detector_triggers:
                            trigger.put(self.last_collected_point)
                        self._data_collector_trigger.put([self.last_collected_point, time.time() - _start_time,
                                                          _position])
            else:
                time.sleep(REFRESH_PERIOD)

        except Exception as err:
            self._macro.error('Timer error: {}'.format(err))
            self.pause()
            raise

        self.pause()

    # ----------------------------------------------------------------------
    def reset_pilcs(self):
        self._counter.manualmode = 1
        self._adc.manualmode = 1

    # ----------------------------------------------------------------------
    def get_scan_point(self):
        return self.last_collected_point

    # ----------------------------------------------------------------------
    def _get_point_for_detector(self, detector, point):
        if PILC_DETECTOR_MAP[detector]['device'] == 'TG0':
            device = self._trigger_generators[0]
        elif PILC_DETECTOR_MAP[detector]['device'] == 'TG1':
            device = self._trigger_generators[1]
        elif PILC_DETECTOR_MAP[detector]['device'] == 'CT':
            device = self._counter
        elif PILC_DETECTOR_MAP[detector]['device'] == 'ADC':
            device = self._adc
        else:
            raise RuntimeError('Wrong PiLC for {}!'.format(detector))

        collected_data = getattr(device, PILC_DETECTOR_MAP[detector]['attribute'])
        if collected_data is None:
            raise RuntimeError(f"No data was recorded for {PILC_DETECTOR_MAP[detector]['attribute']} ({device})\nHit: execute cscan_set_pilc macro and if not helped: check trigger cables")

        return collected_data[point]

    # ----------------------------------------------------------------------
    def _get_positions_for_point(self, point):
        _position = []
        if self._time_scan:
            _position = [self._integration_time*point]
        else:
            for motor in self._movables:
                _position.append(getattr(self._trigger_generators[PILC_MOTORS_MAP[motor]['device']],
                                         'Position{}Data'.format(PILC_MOTORS_MAP[motor]['encoder']))[point])
        return np.array(_position)

    # ----------------------------------------------------------------------
    def get_data_for_point(self, point):
        return self.data_buffer[point]

    # ----------------------------------------------------------------------
    def is_done(self):
        r_trg = self._trigger_generators[self._main_trigger].RemainingTriggers
        trg = self._trigger_generators[self._main_trigger].TriggerCounter

        if r_trg == 0 and trg != 0:
            self._macro.report_debug('PiLC is DONE: r_trg: {}, trg: {}'.format(r_trg, trg))
            return True
        else:
            return False
        # return self._trigger_generators[self._main_trigger].RemainingTrigger == 0 and \
        #        self._trigger_generators[self._main_trigger].TriggerCounter != 0

    # ----------------------------------------------------------------------
    def setup_main_motor(self, motor, start_position):
        if self._time_scan:
            self._main_trigger = 0
            self._trigger_generators[self._main_trigger].TimeTriggerStart = start_position
            self._trigger_generators[self._main_trigger].TriggerMode = 2
        elif self._external_trigger:
            self._main_trigger = 0
            self._trigger_generators[self._main_trigger].TriggerMode = 9
        else:
            self._main_trigger = int(PILC_MOTORS_MAP[motor]['device'])
            self._trigger_generators[self._main_trigger].PositionTriggerStart = start_position
            self._trigger_generators[self._main_trigger].TriggerMode = 3
            self._trigger_generators[self._main_trigger].EncoderTriggering = PILC_MOTORS_MAP[motor]['encoder']

        if len(self._trigger_generators) > 1:
            self._slave_trigger = int(not int(self._main_trigger))
            self._trigger_generators[self._slave_trigger].TriggerMode = 5

    # ----------------------------------------------------------------------
    def set_new_period(self, integration_time):
        self._integration_time = integration_time
        self._trigger_generators[self._main_trigger].TimeTriggerStepSize = integration_time
        self._trigger_generators[self._main_trigger].TriggerPulseLength = PILC_TRIGGER_TIME/1000.

        if len(self._trigger_generators) > 1:
            self._trigger_generators[self._slave_trigger].TimeTriggerStepSize = integration_time
            self._trigger_generators[self._slave_trigger].TriggerPulseLength = PILC_TRIGGER_TIME/1000.

    # ----------------------------------------------------------------------
    def set_npts(self, npts):
        self._macro.report_debug(f'PiLC NPTS {npts}')
        self.npts = npts
        self._trigger_generators[self._main_trigger].NbTriggers = npts + 2

        if len(self._trigger_generators) > 1:
            self._trigger_generators[self._slave_trigger].NbTriggers = npts + 1

    # ----------------------------------------------------------------------
    def pause(self):
        self._trigger_generators[self._main_trigger].Arm = 0

        if len(self._trigger_generators) > 1:
            self._trigger_generators[self._slave_trigger].Arm = 0

        self._paused = True

    # ----------------------------------------------------------------------
    def resume(self):
        self._trigger_generators[self._main_trigger].Arm = 1

        if len(self._trigger_generators) > 1:
            self._trigger_generators[self._slave_trigger].Arm = 1

        self._paused = False

    # ----------------------------------------------------------------------
    def start(self):
        self._macro.report_debug('Timer started')
        self._worker.start()

    # ----------------------------------------------------------------------
    def stop(self):
        self._worker.stop()
        while self._worker.status == 'running':
            time.sleep(REFRESH_PERIOD)
        return self.last_collected_point
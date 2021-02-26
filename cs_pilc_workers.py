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

# cscan imports
from cs_axillary_functions import ExcThread, CannotDoPilc
from cs_constants import *

# ----------------------------------------------------------------------
#                       Timer class
# ----------------------------------------------------------------------


class PILCWorker(object):
    def __init__(self, macro, data_collector_trigger, lambda_trigger, movables, experimental_channels, error_queue):

        self._error_queue = error_queue
        self._macro = macro

        for motor in movables:
            if motor not in PLIC_MOTORS_MAP.keys():
                # self._macro.warning('{} is not linked to any PiLC, tango scan will be performed'.format(motor))
                raise CannotDoPilc()
        self._movables = movables

        self._channels = []
        for channel_info in experimental_channels:
            if 'eh_t' in channel_info.label:
                self._timer_name = channel_info.full_name
            elif 'lmbd' in channel_info.label:
                pass
            elif channel_info.label not in PLIC_DETECTOR_MAP.keys():
                if self._macro.ask_user('{} is not linked to any PiLC. Do tango scan?'.format(channel_info.label)):
                    raise CannotDoPilc()
                else:
                    raise RuntimeError('Cannot do PiLC scan')
            else:
                self._channels.append([channel_info.label, channel_info.full_name])

        self._data_collector_trigger = data_collector_trigger
        self._lambda_trigger = lambda_trigger

        self._trigger_generators = [PyTango.DeviceProxy(device) for device in PLIC_TRIGGERS]
        self._counter = PyTango.DeviceProxy(PLIC_COUNTER)
        self._adc = PyTango.DeviceProxy(PLIC_ADC)

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
        self._paused = True
        self._worker = ExcThread(self._main_loop, 'plic_worker', error_queue)

    # ----------------------------------------------------------------------
    def _main_loop(self):
        _start_time = None
        try:
            while not self._worker.stopped():
                if not self._paused:
                    if self._trigger_generators[self._main_trigger].TriggerCounter == 1 and _start_time is None:
                        self._macro.report_debug('Timer: new start time saved')
                        _start_time = time.time()

                    if self._trigger_generators[self._main_trigger].TriggerCounter > self.last_collected_point + 1:
                        _new_point = self.last_collected_point + 1
                        self._macro.report_debug('Got point {}'.format(_new_point))

                        if MOTORS_POSITION_LOGIC == 'before':
                            _position = self._get_positions_for_point(_new_point-1)

                        elif MOTORS_POSITION_LOGIC == 'center':
                            _position = (self._get_positions_for_point(_new_point-1) +
                                         self._get_positions_for_point(_new_point))/2
                        else:
                            _position = self._get_positions_for_point(_new_point)

                        self.data_buffer[_new_point] = {self._timer_name: self._integration_time}
                        for name, full_name in self._channels:
                            self.data_buffer[_new_point][full_name] = self._get_point_for_detector(name, _new_point)

                        self.last_collected_point = _new_point
                        if self._lambda_trigger is not None:
                            self._lambda_trigger.put(self.last_collected_point)
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
    def _get_point_for_detector(self, detector, point):
        if PLIC_DETECTOR_MAP[detector]['device'] == 'TG0':
            device = self._trigger_generators[0]
        elif PLIC_DETECTOR_MAP[detector]['device'] == 'TG1':
            device = self._trigger_generators[1]
        elif PLIC_DETECTOR_MAP[detector]['device'] == 'CT':
            device = self._counter
        elif PLIC_DETECTOR_MAP[detector]['device'] == 'ADC':
            device = self._adc
        else:
            raise RuntimeError('Wrong PiLC for {}!'.format(detector))

        return getattr(device, PLIC_DETECTOR_MAP[detector]['attribute'])[point]

    # ----------------------------------------------------------------------
    def _get_positions_for_point(self, point):
        _position = []
        for motor in self._movables:
            _position.append(getattr(self._trigger_generators[PLIC_MOTORS_MAP[motor]['device']],
                                     'Position{}Data'.format(PLIC_MOTORS_MAP[motor]['encoder']))[point])
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
        self._main_trigger = int(PLIC_MOTORS_MAP[motor]['device'])

        self._trigger_generators[self._main_trigger].PositionTriggerStart = start_position
        self._trigger_generators[self._main_trigger].TriggerMode = 3
        self._trigger_generators[self._main_trigger].EncoderTriggering = PLIC_MOTORS_MAP[motor]['encoder']

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
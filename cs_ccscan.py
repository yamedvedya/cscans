'''
This is child of CSScan with modified functionality

Author yury.matveev@desy.de
'''

# general python imports
import sys
import time
import os
import PyTango
import numpy as np
import traceback
from collections import OrderedDict

if sys.version_info.major >= 3:
    from queue import Queue
    from queue import Empty as empty_queue

    old_python = False
else:
    from Queue import Queue
    from Queue import Empty as empty_queue

    old_python = True

# Sardana imports

from sardana.macroserver.scan import CSScan

# cscans imports

from cs_axillary_functions import EndMeasurementBarrier, ExcThread, CannotDoPilc, get_tango_device
from cs_setup_detectors import setup_detector, stop_detector
from cs_pilc_workers import PILCWorker
from cs_data_workers import DataSourceWorker, DetectorWorker, TimerWorker
from cs_data_collector import DataCollectorWorker
from cs_movement import SerialMovement
from cs_constants import *


# ----------------------------------------------------------------------
#
# ----------------------------------------------------------------------

class CCScan(CSScan):

    def __init__(self, macro, waypointGenerator=None, periodGenerator=None,
                 moveables=[], env={}, constraints=[], extrainfodesc=[], move_mode='normal'):
        super(CCScan, self).__init__(macro, waypointGenerator, periodGenerator,
                                     moveables, env, constraints, extrainfodesc)

        np.set_printoptions(precision=3)

        self._pilc_scan = False
        # general queue to report errors from all workers
        self._error_queue = Queue()

        # Thread to do motion (Sardana`s manager does not allow normal error handling)
        self._motion_thread = ExcThread(self._go_through_waypoints, 'motion_thread', self._error_queue)

        # corrected integration time after motors speed calculation
        self._integration_time = None

        # position to start, stop data collection as the direction of movement (+1 or -1)
        self._pos_start_measurements = None
        self._pos_stop_measurements = None
        self._movement_direction = None

        # list of movevvc commands
        self._command_lists = [[]]

        # start and stop (in case of "simple" movement) positions of motors (accounting to speed-up overhead)
        self._start_positions = [[]]

        # here we get the class, which provides us all movement functionality
        self.movement = SerialMovement(move_mode, self.macro, self._physical_moveables,
                                       [m.moveable.getName() for m in self.moveables], self._error_queue)

        if self.macro.mode == 'dscan':
            self._original_positions = self.movement.physical_motors_positions()

        # in case of "external crash" we should now do we have to stop all threads
        self._scan_in_process = False

        # to log the timing of all actions
        self._timing_logger = OrderedDict()

        # this list of workers will be read by DataCollector
        self._data_workers = []

        # Lambda has a personal worker, which joins Lambda and LambdaOnlineAnalysis
        self._2d_detectors = {}
        self._analysis_devices = []
        self._2d_detector_workers = {}
        _2d_detector_triggers = []

        # first we parse all channels in MG and see if we have Lambda
        for channel_info in self.measurement_group.getChannelsEnabledInfo():
            for name in DETECTOR_NAMES:
                if name in channel_info.label:
                    # for the first entry we need to prepare LambdaWorker
                    if name not in self._2d_detectors:
                        trigger = Queue()
                        self._2d_detector_workers[name] = DetectorWorker(name, trigger, self._error_queue, self.macro,
                                                                         self._timing_logger)
                        _2d_detector_triggers.append(trigger)
                        self._data_workers.append(self._2d_detector_workers[name])
                        self._2d_detectors[name] = get_tango_device(channel_info)
                    try:
                        analysis_device = self._2d_detector_workers[name].add_channel(channel_info)
                        if analysis_device is not None:
                            if analysis_device not in self._analysis_devices:
                                self._analysis_devices.append(analysis_device)
                    except:
                        raise RuntimeError(f'The {channel_info.label} detector is not supported in continuous scans')

        # we need to pass this trigger to timer worker, so we do it first
        _data_collector_trigger = Queue()

        timer_set = False
        if self.macro.pilc_mode: #PILC_MODE:
            self._pilc_scan = timer_set = self._setup_pilc(_2d_detector_triggers, _data_collector_trigger)
            if self._pilc_scan:
                self.macro.info('Cscan will be performed by PILC')

        if not timer_set:
            self._setup_tango_workers(_2d_detector_triggers, _data_collector_trigger)

        self._data_collector = DataCollectorWorker(self.macro, self._data_workers,
                                                   _data_collector_trigger, self._error_queue, self._extra_columns,
                                                   [m.moveable.getName() for m in self.moveables], self.data)

    # ----------------------------------------------------------------------
    def _setup_pilc(self, detector_triggers, _data_collector_trigger):

        try:
            self._timer_worker = PILCWorker(self.macro, _data_collector_trigger, detector_triggers,
                                            [m.moveable.getName() for m in self.moveables],
                                            self.measurement_group.getChannelsEnabledInfo(),
                                            self._error_queue)
        except CannotDoPilc:
            return False

        except Exception as err:
            self.macro.error(err)
            raise

        self._data_workers.append(self._timer_worker)

        return True

    # ----------------------------------------------------------------------
    def _setup_tango_workers(self, detector_triggers, _data_collector_trigger):

        # self._timing_logger['Total_point_time'] = []
        self._timing_logger['Point_preparation'] = []
        self._timing_logger['Point_dead_time'] = []
        self._timing_logger['Timer'] = []
        self._timing_logger['Data_collection'] = []
        self._timing_logger['Position_measurement'] = []

        num_counters = 0
        _worker_triggers = []

        # first we parse all channels in MG and see if we have Lambda
        for ind, channel_info in enumerate(self.measurement_group.getChannelsEnabledInfo()):
            detector = False
            for name in DETECTOR_NAMES:
                if name in channel_info.label:
                    detector = True
            if not detector:
                _worker_triggers.append(Queue())
                num_counters += 1

        # since Lambda should not be read instantly - we won't wait for it, so we start barrier only for the rest
        self.macro.report_debug('Starting barrier for {} workers'.format(num_counters))
        _workers_done_barrier = EndMeasurementBarrier(num_counters)

        ind = 0
        for channel_info in self.measurement_group.getChannelsEnabledInfo():
            # this is main timer
            for timer_prefix in TIMER_PREFIXES:
                if timer_prefix in channel_info.label:
                    self._timer_worker = TimerWorker(get_tango_device(channel_info), self._error_queue,
                                                     _worker_triggers + detector_triggers, _data_collector_trigger,
                                                     _workers_done_barrier, self.macro, self.movement,
                                                     self._timing_logger)

            # all others sources
            detector = False
            for name in DETECTOR_NAMES:
                if name in channel_info.label:
                    detector = True
            if not detector:
                self._timing_logger[channel_info.label] = []
                self._data_workers.append(DataSourceWorker(ind, channel_info, _worker_triggers[ind],
                                                           _workers_done_barrier, self._error_queue, self.macro,
                                                           self._timing_logger))
                ind += 1

    # ----------------------------------------------------------------------
    def calculate_speed(self, waypoint):

        if not self._pilc_scan:
            self.macro.report_debug('Additional point delay: {}'.format(ADDITIONAL_POINT_DELAY))
            travel_time = (waypoint["integ_time"] + ADDITIONAL_POINT_DELAY) * (waypoint["npts"] - 1)
        else:
            travel_time = waypoint["integ_time"] * (waypoint["npts"] - 1)

        for motor, start_position, final_position in zip(self._physical_moveables, waypoint['start_positions'],
                                                         waypoint['positions']):
            motor_travel_time = np.abs(final_position - start_position) / motor.velocity
            # recalculate cruise duration of motion at top velocity
            if motor_travel_time > travel_time:
                self.macro.warning('The {} motor cannot travel with such high speed'.format(motor.name))
                travel_time = motor_travel_time

        _reset_positions = False
        new_speed = []
        overhead_time = 0
        _main_motor = None
        for idx, (motor, start_position, final_position) in enumerate(zip(self._physical_moveables,
                                                                          waypoint['start_positions'],
                                                                          waypoint['positions'])):
            if start_position != final_position:
                new_speed.append(np.abs((final_position - start_position) / travel_time))
                overhead_time = max(overhead_time, motor.acceleration * new_speed[idx] / motor.velocity)
                if _main_motor is None:
                    _main_motor = idx
                    if self._pilc_scan:
                        self._timer_worker.setup_main_motor(motor.name, start_position)
                    else:
                        self.movement.setup_main_motor(idx)
                        self._movement_direction = np.sign(final_position - start_position)
                        self._pos_start_measurements = start_position
                        monitor_motor_speed = new_speed[-1]
                        self._pos_stop_measurements = final_position
                self.macro.output('Calculated speed for {} is {:.6f} (original: {:.6f})'.format(motor.name, new_speed[idx], motor.velocity))
            else:
                new_speed.append(0)

        if self._pilc_scan:
            motor = self._physical_moveables[_main_motor]
            acceleration_time = motor.acceleration * new_speed[_main_motor] / motor.velocity
            displacement = motor.velocity * np.square(acceleration_time) / (2 * motor.acceleration) \
                                         + new_speed[_main_motor] * (overhead_time - acceleration_time)

            if displacement < PILC_MINIMUM_DISPLACEMENT:
                self.macro.report_debug('overhead_time need corrections due to small start displacement')
                overhead_time += (PILC_MINIMUM_DISPLACEMENT - displacement)/new_speed[_main_motor]

        self.macro.report_debug('overhead_time {:.5f}'.format(overhead_time))

        for idx, (motor, start_position, final_position) in enumerate(zip(self._physical_moveables,
                                                                          waypoint['start_positions'],
                                                                          waypoint['positions'])):

            disp_sign = np.sign(final_position - start_position)
            acceleration_time = motor.acceleration * new_speed[idx] / motor.velocity
            self.macro.report_debug('{} acceleration_time {:.5f}'.format(motor.name, acceleration_time))

            displacement_reach_max_vel = motor.velocity * np.square(acceleration_time) / (2 * motor.acceleration)

            new_initial_pos = start_position - disp_sign * (displacement_reach_max_vel + new_speed[idx] *
                                                            (overhead_time - acceleration_time))

            self.macro.report_debug('original start {:.5f}, corrected {:.5f}'.format(start_position, new_initial_pos))

            if self._check_motor_limits(motor, new_initial_pos):
                self._start_positions[0].append(new_initial_pos)
            else:
                _reset_positions = True
                break

            new_final_pos = final_position + disp_sign * displacement_reach_max_vel
            self.macro.report_debug('original stop {:.5f}, corrected {:.5f}'.format(final_position, new_final_pos))
            if self._check_motor_limits(motor, new_final_pos):
                if start_position != final_position:
                    self._command_lists[0].append(([new_speed[idx], np.round(new_final_pos, POSITION_ROUND)],))
                else:
                    self._command_lists[0].append(None)
            else:
                _reset_positions = True
                break

        if _reset_positions:
            self._command_lists = [[]]
            self._start_positions = [[]]
            for idx, (start_position, final_position, _) in enumerate(zip(waypoint['start_positions'],
                                                                          waypoint['positions'])):
                self._start_positions[0].append(start_position)
                if start_position != final_position:
                    self._command_lists[0].append(([new_speed[idx], final_position],))
                else:
                    self._command_lists[0].append(None)

        for motor, start, cmd in zip(self._physical_moveables, self._start_positions[0], self._command_lists[0]):
            if cmd is not None:
                self.macro.output('Calculated positions for {}: start: {:.5f}, stop: {:.5f}'.format(motor.name,
                                                                                                    start, cmd[0][1]))

        if not self._pilc_scan:
            _integration_time_correction = travel_time / \
                                           ((waypoint["integ_time"] + ADDITIONAL_POINT_DELAY) * (waypoint["npts"] - 1))
        else:
            _integration_time_correction = travel_time / (waypoint["integ_time"] * (waypoint["npts"] - 1))

        self._integration_time = waypoint["integ_time"] * _integration_time_correction

        if not self._pilc_scan:
            self._pos_start_measurements -= self._movement_direction * monitor_motor_speed * self._integration_time / 2

        if _integration_time_correction > 1:
            self.macro.warning(
                'Integration time was corrected to {} due to slow motor(s)'.format(self._integration_time))

        if self._pilc_scan:
            self._timer_worker.set_npts(waypoint["npts"])

    # ----------------------------------------------------------------------
    def _check_motor_limits(self, motor, destination):

        if hasattr(motor, 'UnitLimitMin') and hasattr(motor, 'UnitLimitMax'):
            if motor.UnitLimitMin <= destination <= motor.UnitLimitMax:
                return True
            else:
                if not self.macro.ask_user("""The calculated overhead position {} to sync movement of {} is out of the limits. 
                    The scan can be executed only in non-sync mode.Continue?""".format(destination, motor)):
                    raise RuntimeError('Abort scan')
                return False
        else:
            return True

    # ----------------------------------------------------------------------
    def _calibrate_pilcs_encoders(self):
        for motor_name, encoder_data in PILC_MOTORS_MAP.items():
            try:
                tng_addr = self.macro.getMotor(motor_name).full_name.replace('tango://', '')
                getattr(PyTango.DeviceProxy(PILC_TRIGGERS[encoder_data['device']]),
                        'CalibrateEncoder{}'.format(encoder_data['encoder']))(PyTango.DeviceProxy(tng_addr).Position)
            except Exception as err:
                self.macro.report_debug('Cannot calibrate {} at PILC {}: {}'.format(encoder_data['encoder'],
                                                                                    encoder_data['device'], err))
                raise

    # ----------------------------------------------------------------------
    def _go_through_waypoints(self):
        """
        Internal, unprotected method to go through the different waypoints.
        """
        self.macro.report_debug("_go_through_waypoints() entering...")

        # get integ_time for this loop
        if old_python:
            _, step_info = self.period_steps.next()
        else:
            _, step_info = next(self.period_steps)

        self._data_collector.set_new_step_info(step_info)
        self._timer_worker.set_new_period(self._integration_time)

        setup_detector(self._2d_detectors, self._analysis_devices, self.macro, self._pilc_scan, self._integration_time)

        for worker in self._2d_detector_workers.values():
            worker.set_timeout(self._integration_time)

        if self.macro.isStopped():
            return

        for part, (start, cmd_list) in enumerate(zip(self._start_positions, self._command_lists)):
            # move to start position
            if len(self._start_positions) > 1:
                self.macro.output("Section {} of {}: moving to start position".format(part + 1,
                                                                                      len(self._start_positions)))
            self.movement.move_full_speed(start)

            if self.macro.isStopped():
                return

            self._timer_worker.resume()
            self.motion_event.set()
            # move to waypoint end position
            if len(self._start_positions) > 1:
                self.macro.output("Section {} of {}: start scan movement".format(part + 1, len(self._start_positions)))

            self.movement.move_slowed(cmd_list, monitor=self.macro.motion_monitor)

            self._timer_worker.pause()

        self.motion_event.clear()
        self.macro.report_debug("Clear motion event, state: {}".format(self.motion_event.is_set()))

    # ----------------------------------------------------------------------
    def scan_loop(self):

        self.macro.report_debug("scan loop() entering...")

        self._scan_in_process = True
        if self._pilc_scan:
            self._calibrate_pilcs_encoders()

        if hasattr(self.macro, 'getHooks'):
            for hook in self.macro.getHooks('pre-move-hooks'):
                hook()

            for hook in self.macro.getHooks('pre-acq'):
                hook()

        self._motion_thread.start()

        if self._pilc_scan:
            self._pilc_loop()
        else:
            self._tango_loop()

        self._finish_scan()

        yield 100

    # --------------------------------------------------------------------
    def _check_for_errors(self):
        try:
            err = self._error_queue.get(block=False)
        except empty_queue:
            return False
        else:
            self.macro.error(f'Error during script: {err[1][1]}')
            tr_to_print = ''.join(traceback.format_tb(err[1][2]))
            self.macro.report_debug(f'Thread {err[0]} got an exception {err[1][1]}\n {tr_to_print}')
            return True

    # ----------------------------------------------------------------------
    def _pilc_loop(self):

        self.macro.report_debug("start PiLCs...")
        self._timer_worker.start()

        self.macro.report_debug("waiting for motion event")
        while not self.motion_event.is_set():
            if self.macro.isStopped():
                return

            if self._check_for_errors():
                return

            time.sleep(REFRESH_PERIOD)

        while self.motion_event.is_set() and not self._timer_worker.is_done():

            if self.macro.isStopped():
                break

            if self._check_for_errors():
                break

        self.macro.report_debug("Scan done, motion {}, timer {}".format(self.motion_event.is_set(),
                                                                        self._timer_worker.is_done()))

    # ----------------------------------------------------------------------
    def _tango_loop(self):

        def _check_position(wait_position):
            if self._movement_direction > 0:
                return self.movement.get_main_motor_position() >= wait_position
            else:
                return self.movement.get_main_motor_position() <= wait_position

        self.macro.report_debug("waiting for motion event")
        # wait for motor to reach start position

        while not self.motion_event.is_set():
            if self.macro.isStopped():
                return

            if self._check_for_errors():
                return

            time.sleep(REFRESH_PERIOD)

        _last_notice = time.time()

        # wait for motor to reach max velocity
        self.macro.report_debug("wait for motors to pass start point")

        while not _check_position(self._pos_start_measurements) and self.motion_event.is_set():
            time.sleep(REFRESH_PERIOD)
            if time.time() - _last_notice > 5:
                _last_notice = time.time()
                self.macro.report_debug("current main motor position {}, waiting for {}".format(
                    self.movement.get_main_motor_position(), self._pos_start_measurements))

            if self._check_for_errors():
                return

        self._timer_worker.start()

        while self.motion_event.is_set():

            # allow scan to stop
            if self.macro.isStopped():
                break

            if self._check_for_errors():
                break

            if _check_position(self._pos_stop_measurements):
                break


        self.macro.report_debug('scan finished {} {} {}'.format(self._movement_direction,
                                                         self.movement.get_main_motor_position(),
                                                         self._pos_stop_measurements))
    # ----------------------------------------------------------------------
    def _finish_scan(self):

        if self.movement.is_moving():
            self.macro.report_debug('Stopping motion')
            self.movement.stop_move()

        # Get last started point and set it to data collector
        _last_point_to_read = self._timer_worker.stop()
        self._data_collector.set_last_point(_last_point_to_read)
        self.macro.report_debug(f"waiting for data collector finishes {_last_point_to_read} points")
        self._data_collector.stop(self._integration_time)
        self.macro.report_debug('Data collector state: {}'.format(self._data_collector.status))

        for worker in self._data_workers:
            worker.stop()

        try:
            if hasattr(self.macro, 'getHooks'):
                for hook in self.macro.getHooks('post-acq'):
                    hook()
                for hook in self.macro.getHooks('post-move'):
                    hook()
                for hook in self.macro.getHooks('post-scan'):
                    hook()
        except:
            pass

        env = self._env
        env['acqtime'] = self._data_collector.last_collected_point * self._integration_time
        env['delaytime'] = time.time() - env['acqtime']

        if self.motion_event.is_set():
            self.macro.report_debug("Waiting for motion to end...")

        while self.motion_event.is_set():
            time.sleep(MOTOR_POSITION_REFRESH_PERIOD)

        self.macro.report_debug("scan loop finished")

        self._scan_in_process = False

        if self.macro.timeme and not self._pilc_scan:
            data_to_save = np.arange(self._data_collector.last_collected_point)
            header = 'Point;'
            for name, values in self._timing_logger.items():
                try:
                    median = np.median(values)
                    if name not in ['Lambda', 'Timer', 'Data_collection', 'Total_point_time'] and median > 1e-2:
                        self.macro.warning('ATTENTION!!! SLOW DETECTOR!!!! {:s}: median {:.1f} max {:.1f}'.format(name,
                                                                                                                  median * 1e3,
                                                                                                                  np.max(
                                                                                                                      values) * 1e3))
                    else:
                        self.macro.output('{:s}: median {:.4f} max {:.4f}'.format(name, np.median(values) * 1e3,
                                                                                  np.max(values) * 1e3))
                    data_to_save = np.vstack(
                        (data_to_save, np.array(values[:self._data_collector.last_collected_point])))
                    header += name + ';'
                except:
                    pass

            file_name = os.path.join(self.macro.getEnv('ScanDir'),
                                     'time_log_' + str(self.macro.getEnv('ScanID')) + '.tlog')

            data_to_save = np.transpose(data_to_save)
            np.savetxt(file_name, data_to_save, delimiter=';', newline='\n', header=header)
            self.macro.info('Detector timing log saved in {}'.format(file_name))

        self.macro.report_debug("_finish_scan() done")

        self.macro.flushOutput()

    # ----------------------------------------------------------------------
    def do_restore(self):
        if self._scan_in_process:
            self._finish_scan()

        if self.movement.is_moving():
            self.macro.report_debug('Stopping motion')
            self.movement.stop_move()

        self.macro.report_debug('Resetting motors')
        self._restore_motors()

        if os.path.exists(TMP_FILE):
            os.remove(TMP_FILE)

        stop_detector(self._2d_detectors, self._analysis_devices, self.macro)

        if self.macro.mode == 'dscan':
            self.macro.output("Returning to start positions {}".format(self._original_positions))
            self.movement.move_full_speed(self._original_positions)

        self.movement.close()

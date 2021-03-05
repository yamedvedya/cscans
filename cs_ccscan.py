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

from sardana.macroserver.macro import macro
from sardana.macroserver.scan import CSScan

# cscans imports, always reloaded to track changes

from cs_axillary_functions import EndMeasurementBarrier, ExcThread, CannotDoPilc, get_tango_device
from cs_pilc_workers import PILCWorker
from cs_data_workers import DataSourceWorker, LambdaRoiWorker, TimerWorker
from cs_data_collector import DataCollectorWorker
from cs_movement import ParallelMovement, SerialMovement
from cs_constants import *

# ----------------------------------------------------------------------
#
# ----------------------------------------------------------------------

class CCScan(CSScan):

    def __init__(self, macro, waypointGenerator=None, periodGenerator=None,
                 moveables=[], env={}, constraints=[], extrainfodesc=[]):
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
        if MOTOR_MOVEMENT_TYPE == 'serial':
            self.movement = SerialMovement(self.macro, self._physical_moveables,
                                             [m.moveable.getName() for m in self.moveables], self._error_queue)
        else:
            self.movement = ParallelMovement(self.macro, self._physical_moveables,
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
        self._has_lambda = False
        self._lambda_worker = None
        _lambda_trigger = []

        # first we parse all channels in MG and see if we have Lambda
        for channel_info in self.measurement_group.getChannelsEnabledInfo():
            if 'lmbd' in channel_info.label:
                # for the first entry we need to prepare LambdaWorker
                if self._lambda_worker is None:
                    _lambda_trigger = [Queue()]
                    self._timing_logger['Lambda'] = []
                    self._has_lambda = True
                    self._original_trigger_mode, self._original_operating_mode = None, None
                    self._lambda_worker = LambdaRoiWorker(_lambda_trigger[0], self._error_queue, self.macro,
                                                          self._timing_logger)
                    self._data_workers.append(self._lambda_worker)

                if 'lmbd_countsroi' in channel_info.label or channel_info.label == 'lmbd':
                    self._lambda_worker.add_channel(channel_info)
                else:
                    raise RuntimeError('The {} detector is not supported in continuous scans'.format(channel_info.label))

        #we need to pass this trigger to timer worker, so we do it first
        _data_collector_trigger = Queue()

        timer_set = False
        if PLIC_MODE:
            self._pilc_scan = timer_set = self._setup_pilc(_lambda_trigger, _data_collector_trigger)

        if not timer_set:
            self._setup_tango_workers(_lambda_trigger, _data_collector_trigger)

        self._data_collector = DataCollectorWorker(self.macro, self._data_workers,
                                                   _data_collector_trigger, self._error_queue, self._extra_columns,
                                                   [m.moveable.getName() for m in self.moveables], self.data)

    # ----------------------------------------------------------------------
    def _setup_pilc(self, _lambda_trigger, _data_collector_trigger):

        try:
            self._timer_worker = PILCWorker(self.macro, _data_collector_trigger,
                                            _lambda_trigger[0] if len(_lambda_trigger) else None,
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
    def _setup_tango_workers(self, _lambda_trigger, _data_collector_trigger):

        self._timing_logger['Point_dead_time'] = []
        self._timing_logger['Timer'] = []
        self._timing_logger['Data_collection'] = []
        self._timing_logger['Position_measurement'] = []

        num_counters = 0
        _worker_triggers = []

        # first we parse all channels in MG and see if we have Lambda
        for ind, channel_info in enumerate(self.measurement_group.getChannelsEnabledInfo()):
            if not 'lmbd' in channel_info.label:
                _worker_triggers.append(Queue())
                num_counters += 1

        # since Lambda should not be read instantly - we won't wait for it, so we start barrier only for the rest
        self.macro.report_debug('Starting barrier for {} workers'.format(num_counters))
        _workers_done_barrier = EndMeasurementBarrier(num_counters)

        ind = 0
        for channel_info in self.measurement_group.getChannelsEnabledInfo():
            # this is main timer
            if 'eh_t' in channel_info.label:
                self._timer_worker = TimerWorker(get_tango_device(channel_info), self._error_queue,
                                                 _worker_triggers + _lambda_trigger, _data_collector_trigger,
                                                 _workers_done_barrier, self.macro, self.movement,
                                                 self._timing_logger)

            # all others sources
            if not 'lmbd' in channel_info.label:
                self._timing_logger[channel_info.label] = []
                self._data_workers.append(DataSourceWorker(ind, channel_info, _worker_triggers[ind],
                                                           _workers_done_barrier, self._error_queue, self.macro,
                                                           self._timing_logger))
                ind += 1

    # ----------------------------------------------------------------------
    def _setup_lambda(self, integ_time):

        ### Here we setup Lambda: first we check that Lambda and LambdaOnlineAnalysis are not running,
        # if yes - trying to stop them (within TIMEOUT_LAMBDA)
        ###

        _lambda_proxy = PyTango.DeviceProxy(self.macro.getEnv('LambdaDevice'))
        if _lambda_proxy.State() != PyTango.DevState.ON:
            _lambda_proxy.StopAcq()
            _time_out = time.time()
            while _lambda_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_LAMBDA:
                time.sleep(0.1)
                self.macro.checkPoint()

            if _lambda_proxy.State() != PyTango.DevState.ON:
                self.macro.output(_lambda_proxy.State())
                raise RuntimeError('Cannot stop LAMBDA')

        _lambda_proxy.TriggerMode = 2
        if self._pilc_scan:
            _lambda_proxy.ShutterTime = int(integ_time * 1000) - PILC_TRIGGER_TIME - PILC_LAMBDA_DELAY
        else:
            _lambda_proxy.ShutterTime = int(integ_time * 1000)

        _lambda_proxy.FrameNumbers = max(self.macro.nsteps, 1000)
        _lambda_proxy.StartAcq()

        _time_out = time.time()
        while _lambda_proxy.State() != PyTango.DevState.MOVING and time.time() - _time_out < TIMEOUT_LAMBDA:
            time.sleep(0.1)
            macro.checkPoint()

        if _lambda_proxy.State() != PyTango.DevState.MOVING:
            self.macro.output(_lambda_proxy.State())
            raise RuntimeError('Cannot start LAMBDA')

        self.macro.report_debug('LAMBDA state after setup: {}'.format(_lambda_proxy.State()))

        _lambdaonlineanalysis_proxy = PyTango.DeviceProxy(self.macro.getEnv('LambdaOnlineAnalysis'))
        if _lambdaonlineanalysis_proxy.State() == PyTango.DevState.MOVING:
            _lambdaonlineanalysis_proxy.StopAnalysis()

        _lambdaonlineanalysis_proxy.StartAnalysis()

        if _lambdaonlineanalysis_proxy.State() != PyTango.DevState.MOVING:
                raise RuntimeError('Cannot start LambdaOnlineAnalysis')

        self.macro.report_debug('LambdaOnLineAnalysis state after setup: {}'.format(_lambda_proxy.State()))

    # ----------------------------------------------------------------------
    def calculate_speed(self, waypoint):

        """ In case of ctscan - everything is kind of simple"""

        travel_time = waypoint["integ_time"] * (waypoint["npts"] - 1)

        for motor, start_position, final_position in zip(self._physical_moveables, waypoint['start_positions'],
                                                               waypoint['positions']):
            motor_travel_time = np.abs(final_position - start_position)/motor.velocity
            # recalculate cruise duration of motion at top velocity
            if motor_travel_time > travel_time:
                self.macro.warning('The {} motor cannot travel with such high speed'.format(motor.name))
                travel_time = motor_travel_time

        _reset_positions = False
        new_speed = []
        overhead_time = 0
        _main_motor_found = False
        for idx, (motor, start_position, final_position) in enumerate(zip(self._physical_moveables,
                                                                                waypoint['start_positions'],
                                                                                waypoint['positions'])):
            if start_position != final_position:
                new_speed.append(np.abs((final_position-start_position)/travel_time))
                overhead_time = max(overhead_time, motor.acceleration*new_speed[idx]/motor.velocity)
                if not _main_motor_found:
                    _main_motor_found = True
                    if self._pilc_scan:
                        self._timer_worker.setup_main_motor(motor.name, start_position)
                    else:
                        self.movement.setup_main_motor(idx)
                        self._movement_direction = np.sign(final_position-start_position)
                        self._pos_start_measurements = start_position
                        monitor_motor_speed = new_speed[-1]
                        self._pos_stop_measurements = final_position
                self.macro.output('Calculated speed for {} is {}'.format(motor.name, new_speed[idx]))
            else:
                new_speed.append(0)

        for idx, (motor, start_position, final_position) in enumerate(zip(self._physical_moveables,
                                                                          waypoint['start_positions'],
                                                                          waypoint['positions'])):

            disp_sign = np.sign(final_position-start_position)
            acceleration_time = motor.acceleration*new_speed[idx]/motor.velocity
            displacement_reach_max_vel = motor.velocity*np.square(acceleration_time)/(2*motor.acceleration)

            new_initial_pos = start_position - disp_sign*(displacement_reach_max_vel + new_speed[idx] *
                                                          (overhead_time - acceleration_time))

            if self._check_motor_limits(motor, new_initial_pos):
                self._start_positions[0].append(new_initial_pos)
            else:
                _reset_positions = True
                break

            new_final_pos = final_position + disp_sign * displacement_reach_max_vel
            self.macro.report_debug('new_final_pos {}'.format(new_final_pos))
            if self._check_motor_limits(motor, new_final_pos):
                if start_position != final_position:
                    self._command_lists[0].append(([new_speed[idx], np.round(new_final_pos, POSITION_ROUND)], ))
                else:
                    self._command_lists[0].append(None)
            else:
                _reset_positions =True
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

        _integration_time_correction = travel_time/(waypoint["integ_time"] * waypoint["npts"])
        self._integration_time = waypoint["integ_time"] * _integration_time_correction
        self._pos_start_measurements -= self._movement_direction * monitor_motor_speed * self._integration_time/2
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

        if self._has_lambda:
            self._setup_lambda(self._integration_time)
            self._lambda_worker.set_timeout(self._integration_time)

        if self.macro.isStopped():
            return

        for part, (start, cmd_list) in enumerate(zip(self._start_positions, self._command_lists)):
            # move to start position
            if len(self._start_positions) > 1:
                self.macro.output("Section {} of {}: moving to start position".format(part+1,
                                                                                      len(self._start_positions)))
            self.movement.move_full_speed(start)

            if self.macro.isStopped():
                return

            self._timer_worker.resume()
            self.motion_event.set()
            # move to waypoint end position
            if len(self._start_positions) > 1:
                self.macro.output("Section {} of {}: start scan movement".format(part+1, len(self._start_positions)))

            self.movement.movevcc(cmd_list, monitor=self.macro.motion_monitor)

            self._timer_worker.pause()

        self.motion_event.clear()
        self.macro.report_debug("Clear motion event, state: {}".format(self.motion_event.is_set()))

    # ----------------------------------------------------------------------
    def scan_loop(self):

        self.macro.report_debug("scan loop() entering...")

        self._scan_in_process = True

        if hasattr(self.macro, 'getHooks'):
            for hook in self.macro.getHooks('pre-scan'):
                hook()

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

    # ----------------------------------------------------------------------
    def _pilc_loop(self):

        self.macro.report_debug("start PiLCs...")
        self._timer_worker.start()

        self.macro.report_debug("waiting for motion event")
        self.motion_event.wait()

        while self.motion_event.is_set() and not self._timer_worker.is_done():

            if self.macro.isStopped():
                break

            try:
                err = self._error_queue.get(block=False)
            except empty_queue:
                pass
            else:
                self.macro.report_debug('Thread {} got an exception {} at line {}'.format(err[0], err[1],
                                                                                          err[2].tb_lineno))
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
        self.motion_event.wait()

        _last_notice = time.time()

        _contiune = True

        # wait for motor to reach max velocity
        self.macro.report_debug("wait for motors to pass start point")

        while not _check_position(self._pos_start_measurements) and self.motion_event.is_set():
            time.sleep(REFRESH_PERIOD)
            if time.time() - _last_notice > 5:
                _last_notice = time.time()
                self.macro.report_debug("current main motor position {}, waiting for {}".format(
                    self.movement.get_main_motor_position(), self._pos_start_measurements))

            try:
                err = self._error_queue.get(block=False)
            except empty_queue:
                pass
            else:
                if err is list:
                    self.macro.report_debug('Thread {} got an exception {} at line {}'.format(err[0], err[1],
                                                                                              err[2].tb_lineno))
                else:
                    self.macro.report_debug('Got an exception {} '.format(err))
                _contiune = False
                break

        if _contiune:
            self._timer_worker.start()

            while self.motion_event.is_set():

                # allow scan to stop
                if self.macro.isStopped():
                    break

                try:
                    err = self._error_queue.get(block=False)
                except empty_queue:
                    pass
                else:
                    if err is list:
                        self.macro.report_debug('Thread {} got an exception {} at line {}'.format(err[0], err[1],
                                                                                                  err[2].tb_lineno))
                    else:
                        self.macro.report_debug('Got an exception {} '.format(err))

                    break

                if _check_position(self._pos_stop_measurements):
                    break


            self.macro.report_debug('scan finished {} {} {}'.format(self._movement_direction,
                                                             self.movement.get_main_motor_position(),
                                                             self._pos_stop_measurements))
    # ----------------------------------------------------------------------
    def _finish_scan(self):

        # Get last started point and set it to data collector
        self._data_collector.set_last_point(self._timer_worker.stop())

        self.macro.report_debug("waiting for data collector finishes")

        self.macro.report_debug('Data collector state: {}'.format(self._data_collector.status))
        _timeout_start_time = time.time()
        while time.time() < _timeout_start_time + TIMEOUT and not self._data_collector.is_finished():
            time.sleep(self._integration_time)

        if not self._data_collector.is_finished():
            self.macro.report_debug("Cannot stop DataCollector, waits for {}, collected {}".format(
                self._data_collector._stop_after, self._data_collector.last_collected_point))
            self._data_collector.stop()

        for worker in self._data_workers:
            worker.stop()

        if hasattr(self.macro, 'getHooks'):
            for hook in self.macro.getHooks('post-acq'):
                hook()
            for hook in self.macro.getHooks('post-move'):
                hook()
            for hook in self.macro.getHooks('post-scan'):
                hook()

        env = self._env
        env['acqtime'] = self._data_collector.last_collected_point*self._integration_time
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
                    if name not in ['Lambda', 'Timer', 'Data_collection'] and median > 1e-2:
                        self.macro.warning('ATTENTION!!! SLOW DETECTOR!!!! {:s}: median {:.1f} max {:.1f}'.format(name,
                                                                                                                  median*1e3,
                                                                                                                  np.max(values)*1e3))
                    else:
                        self.macro.output('{:s}: median {:.4f} max {:.4f}'.format(name, np.median(values)*1e3, np.max(values)*1e3))
                    data_to_save = np.vstack((data_to_save, np.array(values[:self._data_collector.last_collected_point])))
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

        self.macro.report_debug('Stopping motion')
        self.movement.stop_move()

        self.macro.report_debug('Resetting motors')
        self._restore_motors()

        if self._has_lambda:
            self.macro.report_debug('Stopping LambdaOnlineAnalysis')
            _lambdaonlineanalysis_proxy = PyTango.DeviceProxy(self.macro.getEnv('LambdaOnlineAnalysis'))
            _lambdaonlineanalysis_proxy.StopAnalysis()
            time.sleep(0.1)

            _time_out = time.time()
            while _lambdaonlineanalysis_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_LAMBDA:
                time.sleep(0.1)

            if _lambdaonlineanalysis_proxy.State() != PyTango.DevState.ON:
                self.macro.output('Cannot stop LambdaOnlineAnalysis!')

            self.macro.report_debug('Stopping Lambda')

            _lambda_proxy = PyTango.DeviceProxy(self.macro.getEnv('LambdaDevice'))
            _lambda_proxy.StopAcq()

            _time_out = time.time()
            while _lambda_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_LAMBDA:
                time.sleep(0.1)

            if _lambda_proxy.State() == PyTango.DevState.ON:
                _lambda_proxy.FrameNumbers = 1
                _lambda_proxy.TriggerMode = 0
            else:
                self.macro.output('Cannot reset Lambda! Check the settings.')

        if self.macro.mode == 'dscan':
            self.macro.output("Returning to start positions {}".format(self._original_positions))
            self.movement.move_full_speed(self._original_positions)

        self.movement.close()
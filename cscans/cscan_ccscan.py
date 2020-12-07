'''
This is child of CSScan with modified functionality

Author yury.matveev@desy.de
'''

# general python imports
import time
import os
import PyTango
import numpy as np
from collections import OrderedDict

from Queue import Queue
from Queue import Empty as empty_queue

# Sardana imports

from sardana.macroserver.macro import macro
from sardana.macroserver.scan import CSScan
from sardana.util.motion import Motor as VMotor
from sardana.util.motion import MotionPath
from sardana.macroserver.scan.gscan import ScanException

# cscan imports, always reloaded to track changes

from cscan_axillary_functions import EndMeasurementBarrier, ExcThread
from cscan_data_workers import DataSourceWorker, LambdaRoiWorker, TimerWorker
from cscan_data_collector import DataCollectorWorker
from cscan_constants import *

# ----------------------------------------------------------------------
#
# ----------------------------------------------------------------------

class CCScan(CSScan):

    def __init__(self, macro, waypointGenerator=None, periodGenerator=None,
                 moveables=[], env={}, constraints=[], extrainfodesc=[]):
        super(CCScan, self).__init__(macro, waypointGenerator, periodGenerator,
                 moveables, env, constraints, extrainfodesc)

        # Parsing measurement group:

        self._finished = False

        self._has_lambda = False
        self._integration_time_correction = 1

        self._integration_time = None
        self._acq_duration = None

        self._position_start = None
        self._position_stop = None
        self._movement_direction = None
        self._motion_paths = []

        try:
            self._main_motor = PyTango.DeviceProxy(self._physical_moveables[0].TangoDevice)
        except:
            self._main_motor = self._physical_moveables[0]

        self._timing_logger = OrderedDict()
        self._timing_logger['Point_dead_time'] = []
        self._timing_logger['Timer'] = []
        self._timing_logger['Data_collection'] = []
        self._timing_logger['Position_measurement'] = []

        self._scan_in_process = False

        num_counters = 0
        _worker_triggers = []
        _lambda_trigger = []
        _lambda_worker = None
        _counters = []

        for ind, channel_info in enumerate(self.measurement_group.getChannelsEnabledInfo()):
            if not 'lmbd' in channel_info.label:
                _worker_triggers.append(Queue())
                num_counters += 1
            else:
                _lambda_trigger = [Queue()]
                self._timing_logger['Lambda'] = []
                self._has_lambda = True
                self._original_trigger_mode, self._original_operating_mode = None, None

        if self.macro.debug_mode:
            self.macro.debug('Starting barrier for {} workers'.format(num_counters))
        _workers_done_barrier = EndMeasurementBarrier(num_counters)

        self._error_queue = Queue()
        # DataWorkers array
        self._data_workers = []

        if self._has_lambda:
            _lambda_worker = LambdaRoiWorker(_lambda_trigger[0], self._error_queue, self.macro, self._timing_logger)
            self._data_workers.append(_lambda_worker)

        #We start main data collector loop
        _data_collector_trigger = Queue()
        self._data_collector = DataCollectorWorker(self.macro, self._data_workers,
                                                   _data_collector_trigger, self._error_queue, self._extra_columns,
                                                   self.moveables, self.data)

        ind = 0
        for channel_info in self.measurement_group.getChannelsEnabledInfo():
            if 'eh_t' in channel_info.label:
                self._timer_worker = TimerWorker(timer_names[channel_info.label], self._error_queue,
                                                 _worker_triggers + _lambda_trigger, _data_collector_trigger,
                                                 _workers_done_barrier, self.macro, self._physical_moveables,
                                                 self._timing_logger)

            if 'lmbd' in channel_info.label:
                if 'lmbd_countsroi' in channel_info.label or channel_info.label == 'lmbd':
                    _lambda_worker.add_channel(channel_info)
                else:
                    raise RuntimeError('The {} detector is not supported in continuous scans'.format(channel_info.label))

            else:
                self._timing_logger[channel_info.label] = []
                self._data_workers.append(DataSourceWorker(channel_info, _worker_triggers[ind],
                                                           _workers_done_barrier, self._error_queue, self.macro,
                                                           self._timing_logger))
                ind += 1

        if self.macro.debug_mode:
            self.macro.debug("__init__ finished")

    # ----------------------------------------------------------------------
    def _setup_lambda(self, integ_time):

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
        _lambda_proxy.ShutterTime = integ_time*1000
        _lambda_proxy.FrameNumbers = max(self.macro.nsteps, 1000)
        _lambda_proxy.StartAcq()

        _time_out = time.time()
        while _lambda_proxy.State() != PyTango.DevState.MOVING and time.time() - _time_out < TIMEOUT_LAMBDA:
            time.sleep(0.1)
            macro.checkPoint()

        if _lambda_proxy.State() != PyTango.DevState.MOVING:
            self.macro.output(_lambda_proxy.State())
            raise RuntimeError('Cannot start LAMBDA')

        if self.macro.debug_mode:
            self.macro.debug('LAMBDA state after setup: {}'.format(_lambda_proxy.State()))

        _lambdaonlineanalysis_proxy = PyTango.DeviceProxy(self.macro.getEnv('LambdaOnlineAnalysis'))
        if _lambdaonlineanalysis_proxy.State() == PyTango.DevState.MOVING:
            _lambdaonlineanalysis_proxy.StopAnalysis()

        _lambdaonlineanalysis_proxy.StartAnalysis()

        if _lambdaonlineanalysis_proxy.State() != PyTango.DevState.MOVING:
                raise RuntimeError('Cannot start LambdaOnlineAnalysis')

        if self.macro.debug_mode:
            self.macro.debug('LambdaOnLineAnalysis state after setup: {}'.format(_lambda_proxy.State()))

     # ----------------------------------------------------------------------
    def prepare_waypoint(self, waypoint):
        ### This function basically repeats the original, The only difference is that "slow_down" factor changed
        ### to fixed travel time, defined by waypoint['integ_time']*waypoint['npts']
        ### some variable were renamed to make code more readable

        if self.macro.debug_mode:
            self.macro.debug("prepare_waypoint() entering...")

        travel_time = waypoint["integ_time"] * waypoint["npts"] * 1.1

        original_duration, self._acq_duration, overhead_time = travel_time, travel_time, 0

        _can_be_synchro = True
        for moveable, start_position, final_position in zip(self.moveables, waypoint['start_positions'], waypoint['positions']):
            motor = moveable.moveable

            try:
                base_vel = motor.getBaseRate()
            except AttributeError:
                base_vel = 0
                _can_be_synchro = False

            try:
                motor_vel = motor.getVelocity()
            except AttributeError:
                raise RuntimeError("{} don't have velocity parameter, cscans is impossible".format(motor))

            # Find the cruise duration of motion at top velocity. For this
            # create a virtual motor which has instantaneous acceleration and
            # deceleration

            # create a path which will tell us which is the cruise
            # duration of this motion at top velocity
            motor_path = MotionPath(VMotor(min_vel=base_vel, max_vel=motor_vel,
                                  accel_time=0, decel_time=0), start_position, final_position)
            motor_path.physical_motor = moveable.moveable

            # recalculate cruise duration of motion at top velocity
            if motor_path.duration > self._acq_duration:
                if self.macro.debug_mode:
                    self.macro.debug(
                        'Ideal path duration: {}, requested duration: {}'.format(motor_path.duration, original_duration))
                self.macro.warning(
                        'The required travel time cannot be reached due to {} motor cannot travel with such high speed'.format(
                            moveable.name))

                self._acq_duration = motor_path.duration

            self._motion_paths.append(motor_path)

        # now that we have the appropriate top velocity for all motors, the
        # cruise duration of motion at top velocity, and the time it takes to
        # recalculate
        _reset_positions = False
        for path in self._motion_paths:
            vmotor = path.motor
            # in the case of pseudo motors or not moving a motor...
            if path.displacement != 0:
                motor = path.physical_motor

                old_top_velocity = motor.getVelocity()
                new_top_vel = path.displacement / self._acq_duration
                vmotor.setMaxVelocity(new_top_vel)

                self.macro.output('The speed of {} motor will be changed from {} to {}'.format(motor,
                                                                                               old_top_velocity,
                                                                                               new_top_vel))

                try:
                    old_accel = motor.getAcceleration()
                    accel_time =  old_accel* new_top_vel / old_top_velocity # to keep acceleration in steps
                    self.macro.output('The acceleration of {} motor will be changed from {} to {}'.format(motor,
                                                                                                   old_accel,
                                                                                                   accel_time))
                    vmotor.setAccelerationTime(accel_time)
                    overhead_time = max(overhead_time, accel_time)

                    old_decel = motor.getDeceleration()
                    decel_time = old_decel * new_top_vel / old_top_velocity # to keep acceleration in steps
                    vmotor.setDecelerationTime(decel_time)
                    self.macro.output('The deceleration of {} motor will be changed from {} to {}'.format(motor,
                                                                                                   old_decel,
                                                                                                   decel_time))

                    if not _can_be_synchro:
                        self.macro.warning("{} motion will not be coordinated".format(motor))
                        self.macro._sync = False

                except AttributeError:
                    self.macro.warning("{} motion don't have acceleration/deceleration time".format(motor))
                    self.macro._sync = False

                if self.macro._sync:
                    disp_sign = 1 if path.positive_displacement else -1
                    new_initial_pos = path.initial_user_pos - disp_sign*(vmotor.displacement_reach_max_vel +
                                                                         new_top_vel * (overhead_time - accel_time))
                    if self._check_motor_limits(motor, new_initial_pos):
                        path.setInitialUserPos(new_initial_pos)
                    else:
                        _reset_positions = True

                    new_final_pos = path.final_user_pos + \
                        disp_sign * vmotor.displacement_reach_min_vel
                    if self._check_motor_limits(motor, new_final_pos):
                        path.setFinalUserPos(new_final_pos)
                    else:
                        _reset_positions = True

        if _reset_positions:
            for path, original_start, original_stop in zip(self._motion_paths, waypoint['start_positions'],
                                                           waypoint['positions']):
                path.setInitialUserPos(original_start)
                path.setFinalUserPos(original_stop)

        for path in self._motion_paths:
            self.macro.output('Calculated positions for motor {}: start: {}, stop: {}'.format(path.physical_motor,
                                                                                              path.initial_user_pos,
                                                                                              path.final_user_pos))

        _integration_time_correction = self._acq_duration/original_duration
        self._integration_time = waypoint["integ_time"] * _integration_time_correction
        if _integration_time_correction > 1:
            self.macro.warning(
                'Integration time was corrected to {} due to slow motor(s)'.format(self._integration_time))

        self._position_start = waypoint['start_positions'][0]
        self._position_stop = waypoint['positions'][0]
        self._movement_direction = self._motion_paths[0].positive_displacement

        return self._motion_paths

    # ----------------------------------------------------------------------
    def _check_motor_limits(self, motor, destination):
        if self.get_min_pos(motor) <= destination <= self.get_max_pos(motor):
            return True
        else:
            options = YES_OPTIONS + NO_OPTIONS
            run_or_not = self.macro.input(
                "The calculated overhead position {} to sync movement of {} is out of the limits. The scan can be executed only in non-sync mode.Continue?".format(destination, motor),
                data_type=options, allow_multiple=False, title="Favorites", default_value='Yes')
            if run_or_not in NO_OPTIONS:
                raise RuntimeError('Abort scan')

            return False

    # ----------------------------------------------------------------------
    def _go_through_waypoints(self):
        """
        Internal, unprotected method to go through the different waypoints.
        """
        if self.macro.debug_mode:
            self.macro.debug("_go_through_waypoints() entering...")

        for _, waypoint in self.steps:

            # get integ_time for this loop
            _, step_info = self.period_steps.next()
            self._data_collector.set_new_step_info(step_info)
            self._timer_worker.set_new_period(self._integration_time)

            if self._has_lambda:
                self._setup_lambda(self._integration_time)

            # execute pre-move hooks
            for hook in waypoint.get('pre-move-hooks', []):
                hook()

            start_pos, final_pos = [], []
            for path in self._motion_paths:
                start_pos.append(path.initial_user_pos)
                final_pos.append(path.final_user_pos)

            if self.macro.debug_mode:
                self.macro.debug('Start pos: {}, final pos: {}'.format(start_pos, final_pos))
            if self.macro.isStopped():
                return

            # move to start position
            if self.macro.debug_mode:
                self.macro.debug("Moving to start position: {}".format(start_pos))
            self.motion.move(start_pos)

            if self.macro.isStopped():
                return

            # prepare motor(s) with the velocity required for synchronization
            for path in self._motion_paths:
                path.physical_motor.setVelocity(path.motor.getMaxVelocity())
                path.physical_motor.setAcceleration(path.motor.getAccelerationTime())
                path.physical_motor.setDeceleration(path.motor.getDecelerationTime())

            if self.macro.isStopped():
                return

            self.motion_event.set()

            # move to waypoint end position
            if self.macro.debug_mode:
                self.macro.debug("Moving to final position: {}".format(final_pos))
            self.motion.move(final_pos)

            self.motion_event.clear()
            # self.macro.output("Clear motion event, state: {}".format(self.motion_event.is_set()))

            # execute post-move hooks
            for hook in waypoint.get('post-move-hooks', []):
                hook()

    # ----------------------------------------------------------------------
    def scan_loop(self):

        if self.macro.debug_mode:
            self.macro.debug("scan loop() entering...")

        self._scan_in_process = True

        manager = self.macro.getManager()

        if hasattr(self.macro, 'getHooks'):
            for hook in self.macro.getHooks('pre-scan'):
                hook()

        if hasattr(self.macro, 'getHooks'):
            for hook in self.macro.getHooks('pre-acq'):
                hook()

        # start move & acquisition as close as possible
        # from this point on synchronization becomes critical
        # self._motor_mover.start()
        manager.add_job(self.go_through_waypoints)

        if self.macro.debug_mode:
            self.macro.debug("waiting for motion event")
        # wait for motor to reach start position
        self.motion_event.wait()

        # wait for motor to reach max velocity
        if self.macro.debug_mode:
            self.macro.debug("wait for motors to pass start point")

        if self._movement_direction:
            while self._main_motor.Position < self._position_start:
                time.sleep(REFRESH_PERIOD)
        else:
            while self._main_motor.Position > self._position_start:
                time.sleep(REFRESH_PERIOD)

        acq_start_time = time.time()
        self._data_collector.set_acq_start_time(acq_start_time)
        self._timer_worker.start()
        self.flushOutput()

        #after first point generate triggers every integ_time
        while self.motion_event.is_set() and not self._finished:

            # allow scan to stop
            if self.macro.isStopped():
                self._finished = True

            try:
                err = self._error_queue.get(block=False)
            except empty_queue:
                pass
            else:
                if self.macro.debug_mode:
                    self.macro.debug('Thread {} got an exception {} at line {}'.format(err[0], err[1], err[2].tb_lineno))
                self._finished = True

            if self._movement_direction:
                self._finished = self._timer_worker.last_position >= self._position_stop
            else:
                self._finished = self._timer_worker.last_position <= self._position_stop

        self._finish_scan()

        yield 100

    # ----------------------------------------------------------------------
    def _finish_scan(self):

        # Get last started point and set it to data collector
        self._data_collector.set_last_point(self._timer_worker.stop())

        if self.macro.debug_mode:
            self.macro.debug("waiting for data collector finishes")

        _timeout_start_time = time.time()
        while time.time() < _timeout_start_time + TIMEOUT and self._data_collector.status != 'finished':
            time.sleep(self._integration_time)

        if self._data_collector.status != 'finished':
            if self.macro.debug_mode:
                self.macro.debug("Cannot stop DataCollector, waits for {}, collected {}".format(
                    self._data_collector._stop_after, self._data_collector.last_collected_point))
            self._data_collector.stop()

        for worker in self._data_workers:
            worker.stop()

        if hasattr(macro, 'getHooks'):
            for hook in macro.getHooks('post-acq'):
                hook()
            for hook in macro.getHooks('post-scan'):
                hook()

        env = self._env
        env['acqtime'] = self._data_collector.last_collected_point*self._integration_time
        env['delaytime'] = time.time() - env['acqtime']

        if self.macro.debug_mode:
            self.macro.debug("scan loop finished")

        self._scan_in_process = False

        if self.macro.timeme:
            data_to_save = np.arange(self._data_collector.last_collected_point)
            header = 'Point;'
            for name, values in self._timing_logger.items():
                try:
                    median = np.median(values)
                    if name not in ['Lambda', 'Timer', 'Data_collection'] and median > 1e-2:
                        self._macro.warning('ATTENTION!!! SLOW DETECTOR!!!! {:s}: median {:.4f} max {:.4f}'.format(name, median, np.max(values)))
                    else:
                        self._macro.output('{:s}: median {:.4f} max {:.4f}'.format(name, np.median(values), np.max(values)))
                    data_to_save = np.vstack((data_to_save, np.array(values[:self._data_collector.last_collected_point])))
                    header += name + ';'
                except:
                    pass

            file_name = os.path.join(self._macro.getEnv('ScanDir'),
                                     'time_log_' + str(self._macro.getEnv('ScanID')) + '.tlog')

            data_to_save = np.transpose(data_to_save)
            np.savetxt(file_name, data_to_save, delimiter=';', newline='\n', header=header)
            self.macro.info('Detector timing log saved in {}'.format(file_name))

        if self.macro.debug_mode:
            self.macro.debug("_finish_scan() done")

        self.macro.flushOutput()

    # ----------------------------------------------------------------------
    def do_restore(self):
        if self._scan_in_process:
            self._finish_scan()

        if self.macro.debug_mode:
            self.macro.debug('Stopping motion')
        self.motion.stop()
        if self.macro.debug_mode:
            self.macro.debug('Resetting motors')
        self._restore_motors()

        if self._has_lambda:
            if self.macro.debug_mode:
                self.macro.debug('Stopping LambdaOnlineAnalysis')
            _lambdaonlineanalysis_proxy = PyTango.DeviceProxy(self.macro.getEnv('LambdaOnlineAnalysis'))
            _lambdaonlineanalysis_proxy.StopAnalysis()
            time.sleep(0.1)

            _time_out = time.time()
            while _lambdaonlineanalysis_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_LAMBDA:
                time.sleep(0.1)

            if _lambdaonlineanalysis_proxy.State() != PyTango.DevState.ON:
                self.macro.output('Cannot stop LambdaOnlineAnalysis!')

            if self.macro.debug_mode:
                self.macro.debug('Stopping Lambda')

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

        try:
            if hasattr(self.macro, 'do_restore'):
                self.macro.do_restore()
        except Exception:
            msg = ("Failed to execute 'do_restore' method of the %s macro" %
                   self.macro.getName())
            if self.macro.debug_mode:
                self.macro.debug(msg)
                self.macro.debug('Details: ', exc_info=True)
            raise ScanException('error while restoring a backup')

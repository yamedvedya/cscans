'''
This is child of CSScan with modified functionality

Author yury.matveev@desy.de
'''

# general python imports
import time
import PyTango
import threading

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
from cscan_data_workers import DataSourceWorker, LambdaRoiWorker, LambdaWorker, TimerWorker
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

        self._has_lambda = False
        self._integration_time_correction = 1

        self._integration_time = None
        self._acq_duration = None
        self._position_start = None
        self._position_stop = None

        if self.macro.timme:
            self._timing_logger = {}

        self._scan_in_process = False

        num_counters = 0
        _worker_triggers = []
        _lambda_settled = False
        _counters = []

        for ind, channel_info in enumerate(self.measurement_group.getChannelsEnabledInfo()):
            _worker_triggers.append(Queue())
            if not 'lmbd' in channel_info.label:
                num_counters += 1
            else:
                if not _lambda_settled:
                    self._has_lambda = True
                    self._original_trigger_mode, self._original_operating_mode = None, None

        # DataWorkers array
        self._data_workers = []

        self.macro.debug('Starting barrier for {} workers'.format(num_counters))
        _workers_done_barrier = EndMeasurementBarrier(num_counters)
        self._error_queue = Queue()
        self._motor_mover = ExcThread(self.go_through_waypoints, '_motor_mover', self._error_queue)

        #We start main data collector loop
        _data_collector_trigger = Queue()
        self._data_collector = DataCollectorWorker(self.macro, self._data_workers,
                                                   _data_collector_trigger, self._error_queue, self._extra_columns,
                                                   self.moveables, self.data)

        ind = 0

        for channel_info in self.measurement_group.getChannelsEnabledInfo():
            if 'eh_t' in channel_info.label:
                self._timer_worker = TimerWorker(timer_names[channel_info.label], self._error_queue,
                                                 _worker_triggers, _data_collector_trigger, _workers_done_barrier,
                                                 self.macro, self.motion, self._timing_logger)
            if 'lmbd' in channel_info.label:
                if 'lmbd_countsroi' in channel_info.label:
                    self._data_workers.append(LambdaRoiWorker(ind, channel_info, _worker_triggers[ind],
                                                               _workers_done_barrier, self._error_queue,
                                                              self.macro, self._timing_logger))
                    ind += 1
                elif channel_info.label == 'lmbd':
                    self._data_workers.append(LambdaWorker(channel_info, _worker_triggers[ind],
                                                           _workers_done_barrier, self._error_queue,
                                                           self.macro))
                    ind += 1
                else:
                    raise RuntimeError('The {} detector is not supported in continuous scans'.format(channel_info.label))
            else:
                self._data_workers.append(DataSourceWorker(ind, channel_info, _worker_triggers[ind],
                                                           _workers_done_barrier, self._error_queue, self.macro,
                                                           self._timing_logger))
                ind += 1

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

        self.macro.debug('LAMBDA state after setup: {}'.format(_lambda_proxy.State()))

        _lambdaonlineanalysis_proxy = PyTango.DeviceProxy(self.macro.getEnv('LambdaOnlineAnalysis'))
        if _lambdaonlineanalysis_proxy.State() == PyTango.DevState.MOVING:
            _lambdaonlineanalysis_proxy.StopAnalysis()

        _lambdaonlineanalysis_proxy.StartAnalysis()

        if _lambdaonlineanalysis_proxy.State() != PyTango.DevState.MOVING:
                raise RuntimeError('Cannot start LambdaOnlineAnalysis')

        self.macro.debug('LambdaOnLineAnalysis state after setup: {}'.format(_lambda_proxy.State()))

     # ----------------------------------------------------------------------
    def prepare_waypoint(self, waypoint, iterate_only=False):
        ### This function basically repeats the original, The only difference is that "slow_down" factor changed
        ### to fixed travel time, defined by waypoint['integ_time']*waypoint['npts']
        ### some variable were renamed to make code more readable

        self.macro.debug("prepare_waypoint() entering...")

        travel_time = waypoint["integ_time"] * waypoint["npts"]

        original_duration, self._acq_duration, overhead_time = travel_time, travel_time, 0
        calculated_paths = []

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
                self.macro.debug(
                        'Ideal path duration: {}, requested duration: {}'.format(motor_path.duration, original_duration))
                self.macro.output(
                        'The required travel time cannot be reached due to {} motor cannot travel with such high speed'.format(
                            moveable.name))

                self._acq_duration = motor_path.duration

            calculated_paths.append(motor_path)

        # now that we have the appropriate top velocity for all motors, the
        # cruise duration of motion at top velocity, and the time it takes to
        # recalculate
        _reset_positions = False
        for path in calculated_paths:
            vmotor = path.motor
            # in the case of pseudo motors or not moving a motor...
            if path.displacement != 0:
                motor = path.physical_motor

                old_top_velocity = motor.getVelocity()
                new_top_vel = path.displacement / self._acq_duration
                vmotor.setMaxVelocity(new_top_vel)

                try:
                    accel_time = motor.getAcceleration() * new_top_vel / old_top_velocity # to keep acceleration in steps
                    vmotor.setAccelerationTime(accel_time)
                    overhead_time = max(overhead_time, accel_time)

                    decel_time = motor.getDeceleration() * new_top_vel / old_top_velocity # to keep acceleration in steps
                    vmotor.setDecelerationTime(decel_time)

                    if not iterate_only and not _can_be_synchro:
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
            for path, original_start, original_stop in zip(calculated_paths, waypoint['start_positions'],
                                                           waypoint['positions']):
                path.setInitialUserPos(original_start)
                path.setFinalUserPos(original_stop)

        for path in calculated_paths:
            self.macro.output('Calculated positions for motor {}: start: {}, stop: {}'.format(motor,
                                                                                              path.initial_user_pos,
                                                                                              path.final_user_pos))

            self.macro.output('The speed of {} motor will be changed from {} to {}'.format(motor,
                                                                                           old_top_velocity,
                                                                                           new_top_vel))

        self._integration_time_correction = self._acq_duration/original_duration

        self._position_start = waypoint['start_positions'][0]
        self._position_stop = waypoint['positions'][0]

        return calculated_paths

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
        self.macro.debug("_go_through_waypoints() entering...")

        for _, waypoint in self.steps:

            motion_paths = self.prepare_waypoint(waypoint)

            # get integ_time for this loop
            _, step_info = self.period_steps.next()
            self._data_collector.set_new_step_info(step_info)
            self._integration_time = step_info['integ_time'] * self._integration_time_correction
            if self._integration_time_correction > 1:
                self.macro.output('Integration time was corrected to {} due to slow motor(s)'.format(self._integration_time))
            self._timer_worker.set_new_period(self._integration_time)

            if self._has_lambda:
                self._setup_lambda(self._integration_time)

            # execute pre-move hooks
            for hook in waypoint.get('pre-move-hooks', []):
                hook()

            start_pos, final_pos = [], []
            for path in motion_paths:
                start_pos.append(path.initial_user_pos)
                final_pos.append(path.final_user_pos)

            self.macro.debug('Start pos: {}, final pos: {}'.format(start_pos, final_pos))
            if self.macro.isStopped():
                self.on_waypoints_end()
                return

            # move to start position
            self.macro.debug("Moving to start position: {}".format(start_pos))
            self.motion.move(start_pos)

            if self.macro.isStopped():
                self.on_waypoints_end()
                return

            # prepare motor(s) with the velocity required for synchronization
            for path in motion_paths:
                path.physical_motor.setVelocity(path.motor.getMaxVelocity())
                path.physical_motor.setAcceleration(path.motor.getAccelerationTime())
                path.physical_motor.setDeceleration(path.motor.getDecelerationTime())

            if self.macro.isStopped():
                self.on_waypoints_end()
                return

            self.motion_event.set()

            # move to waypoint end position
            self.macro.debug("Moving to final position: {}".format(final_pos))
            self.motion.move(final_pos)

            self.motion_event.clear()

            if self.macro.isStopped():
                return self.on_waypoints_end()

            # execute post-move hooks
            for hook in waypoint.get('post-move-hooks', []):
                hook()

        self.on_waypoints_end()

    # ----------------------------------------------------------------------
    def scan_loop(self):

        self.macro.debug("scan loop() entering...")

        self._scan_in_process = True

        macro = self.macro
        # manager = macro.getManager()
        scream = False
        motion_event = self.motion_event

        if hasattr(macro, 'getHooks'):
            for hook in macro.getHooks('pre-scan'):
                hook()

        if hasattr(macro, 'getHooks'):
            for hook in macro.getHooks('pre-acq'):
                hook()

        # start move & acquisition as close as possible
        # from this point on synchronization becomes critical
        self._motor_mover.start()
        # manager.add_job(self.go_through_waypoints)

        while not self._all_waypoints_finished:

            self.macro.debug("waiting for motion event")
            # wait for motor to reach start position
            motion_event.wait()

            # wait for motor to reach max velocity
            self.macro.debug("wait for motors to pass start point")

            while self._motion.readPosition(force=True)[0] < self._position_start:
                time.sleep(REFRESH_PERIOD)

            acq_start_time = time.time()
            self._data_collector.set_acq_start_time(acq_start_time)
            self._timer_worker.start()

            #after first point generate triggers every integ_time
            while motion_event.is_set() and self._timer_worker.last_position < self._position_stop:

                # allow scan to stop
                macro.checkPoint()

                try:
                    err = self._error_queue.get(block=False)
                except empty_queue:
                    pass
                else:
                    self.macro.debug('Thread {} got an exception {} at line {}'.format(err[0], err[1], err[2].tb_lineno))
                    self._timer_worker.stop()
                    break

                # # If there is no more time to acquire... stop!
                # elapsed_time = time.time() - acq_start_time
                # if elapsed_time > self._acq_duration + self._integration_time:
                #     self.macro.debug("Stopping all workers")
                #     # motion_event.clear()
                #     self._timer_worker.stop()
                #     break

            self._finish_scan()

            if scream:
                yield 100.0

    # ----------------------------------------------------------------------
    def _finish_scan(self):

        self._timer_worker.stop()

        self.macro.debug("waiting for motion end")
        self.motion_end_event.wait()

        self.macro.debug("waiting for data collector finishes")

        _timeout_start_time = time.time()
        while time.time() < _timeout_start_time + TIMEOUT and self._data_collector.status == 'collecting':
            time.sleep(self._integration_time)

        if self._data_collector.status == 'collecting':
            self.macro.debug('Killing DataCollector')
            self._data_collector.stop()

        for worker in self._data_workers:
            worker.stop()

        if self.macro.timeme:

                self._macro.output('Acquisition: median {:.4f} max {:.4f}'.format(np.median(self._time_timer),
                                                                                  np.max(self._time_timer)))
                self._macro.output('Data collecting: median {:.4f} max {:.4f}'.format(np.median(self._time_acq),
                                                                                      np.max(self._time_acq)))

        if hasattr(macro, 'getHooks'):
            for hook in macro.getHooks('post-acq'):
                hook()
            for hook in macro.getHooks('post-scan'):
                hook()

        env = self._env
        env['acqtime'] = self._data_collector.last_collected_point*self._integration_time
        env['delaytime'] = time.time() - env['acqtime']

        self.macro.debug("scan loop finished")

        self._scan_in_process = False

    # ----------------------------------------------------------------------
    def do_restore(self):
        if self._scan_in_process:
            self._finish_scan()

        self.macro.debug('Stopping motion')
        self.motion.stop()
        self.macro.debug('Resetting motors')
        self._restore_motors()

        if self._has_lambda:
            self.macro.debug('Stopping LambdaOnlineAnalysis')
            _lambdaonlineanalysis_proxy = PyTango.DeviceProxy(self.macro.getEnv('LambdaOnlineAnalysis'))
            _lambdaonlineanalysis_proxy.StopAnalysis()
            time.sleep(0.1)

            _time_out = time.time()
            while _lambdaonlineanalysis_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_LAMBDA:
                time.sleep(0.1)

            if _lambdaonlineanalysis_proxy.State() != PyTango.DevState.ON:
                self.macro.output('Cannot stop LambdaOnlineAnalysis!')

            self.macro.debug('Stopping Lambda')

            _lambda_proxy = PyTango.DeviceProxy(self.macro.getEnv('LambdaDevice'))
            _lambda_proxy.StopAcq()

            _time_out = time.time()
            while _lambda_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_LAMBDA:
                time.sleep(0.1)

            if _lambda_proxy.State() == PyTango.DevState.ON:
                _lambda_proxy.FrameNumbers = 1
            else:
                self.macro.output('Cannot reset Lambda! Check the settings.')

        try:
            if hasattr(self.macro, 'do_restore'):
                self.macro.do_restore()
        except Exception:
            msg = ("Failed to execute 'do_restore' method of the %s macro" %
                   self.macro.getName())
            self.macro.debug(msg)
            self.macro.debug('Details: ', exc_info=True)
            raise ScanException('error while restoring a backup')
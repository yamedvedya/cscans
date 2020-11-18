#!/usr/bin/env python
'''
File name soft_continuous_lambda.py

The purpose of this script is to take a certain number
of Lambda images synchronized with motor movement.
Synchronization is achieved by a software.

Author yury.matveev@desy.de

Note: this script uses ScanDir and ScanFile from the MacroServer
'''

import PyTango
import threading
import sys
from Queue import Queue
from Queue import Empty as empty_queue
import numpy as np
import time

from sardana.macroserver.scan import *
from sardana.macroserver.macro import *
from sardana.macroserver.scan.gscan import ScanException
from sardana.macroserver.macros.scan import getCallable, UNCONSTRAINED
from sardana.util.motion import Motor as VMotor
from sardana.util.motion import MotionPath


debug = False

# this parameters defines whether we try to synchronise movement of many motors (if there are)
TIMEOUT = 1
TIMEOUT_LAMBDA = 1
REFRESH_PERIOD = 5e-4

DUMMY_MOTOR = 'exp_dmy01'

YES_OPTIONS = ['Yes', 'yes', 'y']
NO_OPTIONS = ['No', 'no', 'n']

COUNTER_RESET_DELAY = 0

# Super ugly, should be better solution:
counter_names = {'sis3820': 'p23/counter/eh'}
timer_names = {'eh_t01': 'p23/dgg2/eh.01',
               'eh_t02': 'p23/dgg2/eh.02'}

__all__ = ['dcscan', 'acscan', 'd2cscan', 'a2cscan','cscan_senv']

# ----------------------------------------------------------------------
#     Child of CSScan with modified functionality
# ----------------------------------------------------------------------

class CCScan(CSScan):

    def __init__(self, macro, waypointGenerator=None, periodGenerator=None,
                 moveables=[], env={}, constraints=[], extrainfodesc=[]):
        super(CCScan, self).__init__(macro, waypointGenerator, periodGenerator,
                 moveables, env, constraints, extrainfodesc)

        # self.macro.output(dir(self.motion))
        # raise RuntimeError('Test run')
        # Parsing measurement group:

        self._has_lambda = False
        self._integration_time_correction = 1

        self._integration_time = None
        self._acq_duration = None
        self._wait_time = None

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

        if debug:
            self.macro.output('Starting barrier for {} workers'.format(num_counters))
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
                                                 self.macro, self.motion)
            if 'lmbd' in channel_info.label:
                if 'lmbd_countsroi' in channel_info.label:
                    self._data_workers.append(LambdaRoiWorker(ind, channel_info, _worker_triggers[ind],
                                                               _workers_done_barrier, self._error_queue, self.macro))
                    ind += 1
                elif channel_info.label == 'lmbd':
                    self._data_workers.append(LambdaWorker(channel_info, _worker_triggers[ind],
                                                               _workers_done_barrier, self._error_queue, self.macro))
                    ind += 1
                else:
                    raise RuntimeError('The {} detector is not supported in continuous scans'.format(channel_info.label))
            else:
                self._data_workers.append(DataSourceWorker(ind, channel_info, _worker_triggers[ind],
                                                           _workers_done_barrier, self._error_queue, self.macro))
                ind += 1

        if debug:
            self.macro.output("__init__ finished")

    # ----------------------------------------------------------------------
    def _setup_lambda(self, integ_time):
        _lambda_proxy = PyTango.DeviceProxy(self.macro.getEnv('LambdaDevice'))
        if _lambda_proxy.State() != PyTango.DevState.ON:
            _lambda_proxy.StopAcq()
            _time_out = time.time()
            while _lambda_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_LAMBDA:
                time.sleep(0.1)
                macro.checkPoint()

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

        if debug:
            self.macro.output('LAMBDA state after setup: {}'.format(_lambda_proxy.State()))

        _lambdaonlineanalysis_proxy = PyTango.DeviceProxy(self.macro.getEnv('LambdaOnlineAnalysis'))
        if _lambdaonlineanalysis_proxy.State() == PyTango.DevState.MOVING:
            _lambdaonlineanalysis_proxy.StopAnalysis()

        _lambdaonlineanalysis_proxy.StartAnalysis()

        if _lambdaonlineanalysis_proxy.State() != PyTango.DevState.MOVING:
                raise RuntimeError('Cannot start LambdaOnlineAnalysis')

        if debug:
            self.macro.output('LambdaOnLineAnalysis state after setup: {}'.format(_lambda_proxy.State()))

     # ----------------------------------------------------------------------
    def prepare_waypoint(self, waypoint, iterate_only=False):
        ### This function basically repeats the original, The only difference is that "slow_down" factor changed
        ### to fixed travel time, defined by waypoint['integ_time']*waypoint['npts']
        ### some variable were renamed to make code more readable

        self.debug("prepare_waypoint() entering...")

        travel_time = waypoint["integ_time"] * waypoint["npts"]

        original_duration, self._acq_duration, self._wait_time = travel_time, travel_time, 0
        calculated_paths = []

        _can_be_synchro = True
        for i, (moveable, start_position, final_position) in enumerate(zip(self.moveables, waypoint['start_positions'],
                                                                           waypoint['positions'])):
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
                if debug:
                    self.macro.output(
                        'Ideal path duration: {}, requested duration: {}'.format(motor_path.duration, original_duration))
                else:
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
                    self._wait_time = max(self._wait_time, accel_time)

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
                                                                         new_top_vel * (self._wait_time - accel_time))
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
                else:
                    self._wait_time = 0

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

            self._wait_time -= self._integration_time

            if self._has_lambda:
                self._setup_lambda(self._integration_time)

            # execute pre-move hooks
            for hook in waypoint.get('pre-move-hooks', []):
                hook()

            start_pos, final_pos = [], []
            for path in motion_paths:
                start_pos.append(path.initial_user_pos)
                final_pos.append(path.final_user_pos)

            if debug:
                self.macro.output('Start pos: {}, final pos: {}'.format(start_pos, final_pos))
            if self.macro.isStopped():
                self.on_waypoints_end()
                return

            # move to start position
            if debug:
                self.macro.output("Moving to start position: {}".format(start_pos))
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
            if debug:
                self.macro.output("Moving to final position: {}".format(final_pos))
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

        if debug:
            self.macro.output("scan loop() entering...")

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

            if debug:
                self.macro.output("waiting for motion event")
            # wait for motor to reach start position
            motion_event.wait()

            # wait for motor to reach max velocity
            if debug:
                self.macro.output("wait {} for motor to reach max velocity".format(self._wait_time))
            if self._wait_time > 0:
                time.sleep(self._wait_time)

            acq_start_time = time.time()
            self._data_collector.set_acq_start_time(acq_start_time)
            self._timer_worker.start()

            #after first point generate triggers every integ_time
            while motion_event.is_set():

                # allow scan to stop
                macro.checkPoint()

                try:
                    err = self._error_queue.get(block=False)
                except empty_queue:
                    pass
                else:
                    if debug:
                        self.macro.output('Thread {} got an exception {} at line {}'.format(err[0], err[1], err[2].tb_lineno))
                    self._timer_worker.stop()
                    break

                # If there is no more time to acquire... stop!
                elapsed_time = time.time() - acq_start_time
                if elapsed_time > self._acq_duration + 3 * self._integration_time:
                    if debug:
                        self.macro.output("Stopping all workers")
                    # motion_event.clear()
                    self._timer_worker.stop()
                    break

            if debug:
                self.macro.output("waiting for motion end")
            self.motion_end_event.wait()

            if debug:
                self.macro.output("waiting for data collector finishes")

            _timeout_start_time = time.time()
            while time.time() < _timeout_start_time + TIMEOUT and self._data_collector.status == 'collecting':
                time.sleep(self._integration_time)

            if self._data_collector.status == 'collecting':
                if debug:
                    self.macro.output('Killing DataCollector')
                self._data_collector.stop()

            for worker in self._data_workers:
                worker.stop()

            if self.macro._timeme:
                self._timer_worker.time_me()

        if hasattr(macro, 'getHooks'):
            for hook in macro.getHooks('post-acq'):
                hook()
            for hook in macro.getHooks('post-scan'):
                hook()

        env = self._env
        env['acqtime'] = self._data_collector.last_collected_point*self._integration_time
        env['delaytime'] = time.time() - env['acqtime']

        if debug:
            self.macro.output("scan loop finished")

        if not scream:
            yield 100.0

    # ----------------------------------------------------------------------
    def do_restore(self):
        if debug:
            self.macro.output('Stopping motion')
        self.motion.stop()
        if debug:
            self.macro.output('Resetting motors')
        self._restore_motors()

        if self._has_lambda:
            if debug:
                self.macro.output('Stopping LambdaOnlineAnalysis')
            _lambdaonlineanalysis_proxy = PyTango.DeviceProxy(self.macro.getEnv('LambdaOnlineAnalysis'))
            _lambdaonlineanalysis_proxy.StopAnalysis()
            time.sleep(0.1)

            _time_out = time.time()
            while _lambdaonlineanalysis_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_LAMBDA:
                time.sleep(0.1)

            if _lambdaonlineanalysis_proxy.State() != PyTango.DevState.ON:
                self.macro.output('Cannot stop LambdaOnlineAnalysis!')

            if debug:
                self.macro.output('Stopping LambdaOnlineAnalysis')

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


# ----------------------------------------------------------------------
#                       Channel data reader class
# ----------------------------------------------------------------------

class DataCollectorWorker(object):

    def __init__(self, macro, data_workers, point_trigger, error_queue,
                 extra_columns, moveables, data):

        self._data_collector_status = 'idle'
        self._last_started_point = -1
        self._macro = macro
        self._data_workers = data_workers
        self._point_trigger = point_trigger

        self._extra_columns = extra_columns
        self._moveables = moveables
        self._data = data
        self._step_info = None

        self._worker = ExcThread(self._data_collector_loop, 'data_collector', error_queue)
        self.status = 'waiting'
        self._worker.start()
        self.last_collected_point = -1

        if debug:
            self._macro.output('Data collector started')

    def _data_collector_loop(self):

        self._data_collector_status = 'running'
        while not self._worker.stopped():
            try:
                _last_started_point, _end_time, _motor_position = self._point_trigger.get(block=False)
            except empty_queue:
                self.status = 'waiting'
                time.sleep(0.1)
            else:
                _not_reported = ''
                self.status = 'collecting'
                if _last_started_point > self.last_collected_point:
                    if debug:
                        self._macro.output("start collecting data for point {}".format(_last_started_point))
                    data_line = {}
                    all_detector_reported = False
                    while not all_detector_reported and not self._worker.stopped():
                        all_detector_reported = True
                        for worker in self._data_workers:
                            if worker.data_buffer.has_key('{:04d}'.format(_last_started_point)):
                                data_line[worker.channel_name] = worker.data_buffer['{:04d}'.format(_last_started_point)]
                            else:
                                all_detector_reported *= False
                                _not_reported = worker.channel_name

                    if not self._worker.stopped():
                        if debug:
                            self._macro.output("data for point {} is collected".format(_last_started_point))

                        self.last_collected_point = _last_started_point

                        for ec in self._extra_columns:
                            data_line[ec.getName()] = ec.read()

                        # Add final moveable positions
                        data_line['point_nb'] = _last_started_point
                        data_line['timestamp'] = _end_time - self.acq_start_time
                        for i, m in enumerate(self._moveables):
                            data_line[m.moveable.getName()] = _motor_position[i]

                        # Add extra data coming in the step['extrainfo'] dictionary
                        if 'extrainfo' in self._step_info:
                            data_line.update(self._step_info['extrainfo'])

                        self._data.addRecord(data_line)
                    else:
                        self.status = 'aborted'
                        if debug:
                            self._macro.output('Not reported: {}'.format(_not_reported))
                            self._macro.output("datacollected was stopped")

        self.status = 'finished'
        if debug:
            self._macro.output("data collector finished")

    def set_new_step_info(self, step_info):
        self._step_info = step_info

    def set_acq_start_time(self, acq_start_time):
        self.acq_start_time = acq_start_time

    def stop(self):
        self._worker.stop()

# ----------------------------------------------------------------------
#                       Channel data reader class
# ----------------------------------------------------------------------


class DataSourceWorker(object):
    def __init__(self, index, source_info, trigger, workers_done_barrier, error_queue, macro):
        # arguments:
        # index = worker index
        # channel_info - channels information from measurement group
        # trigger_queue - threading queue to start measurement
        # workers_done_barrier - barrier to restart timer
        # error_queue - queue to report problems
        # macro - link to marco

        self._index = index
        self._source_info = source_info
        self._trigger = trigger
        self._macro = macro
        self._workers_done_barrier = workers_done_barrier

        self.data_buffer = {}
        tokens = source_info.source.split('/')
        self._device_proxy = PyTango.DeviceProxy('/'.join(tokens[2:-1]))
        self._device_attribute = tokens[-1]
        self.channel_name = source_info.full_name

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
                if debug:
                    self._macro.output('Worker {} was triggered, point {} with data {} in buffer'.format(
                        self.channel_name, index, self.data_buffer['{:04d}'.format(index)]))
                self._workers_done_barrier.report()
                _timeit.append(time.time()-_start_time)
            except empty_queue:
                time.sleep(REFRESH_PERIOD)
            except Exception as err:
                self._macro.output('Error {} {}'.format(err, sys.exc_info()[2].tb_lineno))

        if self._macro._timeme:
            self._macro.output('Mean time to get data for worker {} is {}'.format(self.channel_name,
                                                                                  np.mean(_timeit)))

    def stop(self):
        self._worker.stop()

# ----------------------------------------------------------------------
#                       Lambda ROI reader class
# ----------------------------------------------------------------------


class LambdaRoiWorker(object):
    def __init__(self, index, source_info, trigger, workers_done_barrier, error_queue, macro):
        # arguments:
        # index = worker index
        # channel_info - channels information from measurement group
        # trigger_queue - threading queue to start measurement
        # workers_done_barrier - barrier to restart timer
        # error_queue - queue to report problems
        # macro - link to marco

        self._index = index
        self._trigger = trigger
        self._macro = macro
        self._workers_done_barrier = workers_done_barrier

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
                        self.data_buffer['{:04d}'.format(index)] = data
                        if debug:
                            self._macro.output('Lambda RoI {} was triggered, point {} with data {} in buffer'.format(
                                                self._channel, index, self.data_buffer['{:04d}'.format(index)]))
                        self._workers_done_barrier.report()
                        _timeit.append(time.time()-_start_time)
                        break
                    else:
                        time.sleep(REFRESH_PERIOD)
            except empty_queue:
                time.sleep(REFRESH_PERIOD)

        if self._macro._timeme:
            self._macro.output('Mean time to get data for worker {} is {}'.format(self.channel_name,
                                                                                  np.mean(_timeit)))

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
#                       Timer class
# ----------------------------------------------------------------------


class TimerWorker(object):
    def __init__(self, timer_name, error_queue, triggers, data_collector_trigger,
                 workers_done_barrier, macro, motion):
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
        self._motion = motion
        self._data_collector_trigger = data_collector_trigger
        self._timeit = []

        self._worker = ExcThread(self._main_loop, 'timer_worker', error_queue)

    def _main_loop(self):
        while not self._worker.stopped():
            if debug:
                self._macro.output('Start timer point {}'.format(self._point))
            _start_time = time.time()
            self._device_proxy.StartAndWaitForTimer()
            self._timeit.append(time.time() - _start_time)
            for trigger in self._triggers:
                trigger.put(self._point)
            self._data_collector_trigger.put([self._point, time.time(), self._motion.readPosition(force=True)])
            self._workers_done_barrier.wait()
            self._point += 1

    def time_me(self):
            self._macro.output('Mean timer time: {}'.format(np.mean(self._timeit)))

    def stop(self):
        self._worker.stop()

    def start(self):
        self._worker.start()

    def set_new_period(self, period):
        self._device_proxy.SampleTime = period


# ----------------------------------------------------------------------
#                       Main scan class
# ----------------------------------------------------------------------

class scancl(Hookable):

    def _prepare(self, mode, motor, start_pos, final_pos, nb_steps, integ_time, **opts):

        self.do_scan = True

        # save the user parameters
        self.mode = mode
        self.motors = []
        self.start_pos = []
        self.final_pos = []
        self.original_positions = []

        for mot, start, final in zip(motor, start_pos, final_pos):
            motor, start_pos, final_pos, original_pos = self._parse_motors(mot, start, final)
            self.motors += motor
            self.start_pos += start_pos
            self.final_pos += final_pos
            self.original_positions += original_pos

        self.nsteps = nb_steps
        self.integ_time = integ_time

        try:
            self._sync = self.getEnv('cscan_sync')
        except Exception as err:
            self._sync = True

        try:
            self._timeme = self.getEnv('cscan_timeme')
        except Exception as err:
            self._timeme = False

        if debug:
            self.output('SYNC mode {}'.format(self._sync))

        if not self._sync and len(self.motors) > 1:
            options = YES_OPTIONS + NO_OPTIONS
            run_or_not = self.input("The sync mode is off, the positions of motors will not be syncronized. Continue?".format(self.integ_time),
                                    data_type=options, allow_multiple=False, title="Favorites", default_value='No')
            if run_or_not in NO_OPTIONS:
                self.do_scan = False
                return

        if self.integ_time < 0.1:
            options = YES_OPTIONS + NO_OPTIONS
            run_or_not = self.input("The {} integration time is too short for continuous scans. Recommended > 0.1 sec. Continue?".format(self.integ_time),
                                    data_type=options, allow_multiple=False, title="Favorites", default_value='No')
            if run_or_not in NO_OPTIONS:
                self.do_scan = False
                return

        # the "env" dictionary may be passed as an option
        env = opts.get('env', {})

        self.sources = []
        for element in self.getMeasurementGroup(self.getEnv('ActiveMntGrp')).physical_elements:
            self.sources.append(self.getExpChannel(element))

        self.num_sources = len(self.sources)

        moveables = []
        for m, start, final in zip(self.motors, self.start_pos, self.final_pos):
            moveables.append(MoveableDesc(moveable=m, min_value=min(
                start, final), max_value=max(start, final)))

        constrains = [getCallable(cns) for cns in opts.get(
            'constrains', [UNCONSTRAINED])]
        extrainfodesc = opts.get('extrainfodesc', [])

        # create an instance of CScan
        self._gScan = CCScan(self,
                             waypointGenerator=self._waypoint_generator,
                             periodGenerator=self._period_generator,
                             moveables=moveables, env=env, constraints=constrains, extrainfodesc=extrainfodesc)


    # ----------------------------------------------------------------------
    def _waypoint_generator(self):
        # returns start and further points points. Note that we pass the desired travel time
        yield {"start_positions": self.start_pos, "positions": self.final_pos,
               "integ_time": self.integ_time, "npts": self.nsteps}


    # ----------------------------------------------------------------------
    def _period_generator(self):
        step = {}
        step["integ_time"] = self.integ_time
        point_no = 0
        while (True):  # infinite generator. The acquisition loop is started/stopped at begin and end of each waypoint
            point_no += 1
            step["point_id"] = point_no
            yield step

    # ----------------------------------------------------------------------
    def do_restore(self):
        if self.mode == 'dscan':
            self.output("Returning to start positions {}".format(self.original_positions))
            self.getMotion([m.getName() for m in self.motors]).move(self.original_positions)

    # ----------------------------------------------------------------------
    def _parse_motors(self, motor, start_pos, end_pos):
        try:
            _motor_proxy = PyTango.DeviceProxy(PyTango.DeviceProxy(motor.getName()).TangoDevice)
            if _motor_proxy.info().dev_class in ['SlitExecutor', 'VmExecutor']:
                try:
                    if _motor_proxy.info().dev_class == 'SlitExecutor':
                        self.output("Slit {} found".format(motor.getName()))
                        tango_motors = []
                        for component in ['Left', 'Right', 'Top', 'Bottom']:
                            if _motor_proxy.get_property(component)[component]:
                                tango_motors.append(_motor_proxy.get_property(component)[component][0])
                    elif _motor_proxy.info().dev_class == 'VmExecutor':
                        self.output("VM {} found".format(motor.getName()))

                        sub_devices = _motor_proxy.get_property('__SubDevices')['__SubDevices']
                        tango_motors = [device.replace('hasep23oh:10000/', '') for device in sub_devices if 'vmexecutor' not in device]

                    channel_names = [device.split('/')[-1].replace('.', '/') for device in tango_motors]

                    if debug:
                        self.output('tango_motors {}'.format(tango_motors))
                        self.output('channel_names {}'.format(channel_names))

                    _, real_start, real_finish, real_original = self._parse_dscan_pos(motor, start_pos, end_pos)

                    _motor_proxy.PositionSim = real_start[0]
                    _sim_start_pos = _motor_proxy.ResultSim

                    _motor_proxy.PositionSim = real_finish[0]
                    _sim_end_pos = _motor_proxy.ResultSim

                    _motor_proxy.PositionSim = real_original[0]
                    _sim_original_pos = _motor_proxy.ResultSim

                    motors = []
                    new_start_pos = []
                    new_end_pos = []
                    new_original_pos = []

                    all_motors = self.getMotors()
                    for motor_name, channel_name in zip(tango_motors, channel_names):
                        for key, value in all_motors.items():
                            if channel_name in key:
                                motors.append(value)
                                for entry in _sim_start_pos:
                                    tockens = entry.split(':')
                                    if motor_name in tockens[0]:
                                        new_start_pos.append(float(tockens[1].strip()))

                                for entry in _sim_end_pos:
                                    tockens = entry.split(':')
                                    if motor_name in tockens[0]:
                                        new_end_pos.append(float(tockens[1].strip()))

                                for entry in _sim_original_pos:
                                    tockens = entry.split(':')
                                    if motor_name in tockens[0]:
                                        new_original_pos.append(float(tockens[1].strip()))

                    self.output('Calculated positions for {} :'.format(motor.getName()))
                    for motor, start_pos, end_pos in zip(motors, new_start_pos, new_end_pos):
                        self.output('sub_motor {} start_pos {} final_pos {}'.format(motor, start_pos, end_pos))

                    return motors, new_start_pos, new_end_pos, new_original_pos
                except:
                    raise RuntimeError('Cannot parse {} to components, the cscan cannot be executed'.format(motor.getName()))
            else:
                return self._parse_dscan_pos(motor, start_pos, end_pos)
        except AttributeError:
            return self._parse_dscan_pos(motor, start_pos, end_pos)

    # ----------------------------------------------------------------------
    def _parse_dscan_pos(self, motor, start, stop):
        original_position = self.getMotion([motor.getName()]).readPosition(force=True)
        if self.mode == 'dscan':
            return [motor], [start + original_position[0]], [stop + original_position[0]], original_position
        else:
            return [motor], [start], [stop], original_position

    # ----------------------------------------------------------------------
    def _get_command(self, command):
        if len(self.motors) > 1:
            command += '{}scan'.format(len(self.motors))
        else:
            command += 'scan'

        for motor, start_pos, final_pos in zip(self.motors, self.start_pos, self.final_pos):
            command += ' ' + ' '.join([motor.getName(), '{:.4f} {:.4f}'.format(float(start_pos), float(final_pos))])
        command += ' {} {}'.format(self.nsteps, self.integ_time)
        return command

# ----------------------------------------------------------------------
#                       These classes are called by user
# ----------------------------------------------------------------------

class dcscan(Macro, scancl):

    """ Performs a continuous scan taking current Active Measurement Group
        In case if the Lambda configured - set it to

    """

    # this is used to indicate other codes that the macro is a scan
    hints = {'scan': 'dscan', 'allowsHooks': ('pre-scan', 'pre-move',
                                               'post-move', 'pre-acq',
                                               'post-acq',
                                               'post-scan')}

    env = ['ActiveMntGrp']

    param_def = [
        ['motor',           Type.Moveable,  None,   'Motor to move'],
        ['start_pos',       Type.Float,     None,   'Scan start position'],
        ['final_pos',       Type.Float,     None,   'Scan final position'],
        ['nb_steps',        Type.Integer,   None,   'Nb of steps'],
        ['integ_time',      Type.Float,     None,   'Integration time'],
    ]

    def prepare(self, motor, start_pos, final_pos, nb_steps, integ_time, **opts):

        self.name = 'dscan'

        self._prepare('dscan', [motor], np.array([start_pos], dtype='d'), np.array([final_pos], dtype='d'),
                      nb_steps, integ_time, **opts)

    def run(self, *args):
        if self.do_scan:
            for step in self._gScan.step_scan():
                yield step

    def getCommand(self):
        return self._get_command('a')

# ----------------------------------------------------------------------
class d2cscan(Macro, scancl):
    # this is used to indicate other codes that the macro is a scan
    hints = {'scan': 'd2cscan', 'allowsHooks': ('pre-scan', 'pre-move',
                                                'post-move', 'pre-acq',
                                                'post-acq',
                                                'post-scan')}

    env = ['ActiveMntGrp']

    param_def = [
        ['motor1',          Type.Moveable,  None, 'Motor 1 to move'],
        ['start_pos1',      Type.Float,     None, 'Scan start position 1'],
        ['final_pos1',      Type.Float,     None, 'Scan final position 1'],
        ['motor2',          Type.Moveable,  None, 'Motor 2 to move'],
        ['start_pos2',      Type.Float,     None, 'Scan start position 2'],
        ['final_pos2',      Type.Float,     None, 'Scan final position 2'],
        ['nr_interv',       Type.Integer,   None, 'Nb of scan intervals'],
        ['integ_time',      Type.Float,     None, 'Integration time']
    ]

    def prepare(self, motor1, start_pos1, final_pos1, motor2, start_pos2,
                final_pos2, nb_steps, integ_time, **opts):

        self.name = 'd2scan'

        self._prepare('dscan', [motor1, motor2], np.array([start_pos1, start_pos2], dtype='d'),
                      np.array([final_pos1, final_pos2], dtype='d'),
                      nb_steps, integ_time, **opts)

    def run(self, *args):
        if self.do_scan:
            for step in self._gScan.step_scan():
                yield step

    def getCommand(self):
        return self._get_command('a')

# ----------------------------------------------------------------------
class acscan(Macro, scancl):

    """ Performs a continuous scan taking current Active Measurement Group
        In case if the Lambda configured - set it to

    """

    # this is used to indicate other codes that the macro is a scan
    hints = {'scan': 'ascan', 'allowsHooks': ('pre-scan', 'pre-move',
                                               'post-move', 'pre-acq',
                                               'post-acq',
                                               'post-scan')}

    env = ['ActiveMntGrp']

    param_def = [
        ['motor',           Type.Moveable,  None,   'Motor to move'],
        ['start_pos',       Type.Float,     None,   'Scan start position'],
        ['final_pos',       Type.Float,     None,   'Scan final position'],
        ['nb_steps',        Type.Integer,   None,   'Nb of steps'],
        ['integ_time',      Type.Float,     None,   'Integration time'],
    ]


    def prepare(self, motor, start_pos, final_pos, nb_steps, integ_time, **opts):

        self.name = 'ascan'

        self._prepare('ascan', [motor], np.array([start_pos], dtype='d'), np.array([final_pos], dtype='d'),
                      nb_steps, integ_time, **opts)

    def run(self, *args):
        if self.do_scan:
            for step in self._gScan.step_scan():
                yield step

    def getCommand(self):
        return self._get_command('a')

# ----------------------------------------------------------------------
class a2cscan(Macro, scancl):
    # this is used to indicate other codes that the macro is a scan
    hints = {'scan': 'a2scan', 'allowsHooks': ('pre-scan', 'pre-move',
                                                'post-move', 'pre-acq',
                                                'post-acq',
                                                'post-scan')}

    env = ['ActiveMntGrp']

    param_def = [
        ['motor1',          Type.Moveable,  None, 'Motor 1 to move'],
        ['start_pos1',      Type.Float,     None, 'Scan start position 1'],
        ['final_pos1',      Type.Float,     None, 'Scan final position 1'],
        ['motor2',          Type.Moveable,  None, 'Motor 2 to move'],
        ['start_pos2',      Type.Float,     None, 'Scan start position 2'],
        ['final_pos2',      Type.Float,     None, 'Scan final position 2'],
        ['nr_interv',       Type.Integer,   None, 'Nb of scan intervals'],
        ['integ_time',      Type.Float,     None, 'Integration time']
    ]

    def prepare(self, motor1, start_pos1, final_pos1, motor2, start_pos2,
                final_pos2, nb_steps, integ_time, **opts):

        self.name = 'a2scan'

        self._prepare('ascan', [motor1, motor2], np.array([start_pos1, start_pos2], dtype='d'),
                      np.array([final_pos1, final_pos2], dtype='d'),
                      nb_steps, integ_time, **opts)

    def run(self, *args):
        if self.do_scan:
            for step in self._gScan.step_scan():
                yield step

    def getCommand(self):
        return self._get_command('a')

# ----------------------------------------------------------------------
class ctscan(Macro, scancl):
    # this is used to indicate other codes that the macro is a scan
    hints = {'scan': 'ctscan', 'allowsHooks': ('pre-scan', 'pre-move',
                                                'post-move', 'pre-acq',
                                                'post-acq',
                                                'post-scan')}

    env = ['ActiveMntGrp']

    param_def = [
        ['total_time',      Type.Float,     None, 'Total scan time'],
        ['integ_time',      Type.Float,     None, 'Integration time']
    ]

    def prepare(self, total_time, integ_time, **opts):

        self.name = 'ctscan'

        motor = self.getMotor(DUMMY_MOTOR)
        motor.Acceleration = 0
        motor.Deceleration = 0

        self._steps = int(total_time/integ_time)

        self._prepare('ascan', [motor], np.array([0], dtype='d'),
                      np.array([total_time], dtype='d'), int(total_time/integ_time) + 1, integ_time, **opts)

    def run(self, *args):
        if self.do_scan:
            for step in self._gScan.step_scan():
                yield step

    def getCommand(self):
        return 'ascan {} 0 {} {} {}'.format(DUMMY_MOTOR, self.final_pos[0], self.nsteps, self.integ_time)

# ----------------------------------------------------------------------
#                       Auxiliary class to set environment
# ----------------------------------------------------------------------

class cscan_senv(Macro):
    """ Sets default environment variables """

    def run(self):
        self.setEnv("LambdaDevice", "hasep23oh:10000/p23/lambda/01")
        self.setEnv("LambdaOnlineAnalysis", "hasep23oh:10000/p23/lambdaonlineanalysis/oh.01")
        self.setEnv("AttenuatorProxy", "p23/vmexecutor/attenuator")
        self.setEnv("cscan_sync", True)
        self.setEnv("cscan_timeme", False)

# ----------------------------------------------------------------------
#                       Auxiliary classes
# ----------------------------------------------------------------------

class EndMeasurementBarrier(object):

    def __init__(self, n_workers):
        super(EndMeasurementBarrier, self).__init__()
        self._n_reported_workers = 0
        self._n_workers = n_workers

    def wait(self):
        while self._n_reported_workers < self._n_workers:
            time.sleep(REFRESH_PERIOD)
        self._n_reported_workers = 0

    def report(self):
        self._n_reported_workers += 1


# ----------------------------------------------------------------------
class ExcThread(threading.Thread):

    # ----------------------------------------------------------------------
    def __init__(self, target, threadname, errorBucket,  *args):
        threading.Thread.__init__(self, target=target, name=threadname, args=args)
        self._stop_event = threading.Event()
        self.bucket = errorBucket
        self._name = threadname

    # ----------------------------------------------------------------------
    def run(self):
        try:
            if hasattr(self, '_Thread__target'):
                self.ret = self._Thread__target(*self._Thread__args, **self._Thread__kwargs)
            else:
                self.ret = self._target(*self._args, **self._kwargs)
        except Exception as exp:
            # traceback.print_tb(sys.exc_info()[2])
            self.bucket.put([self._name, sys.exc_info()[0], sys.exc_info()[2]])

    # ----------------------------------------------------------------------
    def stop(self):
        self._stop_event.set()

    # ----------------------------------------------------------------------
    def stopped(self):
        return self._stop_event.isSet()
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
import traceback
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


verbose = False
debug = False
time_me = False

# this parameters defines whether we try to synchronise movement of many motors (if there are)
TIMEOUT = 15
TIMEOUT_LAMBDA = 1
REFRESH_PERIOD = 5e-4

__all__ = ['dcscan', 'acscan', 'd2cscan', 'a2cscan','cscan_senv']

# ----------------------------------------------------------------------
#     Child of CSScan with modified functionality
# ----------------------------------------------------------------------

class CCScan(CSScan):

    def __init__(self, macro, waypointGenerator=None, periodGenerator=None,
                 moveables=[], env={}, constraints=[], extrainfodesc=[]):
        super(CCScan, self).__init__(macro, waypointGenerator, periodGenerator,
                 moveables, env, constraints, extrainfodesc)

        # Super ugly, should be better solution:
        timer_names = {'eh_t01': 'p23/dgg2/eh.01',
                       'eh_t02': 'p23/dgg2/eh.02'}

        counter_names = {'sis3820': 'p23/counter/eh'}

        # Parsing measurement group:
        self._has_lambda = False
        self._integration_time_correction = 1

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

            for counter_name, tango_name in counter_names.items():
                if counter_name in channel_info.full_name:
                    _counters.append([tango_name, int(channel_info.full_name.split('/')[-1])])

        # DataWorkers array
        self._data_workers = []

        if debug:
            self.macro.output('Starting barrier for {} workers'.format(num_counters))
        _workers_done_barrier = EndMeasurementBarrier(num_counters)
        self._error_queue = Queue()

        #We start main data collector loop
        _data_collector_trigger = Queue()
        self._data_collector = DataCollectorWorker(self.macro, self._data_workers,
                                                   _data_collector_trigger, self._error_queue, self._extra_columns,
                                                   self.moveables, self.data)

        ind = 0

        for channel_info in self.measurement_group.getChannelsEnabledInfo():
            if 'eh_t' in channel_info.label:
                self._timer_worker = TimerWorker(timer_names[channel_info.label], self._error_queue, _counters,
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
    def prepare_waypoint(self, waypoint, start_positions, iterate_only=False):
        ### This function basically repeats the original, The only difference is that "slow_down" factor changed
        ### to fixed travel time, defined by waypoint['integ_time']*waypoint['npts']
        ### some variable were renamed to make code more readable

        self.debug("prepare_waypoint() entering...")

        travel_time = waypoint["integ_time"] * waypoint["npts"]
        destination_positions = waypoint['positions']

        original_duration, cruise_duration, delta_start = travel_time, travel_time, 0
        calculated_paths = []
        for i, (moveable, position) in enumerate(zip(self.moveables, destination_positions)):
            motor = moveable.moveable

            try:
                base_vel, top_vel = motor.getBaseRate(), motor.getVelocity()
                accel_time = motor.getAcceleration()

                # Here we check whether this motor can move with required to synchronise speed
                # First we set speed to maximum
                max_top_vel = self.get_max_top_velocity(motor)
                if not iterate_only:
                    motor.setVelocity(max_top_vel)

            except AttributeError:
                if not iterate_only:
                    self.macro.warning("%s motion will not be coordinated", motor)
                base_vel, top_vel, max_top_vel = 0, float('+inf'), float('+inf')
                accel_time, decel_time = 0, 0
                synchronise = False

            last_user_pos = start_positions[i]

            # Find the cruise duration of motion at top velocity. For this
            # create a virtual motor which has instantaneous acceleration and
            # deceleration

            # create a path which will tell us which is the cruise
            # duration of this motion at top velocity
            motor_path = MotionPath(VMotor(min_vel=base_vel, max_vel=max_top_vel,
                                  accel_time=0, decel_time=0), last_user_pos, position)
            motor_path.moveable = moveable
            motor_path.apply_correction = True

            # if really motor is moving in this waypoint
            if motor_path.displacement > 0:
                # recalculate time to reach maximum velocity
                delta_start = max(delta_start, accel_time)

            # recalculate cruise duration of motion at top velocity
            if motor_path.duration > cruise_duration:
                if debug:
                    self.macro.output(
                        'Ideal path duration: {}, requested duration: {}'.format(motor_path.duration, original_duration))
                else:
                    self.macro.output(
                        'The required travel time cannot be reached due to {} motor cannot travel with such high speed'.format(
                            moveable.name))

                cruise_duration = motor_path.duration

            calculated_paths.append(motor_path)

        # now that we have the appropriate top velocity for all motors, the
        # cruise duration of motion at top velocity, and the time it takes to
        # recalculate
        for path in calculated_paths:
            vmotor = path.motor
            # in the case of pseudo motors or not moving a motor...
            if path.displacement != 0:
                moveable = path.moveable
                motor = moveable.moveable

                old_top_velocity = motor.getVelocity()
                new_top_vel = path.displacement / cruise_duration
                vmotor.setMaxVelocity(new_top_vel)

                accel_t, decel_t = motor.getAcceleration(), motor.getDeceleration()
                vmotor.setAccelerationTime(accel_t)
                vmotor.setDecelerationTime(decel_t)

                self.macro.output('Sync: {}'.format(self.macro.sync))
                if self.macro.sync:
                    disp_sign = path.positive_displacement and 1 or -1
                    base_vel = vmotor.getMinVelocity()
                    new_initial_pos = path.initial_user_pos - accel_t * 0.5 * \
                        disp_sign * (new_top_vel + base_vel) - disp_sign * \
                        new_top_vel * (delta_start - accel_t)
                    path.setInitialUserPos(new_initial_pos)

                    new_final_pos = path.final_user_pos + \
                        disp_sign * vmotor.displacement_reach_min_vel
                    path.setFinalUserPos(new_final_pos)
                else:
                    delta_start = 0

                self.macro.output('Calculated positions for motor {}: start: {}, stop: {}'.format(motor,
                                                                                                 path.initial_user_pos,
                                                                                                 path.final_user_pos))

                self.macro.output('The speed of {} motor will be changed from {} to {}'.format(motor,
                                                                                                 old_top_velocity,
                                                                                                 new_top_vel))

        self._integration_time_correction = cruise_duration/original_duration

        return calculated_paths, delta_start, cruise_duration

    # ----------------------------------------------------------------------
    def scan_loop(self):

        if debug:
            self.macro.output("scan loop() entering...")

        macro = self.macro
        manager = macro.getManager()
        scream = False
        motion_event = self.motion_event
        integ_time = 0

        if hasattr(macro, 'getHooks'):
            for hook in macro.getHooks('pre-scan'):
                hook()

        if hasattr(macro, 'getHooks'):
            for hook in macro.getHooks('pre-acq'):
                hook()

        # get integ_time for this loop
        try:
            _, step_info = self.period_steps.next()
            self._data_collector.set_new_step_info(step_info)
            integ_time = step_info['integ_time'] * self._integration_time_correction
            if self._integration_time_correction > 1:
                self.macro.output('Integration time was corrected to {} due to slow motor(s)'.format(integ_time))
            self._timer_worker.set_new_period(integ_time)
        except StopIteration:
            self._all_waypoints_finished = True

        if self._has_lambda:
            self._setup_lambda(integ_time)

        # start move & acquisition as close as possible
        # from this point on synchronization becomes critical
        manager.add_job(self.go_through_waypoints)

        while not self._all_waypoints_finished:

            if debug:
                self.macro.output("waiting for motion event")
            # wait for motor to reach start position
            motion_event.wait()

            # allow scan to stop
            macro.checkPoint()

            if debug:
                self.macro.output("wait for motor to reach max velocity")
            # wait for motor to reach max velocity
            deltat = self.timestamp_to_start - time.time() - integ_time
            if debug:
                self.macro.output('Delta T: {}'.format(deltat))
            if deltat > 0:
                time.sleep(deltat)

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
                    if verbose:
                        self.macro.output('Got an exception {}'.format(err))
                    self._timer_worker.stop()
                    self._data_collector.stop()
                    for worker in self._data_workers:
                        worker.stop()
                    raise RuntimeError(err)

                # If there is no more time to acquire... stop!
                elapsed_time = time.time() - acq_start_time
                if elapsed_time > self.acq_duration + 2 * integ_time:
                    if debug:
                        self.macro.output("Stopping all workers")
                    motion_event.clear()
                    self._timer_worker.stop()
                    break

            if debug:
                self.macro.output("waiting for motion end")
            self.motion_end_event.wait()

            if debug:
                self.macro.output("waiting for data collector finishes")
            _timeout_start_time = time.time()
            while time.time() < _timeout_start_time + TIMEOUT and self._data_collector.status == 'collecting':
                time.sleep(integ_time)
            else:
                self._data_collector.stop()

            for worker in self._data_workers:
                worker.stop()

            if time_me:
                self._timer_worker.time_me()

        if hasattr(macro, 'getHooks'):
            for hook in macro.getHooks('post-acq'):
                hook()
            for hook in macro.getHooks('post-scan'):
                hook()

        env = self._env
        env['acqtime'] = self._data_collector.last_collected_point*integ_time
        env['delaytime'] = time.time() - env['acqtime']

        if debug:
            self.macro.output("scan loop finished")

        if not scream:
            yield 100.0

    # ----------------------------------------------------------------------
    def do_restore(self):
        self._restore_motors()

        if self._has_lambda:
            _lambdaonlineanalysis_proxy = PyTango.DeviceProxy(self.macro.getEnv('LambdaOnlineAnalysis'))
            _lambdaonlineanalysis_proxy.StopAnalysis()
            time.sleep(0.1)

            _time_out = time.time()
            while _lambdaonlineanalysis_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_LAMBDA:
                time.sleep(0.1)

            if _lambdaonlineanalysis_proxy.State() != PyTango.DevState.ON:
                self.macro.output('Cannot stop LambdaOnlineAnalysis!')

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
                self.status = 'collecting'
                if _last_started_point > self.last_collected_point:
                    if debug:
                        self._macro.output("start collecting data for point {}".format(_last_started_point))
                    data_line = {}
                    all_detector_reported = False
                    while not all_detector_reported and not self._worker.stopped():
                        all_detector_reported = True
                        for worker in self._data_workers:
                            data = worker.data_buffer.get(''.format(_last_started_point))
                            if data is None:
                                all_detector_reported *= False
                            else:
                                data_line[worker.channel_name] = data

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
                            self._macro.output("datacollected was stopped")

            except empty_queue:
                self.status = 'waiting'
                time.sleep(0.1)

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

        self._worker = ExcThread(self._main_loop, source_info.name, error_queue)
        self._worker.start()

    def _main_loop(self):
        _timeit = []
        while not self._worker.stopped():
            try:
                index = self._trigger.get(block=False)
                _start_time = time.time()
                self.data_buffer[''.format(index)] = getattr(self._device_proxy, self._device_attribute)
                if debug:
                    self._macro.output('Worker {} was triggered, point {} with data {} in buffer'.format(
                        self.channel_name, index, self.data_buffer[''.format(index)]))
                self._workers_done_barrier.report()
                _timeit.append(time.time()-_start_time)
            except empty_queue:
                time.sleep(REFRESH_PERIOD)

        if time_me:
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
                while not self._worker.stopped():
                    if self._device_proxy.lastanalyzedframe >= index + 1:
                        data = self._device_proxy.getroiforframe([self._channel, index + 1])
                        if self._correction_needed:
                            data *= self._attenuator_proxy.Position
                        self.data_buffer[''.format(index)] = data
                        if debug:
                            self._macro.output('Worker {} was triggered, point {} with data {} in buffer'.format(
                                self.channel_name, index, self.data_buffer[''.format(index)]))
                        self._workers_done_barrier.report()
                        _timeit.append(time.time()-_start_time)
                        break
                    else:
                        time.sleep(REFRESH_PERIOD)
            except empty_queue:
                time.sleep(REFRESH_PERIOD)

        if time_me:
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
                self.data_buffer[''.format(index)] = -1
            except empty_queue:
                time.sleep(REFRESH_PERIOD)

    def stop(self):
        self._worker.stop()

# ----------------------------------------------------------------------
#                       Timer class
# ----------------------------------------------------------------------


class TimerWorker(object):
    def __init__(self, timer_name, error_queue, counters, triggers, data_collector_trigger,
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

        self._counter_proxies = []
        for tango_name, channel in counters:
            self._counter_proxies.append(PyTango.DeviceProxy('{}.{:02d}'.format(tango_name, channel)))

        self._worker = ExcThread(self._main_loop, 'timer_worker', error_queue)

    def _main_loop(self):

        while not self._worker.stopped():
            if debug:
                self._macro.output('Start timer point {}'.format(self._point))
            _start_time = time.time()
            for proxy in self._counter_proxies:
                proxy.Reset()

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

        for mot, start, final in zip(motor, start_pos, final_pos):
            new_mot, new_start, new_final = self._parse_motors(mot, start, final)
            self.motors += new_mot
            self.start_pos += new_start
            self.final_pos += new_final

        self.nsteps = nb_steps
        self.integ_time = integ_time

        try:
            self.sync = self.getEnv('cscan_sync')
        except Exception as err:
            self.sync = True

        if debug:
            self.output('SYNC mode {}'.format(self.sync))

        if not self.sync and len(self.motors) > 1:
            options = "Yes", "No"
            run_or_not = self.input("The sync mode is off, the positions of motors will not be syncronized. Continue?".format(self.integ_time),
                                    data_type=options, allow_multiple=False, title="Favorites", default_value='No')
            if run_or_not == 'No':
                self.do_scan = False
                return

        if self.integ_time < 0.1:
            options = "Yes", "No"
            run_or_not = self.input("The {} integration time is too short for continuous scans. Recommended > 0.1 sec. Continue?".format(self.integ_time),
                                    data_type=options, allow_multiple=False, title="Favorites", default_value='No')
            if run_or_not == 'No':
                self.do_scan = False
                return

        if self.mode == 'dscan':
            self._motion = self.getMotion([m.getName() for m in self.motors])
            self.originalPositions = np.array(self._motion.readPosition(force=True))
            if debug:
                self.output('Original positions: {}'.format(self.originalPositions))
            self.start_pos += self.originalPositions
            self.final_pos += self.originalPositions

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
        yield {"positions": self.start_pos, "waypoint_id": 0}
        yield {"positions": self.final_pos, "waypoint_id": 1, "integ_time": self.integ_time, "npts": self.nsteps}


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
            self.output("Returning to start positions {}".format(self.originalPositions))
            self._motion.move(self.originalPositions)

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

                    _motor_proxy.PositionSim = start_pos
                    _sim_start_pos = _motor_proxy.ResultSim

                    _motor_proxy.PositionSim = end_pos
                    _sim_end_pos = _motor_proxy.ResultSim

                    motors = []
                    new_start_pos = []
                    new_end_pos = []

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

                    self.output('Calculated positions for {} :'.format(motor.getName()))
                    for motor, start_pos, end_pos in zip(motors, new_start_pos, new_end_pos):
                        self.output('sub_motor {} start_pos {} final_pos {}'.format(motor, start_pos, end_pos))

                        return motors, new_start_pos, new_end_pos
                except:
                    raise RuntimeError('Cannot parse {} to components, the cscan cannot be executed'.format(motor.getName()))
            else:
                return [motor], [start_pos], [end_pos]
        except AttributeError:
            return [motor], [start_pos], [end_pos]

    # ----------------------------------------------------------------------
    def _get_command(self, command):
        if len(self.motors) > 1:
            command += '{}scan'.format(len(self.motors))
        else:
            command += 'scan'

        for motor, start_pos, final_pos in zip(self.motors, self.start_pos, self.final_pos):
            command += ' ' + ' '.join([motor.getName(), str(start_pos), str(final_pos)])
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
        return self._get_command('d')

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
        return self._get_command('d')

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
#                       Auxiliary class to set environment
# ----------------------------------------------------------------------

class cscan_senv(Macro):
    """ Sets default environment variables """

    def run(self):
        self.setEnv("LambdaDevice", "hasep23oh:10000/p23/lambda/01")
        self.setEnv("LambdaOnlineAnalysis", "hasep23oh:10000/p23/lambdaonlineanalysis/oh.01")
        self.setEnv("AttenuatorProxy", "p23/vmexecutor/attenuator")
        self.setEnv("cscan_sync", True)

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

    # ----------------------------------------------------------------------
    def run(self):
        try:
            if hasattr(self, '_Thread__target'):
                self.ret = self._Thread__target(*self._Thread__args, **self._Thread__kwargs)
            else:
                self.ret = self._target(*self._args, **self._kwargs)
        except Exception as exp:
            traceback.print_tb(sys.exc_info()[2])
            self.bucket.put(sys.exc_info())

    # ----------------------------------------------------------------------
    def stop(self):
        self._stop_event.set()

    # ----------------------------------------------------------------------
    def stopped(self):
        return self._stop_event.isSet()
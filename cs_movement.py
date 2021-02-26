# ----------------------------------------------------------------------
# Author:        yury.matveev@desy.de
# ----------------------------------------------------------------------

"""
This file contains classes, which provide all motor-related functionality for Cscans:
moving motors, waiting motion to be finished, get the motors position and logging it (if needed)

The base class is Movement, it is inherit by SerialMovement, ParallelMovement

In SerialMovement we address all motor in a sequence - slow and reliable, nothing special

In case of ParallelMovement we try to do inout in parallel.
To do it we start a MotorWorker for each motor and send our requests them
However, the OMS server can die if we use >2 motors...

The Status monitor provides functionality to wait till motion ends.
It has 2 modes: just look to Status attribute, or to establish Tango event and wait for it.
"""

# general python imports
import PyTango
import time
import sys
import numpy as np
from threading import Lock

if sys.version_info.major >= 3:
    from queue import Queue
    from queue import Empty as empty_queue
else:
    from Queue import Queue
    from Queue import Empty as empty_queue

# cscan imports
from cs_axillary_functions import ExcThread, EndMeasurementBarrier, send_movevvc_command
from cs_constants import *


class Movement(object):

    def __init__(self, macro, motors_list, movable_names, error_queue):

        self._error_queue = error_queue
        self._macro = macro

        # index of motor to be used to start and stop acquisition
        self._main_motor = None

        self._last_positions = None

        # in case of HKL scan we need to know which "reciprocal" motor we scan do report it to datacollector
        self._movable_names = movable_names

        # since the requests can come asyncro from different sources sometimes we do not want to overload the net
        # and just pass them last measured position, insted or reading the current one from Tango
        self._last_refresh_period = 0

        self._waiting_for_move_done = False

        self._devices_names = [motor.getName() for motor in motors_list]

        if self._macro.space == 'reciprocal':
            _device_keys = [self._macro.angle_device_names[angle] for angle in self._macro.angle_names]
            # we need to know which column from diffract response correspond to which motor
            self._column_map = [self._devices_names.index(element) for element in _device_keys]

        # this thread logging movement, which is especially needed to track the reciprocal trajectory
        self._movement_monitor = ExcThread(self._monitor, 'movement_monitor', error_queue)
        self._run_monitor = False
        self._movement_monitor.start()

        self._stop = False

        self._devices = None

    # ----------------------------------------------------------------------
    def _wait_move(self, monitor):
        # here we wait till the motion if finished and start, if needed, the motion logger

        self._waiting_for_move_done = True

        if monitor:
            self._run_monitor = True

        while self._is_in_move() and not self._stop:
            time.sleep(REFRESH_PERIOD)

        if monitor:
            self._run_monitor = False

        self._waiting_for_move_done = False

    # ----------------------------------------------------------------------
    def _monitor(self):
        # this thread prints the position of all motors to file

        self._movement_monitor_state = 'running'
        
        start_time = time.time()
        _output_file = None
        
        try:
            while not self._movement_monitor.stopped():

                if self._run_monitor:
                    if _output_file is None:
                        # we make a new file and print the header
                        _output_file = open(self._macro._motion_monitor_name, 'a')
                    
                        _output_file.write('#Time;')
                        if self._macro.space == 'reciprocal':
                            _output_file.write('H;K;L;')
        
                        _output_file.write(';'.join(['{}({})'.format(sardana_name, device.name()) for
                                      sardana_name, device in zip(self._devices_names, self._devices)]) + '\n')
        
                    # first column - time
                    line = '{:.5f};'.format(time.time() - start_time)
                    # in case of reciprocal scan - we print the hkl positions
                    if self._macro.space == 'reciprocal':
                        line += str(';'.join(['{:.5f}'.format(value) for value in self._calculate_hkl()]) + ';')

                    # all physical motors
                    line += str(';'.join(['{:.5f}'.format(value) for value in self.physical_motors_positions()]) + '\n')

                    _output_file.write(line)
     
                time.sleep(MOTOR_POSITION_REFRESH_PERIOD)

        except Exception as err:

            self._macro.error('Motion logging error!:' + err)
            # self._error_queue.put(err)

        if _output_file is not None:
            _output_file.close()
            
        self._movement_monitor_state = 'stopped'
        self._macro.report_debug('Stopping movement monitor, state: {}'.format(self._movement_monitor_state))

    # ----------------------------------------------------------------------
    def _calculate_hkl(self):
        # get the hkl position from diffract

        motors_positions = self.physical_motors_positions()
        self._macro.diffrac.write_attribute("computehkl", [motors_positions[index] for index in self._column_map])
        return self._macro.diffrac.computehkl

    # ----------------------------------------------------------------------
    def get_motors_position(self):
        # this function calculate the motors position for datacollector (which finally will be written to scan data)

        if self._macro.space == 'reciprocal':
            hkl_value = self._calculate_hkl()
            return np.array([hkl_value[HKL_MOTORS_MAP[device]] for device in self._movable_names])

        else:
            return self.physical_motors_positions()

    # ----------------------------------------------------------------------
    def setup_main_motor(self, motor_index):
        self._main_motor = motor_index

    # ----------------------------------------------------------------------
    def get_main_motor_position(self):
        # position of the motor which is used to start and stop acquisition
        self.physical_motors_positions()
        return self._last_positions[self._main_motor]

    # ----------------------------------------------------------------------
    def physical_motors_positions(self):
        raise RuntimeError('Non implemented!')

    # ----------------------------------------------------------------------
    def _is_in_move(self):
        raise RuntimeError('Non implemented!')

    # ----------------------------------------------------------------------
    def close(self):
        # stops all threads
        self._stop = True

        self._movement_monitor.stop()
        while self._movement_monitor_state != 'stopped':
            time.sleep(REFRESH_PERIOD)

        self._macro.report_debug('MovingGroupPosition stopped')

# ----------------------------------------------------------------------
class ParallelMovement(Movement):

    ### In case of ParallelMovement we try to do inout in parallel.
    # To do it we start a MotorWorker for each motor and send our requests them
    # However, the OMS server can die if we use >2 motors...
    ###

    def __init__(self, macro, motors_list, movable_names, error_queue):
        super(ParallelMovement, self).__init__(macro, motors_list, movable_names, error_queue)

        # we can try to improve reliability by locking requests to Tango -
        # in that case this will be something in between parallel and serial communication
        # this can be activated by MOTOR_COMMAND_LOCK in constans
        self._lock = Lock()

        # since the workers are independent - we need the barrier to understand that they are done with request
        self._position_measured = EndMeasurementBarrier(len(motors_list))
        self._moved_done = EndMeasurementBarrier(len(motors_list))

        # this trigger is used to start position measurements
        self._position_measure_trigger = [Queue() for _ in motors_list]

        self._devices = [MotorWorker(motor, trigger, my_ind, self._position_measured, self._moved_done, self._lock,
                                     macro, error_queue)
                         for my_ind, (motor, trigger) in enumerate(zip(motors_list, self._position_measure_trigger))]

    # ----------------------------------------------------------------------
    def _is_in_move(self):
        return not self._moved_done.wait(False)

    # ----------------------------------------------------------------------
    def move_full_speed(self, positions, mode='sync', monitor=False):
        # we just pass the coordinate to motor workers and wait for the movement to be finished
        for device, position in zip(self._devices, positions):
            device.move_full_speed(position, mode)

        self._wait_move(monitor if mode == 'sync' else False)

        self._macro.report_debug('Full speed movement done')

    # ----------------------------------------------------------------------
    def movevcc(self, command_lists, mode='sync', monitor=False):
        # we just pass the cmd list to motor workers and wait for the movement to be finished
        for device, command_list in zip(self._devices, command_lists):
            device.move_vvc(command_list, mode)

        self._wait_move(monitor if mode == 'sync' else False)

        self._macro.report_debug('VVC speed movement done')

    # ----------------------------------------------------------------------
    def stop_move(self):
        for device in self._devices:
            device.stop_move()

    # ----------------------------------------------------------------------
    def physical_motors_positions(self):
        # first we check do we need to update positions
        if time.time() - self._last_refresh_period > MOTOR_POSITION_REFRESH_PERIOD:

            # if yes - we trigger all motor workers and wait untill they reported (or timeout happend)
            for ind, trigger in enumerate(self._position_measure_trigger):
                trigger.put(1)

            start_time = time.time()
            measured = False
            while not measured and time.time() - start_time < TIMEOUT:
                measured = self._position_measured.wait(False)
                time.sleep(REFRESH_PERIOD)

            # if timeout - raise the error
            if not measured:
                raise RuntimeError('Cannot measure motors position!')

            self._last_positions = np.array([device.position for device in self._devices])
            self._last_refresh_period = time.time()

        return self._last_positions

    # ----------------------------------------------------------------------
    def close(self):

        for device in self._devices:
            device.close()

        super(ParallelMovement, self).close()

# ---------------------------------------------------------------------
class SerialMovement(Movement):

    def __init__(self, macro, motors_list, movable_names, error_queue):
        super(SerialMovement, self).__init__(macro, motors_list, movable_names, error_queue)

        self._devices = []

        for motor in motors_list:
            if hasattr(motor, 'TangoDevice'):
                self._devices.append(PyTango.DeviceProxy(motor.TangoDevice))
            else:
                try:
                    self._devices.append(PyTango.DeviceProxy(motor.full_name.strip('tango://')))
                except:
                    raise RuntimeError('Cannot parse motor {}'.format(motor.name))

    # ----------------------------------------------------------------------
    def _is_in_move(self):
        # TODO: plug the Status monitor

        return np.any(np.array([proxy.state() is PyTango.DevState.MOVING for proxy in self._devices]))

    # ----------------------------------------------------------------------
    def move_full_speed(self, positions, mode='sync', monitor=False):
        # just write a new position to each motor and wait, if needed
        try:
            self._macro.report_debug('Start full speed movement')

            for device, position in zip(self._devices, positions):
                device.position = position

            self._wait_move(monitor if mode == 'sync' else False)

            self._macro.report_debug('Full speed movement done')

        except Exception as err:
            self._macro.error('Error during move: {}'.format(err))
            self._error_queue.put(err)

    # ----------------------------------------------------------------------
    def movevcc(self, command_lists, mode='sync', monitor=False):

        try:
            self._macro.report_debug('Start vvc movement')

            # first we check that motor is not in a closed loop,
            # in case if yes - we store it, switch motor to open loop, and reset after move

            close_loop_state = [None for _ in self._devices]
            for ind, (device, command_list) in enumerate(zip(self._devices, command_lists)):

                # in case motor does not need move main class will send us None as instruction, so we skip it
                if command_list is not None:
                    close_loop_state[ind] = send_movevvc_command(device, command_list)
                    if close_loop_state[ind] is not None:
                        self._macro.report_debug('{} was in closed loop, switching'.format(device.name()))

            self._wait_move(monitor if mode == 'sync' else False)

            for state, device in zip(close_loop_state, self._devices):
                if state is not None:
                    device.FlagClosedLoop = state

            self._macro.report_debug('VVC movement done')
        except Exception as err:
            self._macro.error('Error during move: {}'.format(err))
            self._error_queue.put(err)

    # ----------------------------------------------------------------------
    def physical_motors_positions(self):

        if time.time() - self._last_refresh_period > MOTOR_POSITION_REFRESH_PERIOD:
            self._last_positions = np.array([device.position for device in self._devices])
            self._last_refresh_period = time.time()

        return self._last_positions

    # ----------------------------------------------------------------------
    def stop_move(self):
        for device in self._devices:
            device.StopMove()

# ----------------------------------------------------------------------
#                       Motor position
# ----------------------------------------------------------------------


class MotorWorker(object):
    ### the individual worker for every motor in Parallel movement. Has two threads:
    # position_worker - waits for trigger and reports the position
    # movement_worker - waits for the movement instruction, send them to motor and, if needed, waits till the motion is finished

    def __init__(self, motor, trigger, my_ind, position_barrier, movement_barrier, lock, macro, error_queue):

        self._macro = macro

        try:
            self._device_proxy = PyTango.DeviceProxy(motor.TangoDevice)
            self._name = self._device_proxy.name()
        except:
            self._device_proxy = motor
            self._name = self._device_proxy.name

        self._sardana_name = motor.getName()

        # tango inout lock
        self._lock = lock

        # id to be reported to barrier
        self._my_id = my_ind
        self._position_barrier = position_barrier
        self._movement_barrier = movement_barrier

        # movement status monitor
        self._status_monitor = Status_Monitor(self._device_proxy, mode='status')

        # trigger to measure position
        self._measure_position_trigger = trigger

        self.position = None

        self._position_worker = ExcThread(self._position_loop, self._name + '_position', error_queue)
        self._position_worker_state = 'idle'
        self._position_worker.start()

        self._command_queue = Queue()

        self._movement_worker = ExcThread(self._movement_loop, self._name + '_movement', error_queue)
        self._movement_worker_state = 'idle'
        self._movement_worker.start()

    # ----------------------------------------------------------------------
    def name(self):
        return self._name

    # ----------------------------------------------------------------------
    def _position_loop(self):

        self._position_worker_state = 'running'
        while not self._position_worker.stopped():

            try:
                ind = self._measure_position_trigger.get(block=False)
                try:
                    self.position = self._device_proxy.Position

                except Exception as err:
                    self._macro.report_debug('Cannot measure position of {}: {}'.format(self._name, err))
                    self.position = -np.Inf

                self._position_barrier.report(self._my_id)

            except empty_queue:
                time.sleep(REFRESH_PERIOD)

        self._position_worker_state = 'stopped'

    # ----------------------------------------------------------------------
    def _movement_loop(self):

        self._movement_worker_state = 'running'

        _in_move = False
        old_close_loop_state = None

        self._status_monitor.start()

        while not self._movement_worker.stopped():
            try:
                (mode, command, sync) = self._command_queue.get(block=False)
                try:

                    if mode == 'move_full_speed':
                        if self._device_proxy.Position != command:
                            if sync == 'sync':
                                self._status_monitor.reset()
                                _in_move = True

                            # we can just pass the command to motor, or wait till the lock is free
                            if MOTOR_COMMAND_LOCK:
                                with self._lock:
                                    self._device_proxy.Position = command
                            else:
                                self._device_proxy.Position = command

                            if sync != 'sync':
                                msg = '{}({}) non-sync move to {} started, reported'.format(self._sardana_name, self._name, command)
                                self._movement_barrier.report(self._my_id)
                            else:
                                msg = '{}({}) sync move to {} started'.format(self._sardana_name, self._name, command)
                        else:
                            msg = '{}({}) does not need move, reported'.format(self._sardana_name, self._name)
                            self._movement_barrier.report(self._my_id)

                    elif mode == 'move_vcc':

                        if sync == 'sync':
                            self._status_monitor.reset()
                            _in_move = True

                        # first we check that motor is not in a closed loop,
                        # in case if yes - we store it, switch motor to open loop, and reset after move

                        if MOTOR_COMMAND_LOCK:
                            with self._lock:
                                old_close_loop_state = send_movevvc_command(self._device_proxy, command)
                        else:
                            old_close_loop_state = send_movevvc_command(self._device_proxy, command)

                        if old_close_loop_state is not None:
                            self._macro.report_debug('{} was in closed loop, switching'.format(self._name))

                        if sync != 'sync':
                            msg = '{}({}) non-sync vvc move started, reported'.format(self._sardana_name, self._name)
                            if old_close_loop_state is not None:
                                self._macro.info('{}: closed loop changed to off!')
                        else:
                            msg = '{}({}) sync vvc move started'.format(self._sardana_name, self._name)
                    else:
                        raise RuntimeError('Error, unknown move mode!')
                    self._macro.report_debug(msg)

                except Exception as err:

                    self._macro.report_debug('ERROR during {}: {}'.format(mode, err))
                    # if the _in_move was activated
                    if _in_move:
                        self._macro.report_debug('{}({}) did not move, reported'.format(self._sardana_name, self._name))
                        _in_move = False

                    # or FlagClosedLoop was changed
                    if old_close_loop_state is not None:
                        self._device_proxy.FlagClosedLoop = old_close_loop_state
                    self._movement_barrier.report(self._my_id)

            except empty_queue:
                pass

            # if we need to wait till motion is finished:
            if _in_move:
                if self._status_monitor.moved():

                    self._macro.report_debug('{}({}) finished move, reported'.format(self._sardana_name, self._name))
                    _in_move = False

                    # if we need to reset the FlagClosedLoop
                    if old_close_loop_state is not None:
                        self._device_proxy.FlagClosedLoop = old_close_loop_state

                    self._movement_barrier.report(self._my_id)

            time.sleep(REFRESH_PERIOD)

        self._status_monitor.stop_monitor()
        self._movement_worker_state = 'stopped'

    # ----------------------------------------------------------------------
    def stop_move(self):
        # we trying to stop the motor and in case of failure raise error

        self._device_proxy.StopMove()

        _time_out= time.time()
        while self._device_proxy.State == PyTango.DevState.MOVING and time.time() - _time_out < 3:
            time.sleep(REFRESH_PERIOD)

        if self._device_proxy.State == PyTango.DevState.MOVING:
            raise RuntimeError('Cannot stop {}'.format(self._name))

    # ----------------------------------------------------------------------
    def move_full_speed(self, position, mode):

        self._command_queue.put(('move_full_speed', position, mode))

    # ----------------------------------------------------------------------
    def move_vvc(self, command_list, mode):

        # in case motor does not need move main class will send us None as instruction
        # - so we instantly report that we moved

        if command_list is not None:
            self._command_queue.put(('move_vcc', command_list, mode))
        else:
            self._movement_barrier.report(self._my_id)

    # ----------------------------------------------------------------------
    def close(self):

        self._position_worker.stop()
        self._movement_worker.stop()

        while self._movement_worker_state != "stopped" and self._position_worker_state != "stopped":
            time.sleep(REFRESH_PERIOD)

# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------

class Status_Monitor(object):

    POOLING_PERIOD = 5

    attributes = ["state"]

    def __init__(self, proxy=None, address='', mode='event'):
        super(Status_Monitor, self).__init__()

        if proxy is not None:
            self.device_proxy = proxy
        elif address != '':
            self.device_proxy = PyTango.DeviceProxy(address)
        else:
            raise RuntimeError('Either proxy, either address is needed')

        self._events = []

        self._mode = mode
        self._start_values = dict.fromkeys(self.attributes)
        self._intermediate_values = dict.fromkeys(self.attributes)
        self._in_move = dict.fromkeys(self.attributes, False)
        self._move_completed = dict.fromkeys(self.attributes, False)

        for attribute in self.attributes:
            self._start_values[attribute] = self.device_proxy.read_attribute(attribute).value

    # ----------------------------------------------------------------------
    def __del__(self):
        if self._mode == 'event':
            self.stop_monitor()

    # ----------------------------------------------------------------------
    def start(self):
        if self._mode == 'event':
            for attribute in self.attributes:
                self.device_proxy.poll_attribute(attribute, self.POOLING_PERIOD)
                self._events.append(self.device_proxy.subscribe_event(attribute, PyTango.EventType.CHANGE_EVENT, self.push_event, []))
            self.reset()

    # ----------------------------------------------------------------------
    def reset(self):
        for attribute in self.attributes:
            self._start_values[attribute] = self.device_proxy.read_attribute(attribute).value
            self._intermediate_values[attribute] = False
            self._in_move[attribute] = False
            self._move_completed[attribute] = False

    # ----------------------------------------------------------------------
    def push_event(self, event):
        print(event)
        if not event.err:
            name = event.attr_name.split('/')[-1]
            if name in self.attributes:
                if not self._in_move[name] and event.attr_value.value != self._start_values[name]:
                    self._in_move[name] = True
                    self._intermediate_values[name] = event.attr_value.value
                elif self._in_move[name] and event.attr_value.value != self._intermediate_values[name]:
                    self._move_completed[name] = True
        else:
            print(event.errors)
            raise RuntimeError(event.errors)

    # ----------------------------------------------------------------------
    def stop_monitor(self):
        if self._mode == 'event':
            for event in self._events:
                self.device_proxy.unsubscribe_event(event)
            for attribute in self.attributes:
                self.device_proxy.stop_poll_attribute(attribute)

    # ----------------------------------------------------------------------
    def moved(self):
        if self._mode == 'event':
            done = True
            for value in self._move_completed.values():
                done *= value

            return done
        else:
            return self.device_proxy.State() == PyTango.DevState.ON

    # ----------------------------------------------------------------------
    def in_move(self):
        if self._mode == 'event':
            move = True
            for value in self._in_move.values():
                move *= value

            return move
        else:
            return self.device_proxy.State() == PyTango.DevState.MOVING
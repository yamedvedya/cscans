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
import numpy as np
import os

# cscans imports
from cs_axillary_functions import ExcThread
from cs_constants import *


class SerialMovement(object):

    def __init__(self, move_mode, macro, motors_list, movable_names, error_queue):

        self._error_queue = error_queue
        self._macro = macro
        self._move_mode = move_mode

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
        if time.time() - self._last_refresh_period > MOTOR_POSITION_REFRESH_PERIOD:
            self._last_positions = np.array([device.position for device in self._devices])
            self._last_refresh_period = time.time()

        return self._last_positions

    # ----------------------------------------------------------------------
    def _is_in_move(self):
        return np.any(np.array([proxy.state() is PyTango.DevState.MOVING for proxy in self._devices]))

    # ----------------------------------------------------------------------
    def stop_move(self):
        self._macro.report_debug('Stopping motors')
        for device in self._devices:
            device.StopMove()

    # ----------------------------------------------------------------------
    def is_moving(self):
        return self._is_in_move()

    # ----------------------------------------------------------------------
    def close(self):
        # stops all threads
        self._stop = True

        self._movement_monitor.stop()
        while self._movement_monitor_state != 'stopped':
            time.sleep(REFRESH_PERIOD)

        self._macro.report_debug('MovingGroupPosition stopped')

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
    def move_slowed(self, command_lists, mode='sync', monitor=False):

        try:
            self._macro.report_debug('Start slow movement')

            # first we check that motor is not in a closed loop,
            # in case if yes - we store it, switch motor to open loop, and reset after move

            close_loop_state = [None for _ in self._devices]
            for ind, (device, command_list) in enumerate(zip(self._devices, command_lists)):

                # in case motor does not need move main class will send us None as instruction, so we skip it
                if command_list is not None:
                    close_loop_state[ind] = send_move_command(device, command_list, self._move_mode, self._macro)
                    if close_loop_state[ind] is not None:
                        self._macro.report_debug('{} was in closed loop, switching'.format(device.name()))

            self._wait_move(monitor if mode == 'sync' else False)

            for state, device in zip(close_loop_state, self._devices):
                if state is not None:
                    device.FlagClosedLoop = state

            self._macro.report_debug('Slow movement done')

        except Exception as err:
            self._macro.error('Error during move: {}'.format(err))
            self._error_queue.put(err)


# ---------------------------------------------------------------------
# ---------------------------------------------------------------------
# ---------------------------------------------------------------------
def send_move_command(device, command_list, mode, macro):
    close_loop_state = None

    if hasattr(device, 'movevvc') and hasattr(device, 'conversion') and mode == 'vvc':
        if device.FlagClosedLoop:
            close_loop_state = device.FlagClosedLoop
            device.FlagClosedLoop = 0

        device.movevvc(["slew: {}, position: {}".format(int(speed * device.conversion), position)
                        for speed, position in command_list])
    else:
        if len(command_list) > 1:
            raise RuntimeError('The variable speed move in this mode is not possible')

        with open(TMP_FILE, 'w') as f:
            if hasattr(device, 'slewrate') and hasattr(device, 'conversion'):
                old_slewrate = device.slewrate
                f.write('{};slewrate;{}\n'.format(device.name(), old_speed))
                device.slewrate = int(command_list[0][0] * device.conversion)
                device.position = command_list[0][1]
                macro.report_debug(
                    '{} slew rate changed from {} to {}'.format(device.name(), old_speed, device.slewrate))

            elif hasattr(device, 'velocity'):
                f.write('{};velocity;{}\n'.format(device.name(), device.velocity))
                device.velocity = command_list[0][0]
                device.position = command_list[0][1]
                macro.report_debug(
                    '{} velocity changed from {} to {}'.format(device.name(), old_speed, device.slewrate))
            else:
                raise RuntimeError('Could not move {}'.format(device.name()))

    return close_loop_state
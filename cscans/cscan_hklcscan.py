'''
This is child of CSScan with modified functionality

Author yury.matveev@desy.de
'''

# general python imports
import sys
if sys.version_info.major >= 3:
    from queue import Queue
    from queue import Empty as empty_queue
    old_python = False
else:
    from Queue import Queue
    from Queue import Empty as empty_queue
    old_python = True

import numpy as np
import PyTango
import time

# Sardana imports
from sardana.util.motion import Motor as VMotor

# cscan imports, always reloaded to track changes
from cscans.cscan_ccscan import CCScan
from cscans.cscan_movement_monitor import Status_Monitor

from cscans.cscan_constants import *


# ----------------------------------------------------------------------
#
# ----------------------------------------------------------------------

class HklCScan(CCScan):

    def __init__(self, macro, waypointGenerator=None, periodGenerator=None,
                 moveables=[], env={}, constraints=[], extrainfodesc=[]):
        super(HklCScan, self).__init__(macro, waypointGenerator, periodGenerator,
                 moveables, env, constraints, extrainfodesc)

        self._command_lists = []
        self._start_positions = None
        self._motors_with_movement = None
        self._motion_monitor_motors = None

        # for motor in self._physical_moveables:
        #     for param in ['getBaseRate', 'getVelocity', 'getAcceleration', 'getName']:
        #         self.macro.output('{}: {}'.format(param, getattr(motor, param)()))
        self.prepare_waypoint(self.macro._step_info)

        # self.macro.output('moveables_trees {}'.format(self._moveables_trees))
        # self.macro.output('physical_motion {}'.format(self._physical_motion))
        # self.macro.output('physical_moveables {}'.format(self._physical_moveables))
        raise RuntimeError('Test run')

    # ----------------------------------------------------------------------
    def _check_motors_acceleration(self):
        accel = 0

        for motor in self._physical_moveables:
            try:
                accel = max(accel, motor.getAcceleration())
            except AttributeError:
                raise RuntimeError("{} don't have Acceleration parameter, cscans is impossible".format(motor))

        self.macro.report_debug('All motors acceleration will be set to {}'.format(accel))

        for motor in self._physical_moveables:
            motor.Acceleration = accel

    # ----------------------------------------------------------------------
    def _get_positions_for_hkl(self, hkl_values):

        self.macro.diffrac.write_attribute("computetrajectoriessim", hkl_values)
        return self.macro.diffrac.trajectorylist[0]

    # ----------------------------------------------------------------------
    def _get_speed_for_dx(self, v_st, dx, dt, a):
        if v_st*dt > dx:
            a *= -1

        b = -2*(a*dt + v_st)
        c = np.square(v_st) + 2*a*dx
        D = np.square(b) - 4*c

        v_end = -(b+np.sqrt(D))/2
        t = (v_end-v_st)/a

        #only one root has a physical meaning
        if 0 < t < dt:
            return v_end
        else:
            return -(b-np.sqrt(D))/2

    # ----------------------------------------------------------------------
    def _calculate_speeds(self, trajectory, ideal_period):
        real_period = ideal_period
        real_accels = np.zeros(trajectory.shape[1])
        for mot_ind, motor in enumerate(self._physical_moveables):
            try:
                motor_vel = motor.getVelocity()
            except AttributeError:
                raise RuntimeError("{} don't have Velocity parameter, cscans is impossible".format(motor))

            max_motor_period = np.max(np.abs(np.diff(trajectory[:, mot_ind])) / motor_vel)
            if max_motor_period > real_period:
                real_period = max_motor_period
                self.macro.output(
                    'The required travel time cannot be reached due to {} cannot travel with such high speed'.format(
                        motor.getName()))

            try:
                motor_accel = motor.getAcceleration()
            except AttributeError:
                raise RuntimeError("{} don't have Acceleration parameter, cscans is impossible".format(motor))
            real_accels[mot_ind] = motor_vel / motor_accel

            accel_correction = np.sqrt(
                np.max(np.abs(np.diff(np.diff(trajectory[:, mot_ind]) / real_period) / real_period))
                / real_accels[mot_ind])
            if accel_correction > 1:
                self.macro.output(
                    'The required travel time cannot be reached due to {} cannot accelerate so fast'.format(
                        motor.getName()))
                real_period *= accel_correction

        self.macro.report_debug('ideal_period {} period {}'.format(ideal_period, real_period))

        # calculate ideal start speed:
        _solution_found = False
        _attempt = 0
        _motor_has_movement = [False for _ in range(len(self._physical_moveables))]
        _movement_monitor = [True for _ in range(len(self._physical_moveables))]

        while not _solution_found and _attempt < 1000:
            _attempt += 1
            _solution_found = True
            calculated_speeds = np.zeros_like(trajectory)

            for mot_ind, motor in enumerate(self._physical_moveables):
                if np.abs(trajectory[1, mot_ind] - trajectory[0, mot_ind]) > 0:
                    calculated_speeds[1, mot_ind] = calculated_speeds[0, mot_ind] = \
                        np.abs((trajectory[1, mot_ind] - trajectory[0, mot_ind])) / real_period
                    _motor_has_movement[mot_ind] = True
                else:
                    calculated_speeds[1, mot_ind] = calculated_speeds[0, mot_ind] = 0
                    _movement_monitor[mot_ind] = False

                for idx, dx in enumerate(np.diff(trajectory[1:, mot_ind])):
                    if np.abs(dx) > 0:
                        _motor_has_movement[mot_ind] = True
                        _new_speed = self._get_speed_for_dx(calculated_speeds[idx + 1, mot_ind],
                                                            np.abs(dx), real_period, real_accels[mot_ind])

                        if np.isnan(_new_speed):
                            _solution_found = False
                            real_period *= 1.01
                            self.macro.report_debug('Solution not found. Need to increase period to{}'.format(real_period))
                            break
                        else:
                            calculated_speeds[idx + 2, mot_ind] = _new_speed
                    else:
                        _movement_monitor = False
                        calculated_speeds[idx + 2, mot_ind] = calculated_speeds[idx + 1, mot_ind]

        if not _solution_found:
            raise RuntimeError("Cannot find solution")

        start_positions = np.zeros(trajectory.shape[1])
        end_positions = np.zeros(trajectory.shape[1])

        for mot_ind, motor in enumerate(self._physical_moveables):
            disp_sign = np.sign(trajectory[1, mot_ind] - trajectory[0, mot_ind])

            start_positions[mot_ind] = trajectory[0, mot_ind] - disp_sign * VMotor(min_vel=motor.getBaseRate(),
                                                                                   max_vel=calculated_speeds[
                                                                                       0, mot_ind],
                                                                                   accel_time=motor.getAcceleration(),
                                                                                   decel_time=1e-15).displacement_reach_max_vel

            if not self._check_motor_limits(motor, start_positions[mot_ind]):
                raise RuntimeError(
                    'Cscan cannot be performed due to the start overhead for {} is out of the limits'.format(
                        motor.getName()))

            end_positions[mot_ind] = trajectory[-1, mot_ind] + disp_sign * VMotor(min_vel=motor.getBaseRate(),
                                                                                  max_vel=calculated_speeds[
                                                                                      -1, mot_ind],
                                                                                  accel_time=1e-15,
                                                                                  decel_time=motor.getAcceleration()).displacement_reach_min_vel
            if not self._check_motor_limits(motor, start_positions[mot_ind]):
                raise RuntimeError(
                    'Cscan cannot be performed due to the stop overhead for {} is out of the limits'.format(
                        motor.getName()))

        return calculated_speeds, start_positions, end_positions, real_period / ideal_period, \
               _motor_has_movement, _movement_monitor

    # ----------------------------------------------------------------------
    def prepare_waypoint(self, waypoint):
        ### This function basically repeats the original, The only difference is that "slow_down" factor changed
        ### to fixed travel time, defined by waypoint['integ_time']*waypoint['npts']
        ### some variable were renamed to make code more readable

        self.macro.report_debug("prepare_waypoint() entering...")

        self._acq_duration = waypoint["integ_time"] * waypoint["npts"]

        # first calculate how many steps are needed for each movement:
        n_steps = 0
        for start, stop in zip(waypoint['start_positions'], waypoint['positions']):
            n_steps = max(n_steps, int(np.ceil(np.abs(stop-start)/HKL_GRID_RESOLUTION)) + 1)

        # then generate 3xsteps matrix with current positions:
        _hkl_trajectory = np.ones((n_steps, 3))
        _hkl_trajectory[:, 0] = self.macro.h_device.position
        _hkl_trajectory[:, 1] = self.macro.k_device.position
        _hkl_trajectory[:, 2] = self.macro.l_device.position

        column_map = {'e6c_h': 0, 'e6c_k': 1, 'e6c_l': 2}

        for motor, start, stop in zip(self.moveables, waypoint['start_positions'], waypoint['positions']):
            _hkl_trajectory[:, column_map[motor.name]] = np.linspace(start, stop, n_steps)

        # self.macro.report_debug(_hkl_trajectory)

        _real_trajectory = np.zeros((n_steps, self.macro.nb_motors))
        column_map = {}
        for ind, motor in enumerate(self._physical_moveables):
            column_map[motor.getName()] = ind

        _point_device_keys = [self.macro.angle_device_names[angle] for angle in self.macro.angle_names]

        for step, (h, k, l) in enumerate(_hkl_trajectory):
            _point = self._get_positions_for_hkl([h, k, l])
            for _coordinate, _key in zip(_point, _point_device_keys):
                _real_trajectory[step, column_map[_key]] = _coordinate

        # self.macro.report_debug('_real_trajectory: {}'.format(_real_trajectory))

        calculated_speeds, self._start_positions, end_positions, integration_time_correction, \
        self._motors_with_movement, self._motion_monitor_motors = \
            self._calculate_speeds(_real_trajectory, self._acq_duration / n_steps)

        self._integration_time = waypoint["integ_time"] * integration_time_correction

        for ind, _ in enumerate(self._physical_moveables):
            cmd = ["slew: {}, position: {}".format(slew_rate, position) for slew_rate, position in
                    zip(calculated_speeds[:, ind], _real_trajectory[:, ind])]
            cmd.append("slew: {}, position: {}".format(calculated_speeds[-1, ind], end_positions[ind]))

            self._command_lists.append(cmd)

        self._position_start = _real_trajectory[0, 0]
        self._movement_direction = True if _real_trajectory[-1, 0] - _real_trajectory[0, 0] > 0 else False
        self._position_stop = _real_trajectory[-1, 0]

    # ----------------------------------------------------------------------
    def _go_through_waypoints(self):
        """
        Internal, unprotected method to go through the different waypoints.
        """
        self.macro.report_debug("_go_through_waypoints() entering...")

        for _, waypoint in self.steps:

            # get integ_time for this loop
            if old_python:
                _, step_info = self.period_steps.next()
            else:
                _, step_info = next(self.period_steps)

            self._data_collector.set_new_step_info(step_info)
            self._timer_worker.set_new_period(self._integration_time)

            if self._has_lambda:
                self._setup_lambda(self._integration_time)

            # execute pre-move hooks
            for hook in waypoint.get('pre-move-hooks', []):
                hook()

            if self.macro.isStopped():
                return

            # move to start position
            self.macro.report_debug("Moving to start position: {}".format(self._start_positions))
            self._physical_motion.move(self._start_positions)

            _motor_proxies = [PyTango.DeviceProxy(motor.TangoDevice) for motor in self._physical_moveables]
            _movement_monitor = Status_Monitor(_motor_proxies[0])

            if self.macro.isStopped():
                return

            self._timer_worker.set_start_position()

            self.motion_event.set()

            # move to waypoint end position
            self.macro.report_debug("Start moving")

            _movement_monitor.start()

            for ind, proxy in enumerate(_motor_proxies):
                proxy.command_inout("movevvc", self._command_lists[ind])

            while not _movement_monitor.moved():
                time.sleep(0.1)

            self.motion_event.clear()
            # self.macro.output("Clear motion event, state: {}".format(self.motion_event.is_set()))

            # execute post-move hooks
            for hook in waypoint.get('post-move-hooks', []):
                hook()


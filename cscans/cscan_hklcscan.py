'''
This is child of CSScan with modified functionality

Author yury.matveev@desy.de
'''

# general python imports
import time
import PyTango

import numpy as np

# Sardana imports
from sardana.util.motion import Motor as VMotor
from sardana.util.motion import MotionPath

# cscan imports, always reloaded to track changes
from cscan_ccscan import CCScan
from cscan_constants import *

# ----------------------------------------------------------------------
#
# ----------------------------------------------------------------------

class HklCScan(CCScan):

    def __init__(self, macro, waypointGenerator=None, periodGenerator=None,
                 moveables=[], env={}, constraints=[], extrainfodesc=[]):
        super(HklCScan, self).__init__(macro, waypointGenerator, periodGenerator,
                 moveables, env, constraints, extrainfodesc)

        for motor in self._physical_moveables:
            for param in ['getBaseRate', 'getVelocity', 'getAcceleration', 'getName']:
                self.macro.output('{}: {}'.format(param, getattr(motor, param)()))
        self._go_through_waypoints()

        self.macro.output('moveables_trees {}'.format(self._moveables_trees))
        self.macro.output('physical_motion {}'.format(self._physical_motion))
        self.macro.output('physical_moveables {}'.format(self._physical_moveables))
        raise RuntimeError('Test run')

    # ----------------------------------------------------------------------
    def _check_motors_acceleration(self):
        accel = 0

        for motor in self._physical_moveables:
            try:
                accel = max(accel, motor.getAcceleration())
            except AttributeError:
                raise RuntimeError("{} don't have Acceleration parameter, cscans is impossible".format(motor))

        if debug:
            self.macro.output('All motors acceleration will be set to {}'.format(accel))

        for motor in self._physical_moveables:
            motor.Acceleration = accel

    # ----------------------------------------------------------------------
    def _get_positions_for_hkl(self, hkl_values):

        self.macro.diffrac.write_attribute("computetrajectoriessim", hkl_values)
        return self.macro.diffrac.trajectorylist[0]

    # ----------------------------------------------------------------------
    def _check_speed_for_segments(self, trajectory, slow_down=1):

        calculated_paths = np.empty((trajectory.shape[0] - 1, trajectory.shape[1])).tolist()

        for mot_ind, motor in enumerate(self._physical_moveables):
            try:
                motor_vel = motor.getVelocity()
            except AttributeError:
                raise RuntimeError("{} don't have Velocity parameter, cscans is impossible".format(motor))

            motor_accel = motor.getAcceleration()

            for point_ind in range(trajectory.shape[0] - 1):

                motor_path = MotionPath(VMotor(min_vel=motor_vel, max_vel=motor_vel,
                                               accel_time=motor_accel, decel_time=1e-15),
                                        trajectory[point_ind, mot_ind], trajectory[point_ind+1, mot_ind])


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

    # ----------------------------------------------------------------------
    def _calculate_start_displacement(self):
        try:
            base_vel = motor.getBaseRate()
        except AttributeError:
            raise RuntimeError("{} don't have BaseRate parameter, cscans is impossible".format(motor))
    # ----------------------------------------------------------------------
    def prepare_waypoint(self, waypoint, iterate_only=False):
        ### This function basically repeats the original, The only difference is that "slow_down" factor changed
        ### to fixed travel time, defined by waypoint['integ_time']*waypoint['npts']
        ### some variable were renamed to make code more readable

        self.debug("prepare_waypoint() entering...")

        travel_time = waypoint["integ_time"] * waypoint["npts"]

        # first calculate how many steps are needed for each movement:
        n_steps = 0
        for start, stop in zip(waypoint['start_positions'], waypoint['positions']):
            n_steps = max(n_steps, int(np.ceil(np.abs(stop-start)/HKL_GRID_RESOLUTION)) + 1)

        # then generate 3xsteps matrix with current positions:
        _hkl_trajectory = np.ones((n_steps, 3))
        _hkl_trajectory[:, 0] = self.macro.h_device.position
        _hkl_trajectory[:, 1] = self.macro.k_device.position
        _hkl_trajectory[:, 2] = self.macro.l_device.position

        column_map = {'h4c_h': 0, 'h4c_k': 1, 'h4c_l': 2}

        for motor, start, stop in zip(self.moveables, waypoint['start_positions'], waypoint['positions']):
            _hkl_trajectory[:, column_map[motor.name]] = np.linspace(start, stop, n_steps)

        if debug:
            self.macro.output(_hkl_trajectory)

        _real_trajectory = np.zeros((n_steps, self.macro.nb_motors))
        column_map = {}
        for ind, motor in enumerate(self._physical_moveables):
            column_map[motor.getName()] = ind

        _point_device_keys = [self.macro.angle_device_names[angle] for angle in self.macro.angle_names]

        for step, (h, k, l) in enumerate(_hkl_trajectory):
            _point = self._get_positions_for_hkl([h, k, l])
            for _coordinate, _key in zip(_point, _point_device_keys):
                _real_trajectory[step, column_map[_key]] = _coordinate

        if debug:
            self.macro.output('_real_trajectory: {}'.format(_real_trajectory))

        raise RuntimeError('Test run')
    # ----------------------------------------------------------------------
    def _go_through_waypoints(self):
        """
        Internal, unprotected method to go through the different waypoints.
        """
        self.macro.output("_go_through_waypoints() entering...")

        self._check_motors_acceleration()

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
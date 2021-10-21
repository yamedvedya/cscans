'''
This is child of CCScan with modified functionality

Author yury.matveev@desy.de
'''

# general python imports
import numpy as np

# cscans imports, always reloaded to track changes
from cs_ccscan import CCScan
from cs_constants import *

# ----------------------------------------------------------------------
#
# ----------------------------------------------------------------------

class HklCScan(CCScan):

    def __init__(self, macro, waypointGenerator=None, periodGenerator=None,
                 moveables=[], env={}, constraints=[], extrainfodesc=[],
                 constants=None, motors_settings=None):

        super(HklCScan, self).__init__(macro, waypointGenerator, periodGenerator,
                 moveables, env, constraints, extrainfodesc, move_mode='vvc')

        self._main_motor_found = False

        self.constants = {'hkl_grid': HKL_GRID_RESOLUTION,
                          'lin_move_threshold': LIN_MOVE_THRESHOLD,
                          'min_displacement': MIN_DISPLACEMENT,
                          'position_round': POSITION_ROUND}

        self.motors_settings = ['free' for _ in self._physical_moveables]

        if constants is not None:
            constants = constants.split(';')
            self.constants = {'hkl_grid': float(constants[0]),
                              'lin_move_threshold': float(constants[1]),
                              'min_displacement': float(constants[2]),
                              'position_round': int(constants[3])}

        if motors_settings is not None:
            motors_settings = {pair.split(':')[0]: pair.split(':')[1] for pair in motors_settings.split(';')}
            for ind, motor in enumerate(self._physical_moveables):
                self.motors_settings[ind] = motors_settings[motor.getName()]

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
        if 0 < t <= dt:
            return v_end
        else:
            return -(b-np.sqrt(D))/2

    # ----------------------------------------------------------------------
    def _get_accel_dx(self, motor, end_speed):
        base_speed = motor.getBaseRate()
        accel = motor.getVelocity()/motor.getAcceleration()

        accel_time = (end_speed-base_speed)/accel
        return base_speed*accel_time + accel*np.power(accel_time, 2)/2, accel_time

    # ----------------------------------------------------------------------
    def _get_deccel_dx(self, motor, start_speed):
        base_speed = motor.getBaseRate()
        accel = motor.getVelocity()/motor.getAcceleration()

        accel_time = (start_speed-base_speed)/accel
        return start_speed*accel_time - accel*np.power(accel_time, 2)/2

    # ----------------------------------------------------------------------
    def _find_speeds(self, full_trajectories, ideal_period):

        real_period = ideal_period
        real_accels = np.zeros(full_trajectories.shape[0])

        trajectories = []
        linear_move = []
        motors_with_movement = [False for _ in self._physical_moveables]

        for ind, (trajectory, status) in enumerate(zip(full_trajectories, self.motors_settings)):
            displacement = trajectory - trajectory[0]

            # check if there is a movement:
            if np.any(np.abs(displacement) > self.constants['min_displacement']) and status != 'exclude':
                motors_with_movement[ind] = True

                # check if movement is linear:
                lin_move = trajectory[0] + displacement[-1]/(len(trajectory)-1) * np.arange(len(trajectory))
                if np.any(np.abs(trajectory - lin_move) > self.constants['lin_move_threshold']) and status != 'linear':
                    linear_move.append(False)
                    displacement = np.round(displacement, self.constants['position_round'])

                    reduced_trajectory = np.array([0, trajectory[0]])
                    old_value = displacement[0]
                    for idx, value in enumerate(displacement):
                        if value != old_value:
                            old_value = value
                            reduced_trajectory = np.vstack((reduced_trajectory, np.array([idx, trajectory[idx]])))

                else:
                    linear_move.append(True)
                    reduced_trajectory = np.vstack((np.array([0, trajectory[0]]),
                                                    np.array([len(trajectory), trajectory[-1]])))

                trajectories.append(np.transpose(reduced_trajectory))

                if not self._main_motor_found and (np.all(np.diff(displacement[1:]) > 0)
                                              or np.all(np.diff(displacement[1:]) < 0)):
                    self._main_motor_found = True
                    self.movement.setup_main_motor(ind)
                    self._movement_direction = np.sign(trajectories[ind][1, -1] - trajectories[ind][1, 0])
                    self._pos_start_measurements = trajectories[ind][1, 0]
                    self._pos_stop_measurements = trajectories[ind][1, -1]

                    self.macro.report_debug(
                        'Motor {} taken as motion monitor. Start position {}, final position {}'.format(
                            self._physical_moveables[ind].getName(), self._pos_start_measurements,
                            self._pos_stop_measurements))

            else:
                linear_move.append(None)
                trajectories.append(None)

        # _real_trajectory = np.round(_real_trajectory, MOTORS_POSITION_ROUND)

        if not self._main_motor_found:
            raise RuntimeError('Cannot find motor to monitor movement!')

        for mot_ind, (motor, does_move, linear, trajectory) in enumerate(zip(self._physical_moveables, motors_with_movement,
                                                                  linear_move, trajectories)):
            if does_move:
                try:
                    motor_vel = motor.getVelocity()
                except AttributeError:
                    raise RuntimeError("{} don't have Velocity parameter, cscans is impossible".format(motor))

                speed_per_segment = np.abs(np.diff(trajectory[1])/(real_period*np.diff(trajectory[0])))
                max_speed = np.max(speed_per_segment)
                if max_speed > motor_vel:
                    real_period *= max_speed/motor_vel
                    self.macro.output(
                        'The required travel time cannot be reached due to {} cannot travel with such high speed'.format(
                            motor.getName()))

                try:
                    motor_accel = motor.getAcceleration()
                except AttributeError:
                    raise RuntimeError("{} don't have Acceleration parameter, cscans is impossible".format(motor))
                real_accels[mot_ind] = motor_vel / motor_accel

                if not linear:
                    time_pre_segment = real_period*np.diff(trajectory[0])
                    speed_per_segment = np.abs(np.diff(trajectory[1])/time_pre_segment)
                    max_accel_per_segment = np.max(np.abs(np.diff(speed_per_segment)/time_pre_segment[0:-1]))
                    if max_accel_per_segment > real_accels[mot_ind]:
                        self.macro.output(
                            'The required travel time cannot be reached due to {} cannot accelerate so fast'.format(
                                motor.getName()))
                        real_period *= max_accel_per_segment/real_accels[mot_ind]

        self.macro.report_debug('ideal_period {} period {}'.format(ideal_period, real_period))

        # calculate ideal start speed:
        _solution_found = False
        _attempt = 0
        _main_motor_found = False
        _accel_time = 0

        while not _solution_found and _attempt < 10:
            _attempt += 1
            _solution_found = True
            vvc_lists = [None for _ in self._physical_moveables]

            for ind, (motor, does_move, accel, linear, trajectory) in enumerate(zip(self._physical_moveables,
                                                                                     motors_with_movement,
                                                                                     real_accels,
                                                                                     linear_move,
                                                                                     trajectories)):
                if does_move:
                    displacements = np.abs(np.diff(trajectory[1]))
                    time_pre_segment = np.diff(trajectory[0])*real_period

                    start_speed = displacements[0]/time_pre_segment[0]
                    _accel_time = np.max([start_speed/accel, _accel_time])
                    cmd_list = np.vstack((np.array([start_speed, trajectory[1, 0]]),
                                          np.array([start_speed, trajectory[1, 1]])))

                    if not linear:
                        for idx, (dx, dt) in enumerate(zip(displacements[1:], time_pre_segment[1:])):
                            _new_speed = self._get_speed_for_dx(cmd_list[idx+1, 0], dx, dt, accel)

                            if np.isnan(_new_speed):
                                _solution_found = False
                                real_period *= 1.05
                                self.macro.report_debug('Solution not found. New period {:.3f}'.format(real_period))
                                break
                            else:
                                cmd_list = np.vstack((cmd_list, np.array([_new_speed, trajectory[1, idx+2]])))

                    vvc_lists[ind] = cmd_list

        if not _solution_found:
            raise RuntimeError("Cannot find solution")

        start_positions = full_trajectories[:, 0]

        for mot_ind, (motor, does_move, trajectory, cmd_list) in enumerate(zip(self._physical_moveables,
                                                                               motors_with_movement,
                                                                               full_trajectories, vvc_lists)):

            if does_move:
                dx, dt = self._get_accel_dx(motor, cmd_list[0, 0])
                start_positions[mot_ind] = trajectory[0] - np.sign(trajectory[1] - trajectory[0])\
                                                    *((_accel_time-dt)*cmd_list[0, 0]+dx)


                if not self._check_motor_limits(motor, start_positions[mot_ind]):
                    raise RuntimeError(
                        'Cscan cannot be performed due to the start overhead for {} is out of the limits'.format(
                            motor.getName()))
                cmd_list[-1, 1] = np.round(trajectory[-1] + np.sign(trajectory[-1] - trajectory[-2])
                                           * self._get_deccel_dx(motor, cmd_list[-1, 0]),
                                           self.constants['position_round'])

                if not self._check_motor_limits(motor, start_positions[mot_ind]):
                    raise RuntimeError(
                        'Cscan cannot be performed due to the stop overhead for {} is out of the limits'.format(
                            motor.getName()))

        return vvc_lists, np.round(start_positions, self.constants['position_round']), real_period / ideal_period

    # ----------------------------------------------------------------------
    def _check_movement_direction(self, trajectory):
        break_points = []

        for row in np.diff(trajectory):
            if np.any(row < 0) and np.any(row > 0):
                sign = np.sign(row)

                start_ind = 0
                finished = False
                while not finished:
                    res = np.where(sign[start_ind:] != sign[start_ind])[0]
                    if len(res):
                        start_ind = start_ind + res[0]
                        break_points.append(start_ind)
                    else:
                        finished = True

        if break_points:
            if not self.macro.ask_user('The trajectory is not monotonous, scan will be performed in {} parts. Continue?'.format(len(break_points) + 1)):
                #self.output(str(break_points))
                return False, []
            else:
                break_points.append(trajectory.shape[1])
                break_points.sort()
                return True, break_points
        else:
            return True, [trajectory.shape[1]]

    # ----------------------------------------------------------------------
    def calculate_speed(self, waypoint):

        self.macro.report_debug("prepare_waypoint() entering...")

        _acq_duration = waypoint["integ_time"] * waypoint["npts"]

        # first calculate how many steps are needed for each movement:
        n_steps = 0
        for start, stop in zip(waypoint['start_positions'], waypoint['positions']):
            n_steps = max(n_steps, int(np.ceil(np.abs(stop-start)/self.constants['hkl_grid'])) + 1)

        # then generate 3xsteps matrix with current positions:
        _hkl_trajectory = np.ones((n_steps, 3))
        _hkl_trajectory[:, 0] = self.macro.h_device.position
        _hkl_trajectory[:, 1] = self.macro.k_device.position
        _hkl_trajectory[:, 2] = self.macro.l_device.position

        for motor, start, stop in zip(self.moveables, waypoint['start_positions'], waypoint['positions']):
            _hkl_trajectory[:, HKL_MOTORS_MAP[motor.name]] = np.linspace(start, stop, n_steps)

        # self.macro.report_debug(_hkl_trajectory)

        _real_trajectory = np.zeros((self.macro.nb_motors, n_steps))
        column_map = {}
        for ind, motor in enumerate(self._physical_moveables):
            column_map[motor.getName()] = ind

        _point_device_keys = [self.macro.angle_device_names[angle] for angle in self.macro.angle_names]

        for step, (h, k, l) in enumerate(_hkl_trajectory):
            _point = self._get_positions_for_hkl([h, k, l])
            for _coordinate, _key in zip(_point, _point_device_keys):
                _real_trajectory[column_map[_key], step] = _coordinate

        # _real_trajectory = np.round(_real_trajectory, MOTORS_POSITION_ROUND)

        do_scan, break_points = self._check_movement_direction(_real_trajectory)
        if do_scan:
            _, _, integration_time_correction = self._find_speeds(np.copy(_real_trajectory), _acq_duration / n_steps)

            self._integration_time = waypoint["integ_time"] * integration_time_correction
            if integration_time_correction > 1:
                self.macro.warning(
                    'Integration time was corrected to {} due to slow motor(s)'.format(self._integration_time))

            _acq_duration = self._integration_time * waypoint["npts"]

            start_ind = 0
            self._start_positions = [[] for _ in break_points]
            self._command_lists = [[] for _ in break_points]

            for idx, point in enumerate(break_points):

                vvc_lists, self._start_positions[idx], integration_time_correction = \
                    self._find_speeds(_real_trajectory[:, start_ind:point], _acq_duration / n_steps)

                for cmd_list in vvc_lists:
                    if cmd_list is not None:
                        self._command_lists[idx].append(cmd_list[1:])
                    else:
                        self._command_lists[idx].append(None)
                start_ind = point
        else:
            raise RuntimeError('Non-monotonous movement')
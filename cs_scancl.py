'''
This is main class for continuous scans.

Author yury.matveev@desy.de
'''

# general python imports
import PyTango
import os

# Sardana imports

from sardana.macroserver.macro import iMacro, Hookable
from sardana.macroserver.macros.scan import getCallable, UNCONSTRAINED
from sardana.macroserver.scan.scandata import MoveableDesc

# cscans imports
from cs_ccscan import CCScan
from cs_hklcscan import HklCScan
from cs_constants import *

# ----------------------------------------------------------------------
#
# ----------------------------------------------------------------------

class CscanClass(Hookable):

    def _prepare(self, mode, space, motor, start_pos, final_pos, nb_steps, integ_time, **opts):

        self.do_scan = True

        # save the user parameters
        self.mode = mode
        self.motors = []
        self.start_pos = []
        self.final_pos = []
        self.space = space

        # first we parse all motors and check whether we have VMExecutors
        for mot, start, final in zip(motor, start_pos, final_pos):
            motor, start_pos, final_pos = self._parse_motors(mot, start, final)
            self.motors += motor
            self.start_pos += start_pos
            self.final_pos += final_pos

        self.nsteps = nb_steps
        self.integ_time = integ_time

        self._step_info = {"start_positions": self.start_pos, "positions": self.final_pos,
               "integ_time": self.integ_time, "npts": self.nsteps}

        # should we print debug info
        try:
            self.debug_mode = self.getEnv('cscan_debug')
            if self.debug_mode:
                self._debug_file_name = os.path.join(self.getEnv('ScanDir'),
                                                     'debug_log_' + str(self.getEnv('ScanID') + 1) + '.log')
                self.warning('ATTENTION! cscan_debug set to True!')
                self.info('The debug info will be printed to {}'.format(self._debug_file_name))
        except Exception as err:
            self.debug_mode = False

        # do we need to syncro motors
        try:
            self.pilc_mode = self.getEnv('cscan_pilc')
            self.report_debug('pilc_mode: {}'.format(self.pilc_mode))
        except Exception as err:
            self.report_debug('Cannot get cscan_pilc, pilc_mode set to False')
            self.pilc_mode = False

        if self.integ_time < 0.02 and not self.pilc_mode:
            if not self.ask_user("The {} integration time is too short for continuous scans. Recommended > 0.02 sec. Continue?".format(self.integ_time)):
                self.do_scan = False
                return

        # do we need to syncro motors
        try:
            self._sync = self.getEnv('cscan_sync')
        except Exception as err:
            self._sync = True

        self.report_debug('SYNC mode {}'.format(self._sync))

        if not self._sync and len(self.motors) > 1:
            if not self.ask_user("The sync mode is off, the positions of motors will not be syncronized. Continue?"):
                self.do_scan = False
                return

        # timing log
        try:
            self.timeme = self.getEnv('cscan_timeme')
        except Exception as err:
            self.timeme = False

        # should we monitor movement
        try:
            self.motion_monitor = self.getEnv('cscan_monitor')
            if self.motion_monitor:
                self._motion_monitor_name = os.path.join(self.getEnv('ScanDir'),
                                                     'motion_log_' + str(self.getEnv('ScanID') + 1) + '.log')
                self.info('The motion monitor info will be printed to {}'.format(self._motion_monitor_name))
        except Exception as err:
            self.motion_monitor = False

        # this is a peace of code from "standart" scans
        moveables = []
        for m, start, final in zip(self.motors, self.start_pos, self.final_pos):
            moveables.append(MoveableDesc(moveable=m, min_value=min(
                start, final), max_value=max(start, final)))
        moveables[0].is_reference = True

        env = opts.get('env', {})
        constrains = [getCallable(cns) for cns in opts.get('constrains', [UNCONSTRAINED])]
        extrainfodesc = opts.get('extrainfodesc', [])

        # create an instance of CScan or HklScac
        if self.space == 'real':
            self._gScan = CCScan(self,
                                 waypointGenerator=self._waypoint_generator,
                                 periodGenerator=self._period_generator,
                                 moveables=moveables, env=env,
                                 constraints=constrains, extrainfodesc=extrainfodesc)

        elif self.space == 'reciprocal':
            for moveable in moveables:
                moveable.moveable.elements = moveable.moveable.getPoolData()['elements']

            self._gScan = HklCScan(self,
                                   waypointGenerator=self._waypoint_generator,
                                   periodGenerator=self._period_generator,
                                   moveables=moveables, env=env,
                                   constraints=constrains, extrainfodesc=extrainfodesc,
                                   constants=opts.get('constants'), motors_settings=opts.get('motors_settings'))
        else:
            raise RuntimeError('Unknown space {}!'.format(space))

    # ----------------------------------------------------------------------
    def _waypoint_generator(self):
        # returns start and stop points
        yield self._step_info

    # ----------------------------------------------------------------------
    def _period_generator(self):
        # infinite generator. The acquisition loop is started/stopped at begin and end of each waypoint
        step = {}
        step["integ_time"] = self.integ_time
        point_no = 0
        while (True):
            point_no += 1
            step["point_id"] = point_no
            yield step

    # ----------------------------------------------------------------------
    def _parse_motors(self, motor, start_pos, end_pos):

    ### This cscans check whether the motors are VmMotors.
    # In that case we need to split it to real motors and recalculate the start and stop positions for each motor

        try:
            _motor_proxy = PyTango.DeviceProxy(PyTango.DeviceProxy(motor.getName()).TangoDevice)
            if _motor_proxy.info().dev_class in ['VmExecutor', ]:
                try:
                    self.output("VM {} found".format(motor.getName()))

                    sub_devices = _motor_proxy.get_property('__SubDevices')['__SubDevices']
                    tango_motors = [device.replace('hasep23oh:10000/', '') for device in sub_devices if 'vmexecutor' not in device]

                    channel_names = [device.split('/')[-1].replace('.', '/') for device in tango_motors]

                    self.report_debug('tango_motors {}'.format(tango_motors))
                    self.report_debug('channel_names {}'.format(channel_names))

                    _, real_start, real_finish = self._parse_dscan_pos(motor, start_pos, end_pos)

                    _motor_proxy.PositionSim = real_start[0]
                    _sim_start_pos = _motor_proxy.ResultSim

                    _motor_proxy.PositionSim = real_finish[0]
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
                    raise RuntimeError('Cannot parse {} to components, the cscans cannot be executed'.format(motor.getName()))
            else:
                return self._parse_dscan_pos(motor, start_pos, end_pos)
        except AttributeError:
            return self._parse_dscan_pos(motor, start_pos, end_pos)

    # ----------------------------------------------------------------------
    def _parse_dscan_pos(self, motor, start, stop):
        # if user ask for dscan here we convert the position to the absolute for given motor

        original_position = self.getMotion([motor.getName()]).readPosition(force=True)
        if self.mode == 'dscan':
            return [motor], [start + original_position[0]], [stop + original_position[0]]
        else:
            return [motor], [start], [stop]

    # ----------------------------------------------------------------------
    def _get_command(self, command):
        # this function is needed to deceive Sardana and all other software that we are "regular" scan

        if len(self.motors) > 1:
            command += '{}scan'.format(len(self.motors))
        else:
            command += 'scan'

        for motor, start_pos, final_pos in zip(self.motors, self.start_pos, self.final_pos):
            command += ' ' + ' '.join([motor.getName(), '{:.4f} {:.4f}'.format(float(start_pos), float(final_pos))])
        command += ' {} {}'.format(self.nsteps, self.integ_time)
        return command

    # ----------------------------------------------------------------------
    def report_debug(self, msg):
        # general function that prints debug information
        self.debug(msg)
        if self.debug_mode:
            with open(self._debug_file_name, 'a') as f:
                f.write(str(msg) + '\n')

    # ----------------------------------------------------------------------
    def ask_user(self, msg):

        options = YES_OPTIONS + NO_OPTIONS
        return self.input(msg, data_type=options, allow_multiple=False, title="Favorites", default_value='No') in YES_OPTIONS

# ----------------------------------------------------------------------
class aNcscan(iMacro, CscanClass):

    def run(self, *args):
        self._gScan.calculate_speed(self._step_info)

        if self.do_scan:
            for step in self._gScan.step_scan():
                yield step

    def getCommand(self):
        return self._get_command('a')

    # ----------------------------------------------------------------------
    def on_abort(self):
        self.warning('GOT ON_STOP REQUEST')
        self._gScan.do_restore()
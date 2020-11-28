'''
This is main class for continuous scans.

Author yury.matveev@desy.de
'''

# general python imports
import PyTango
from imp import reload

# Sardana imports

from sardana.macroserver.macro import Macro, Hookable
from sardana.macroserver.macros.scan import getCallable, UNCONSTRAINED
from sardana.macroserver.scan.scandata import MoveableDesc
from sardana.taurus.core.tango.sardana.pool import StopException

# cscan imports
from cscan_ccscan import CCScan
from cscan_hklcscan import HklCScan
from cscan_constants import *

# ----------------------------------------------------------------------
#
# ----------------------------------------------------------------------

class scancl(Hookable):

    def _prepare(self, mode, space, motor, start_pos, final_pos, nb_steps, integ_time, **opts):

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
            self.timeme = self.getEnv('cscan_timeme')
        except Exception as err:
            self.timeme = False

        try:
            self.debug_mode = self.getEnv('cscan_debug')
            if self.debug_mode:
                self.output('ATTENTION! cscan_debug set to True!!! If you are not debuging now it is recommended to set it to False!!!!!')
        except Exception as err:
            self.debug_mode = False

        if self.debug_mode:
            self.debug('SYNC mode {}'.format(self._sync))

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

        self.sources = []
        for element in self.getMeasurementGroup(self.getEnv('ActiveMntGrp')).physical_elements:
            self.sources.append(self.getExpChannel(element))

        self.num_sources = len(self.sources)

        moveables = []
        for m, start, final in zip(self.motors, self.start_pos, self.final_pos):
            moveables.append(MoveableDesc(moveable=m, min_value=min(
                start, final), max_value=max(start, final)))
        moveables[0].is_reference = True

        env = opts.get('env', {})
        constrains = [getCallable(cns) for cns in opts.get('constrains', [UNCONSTRAINED])]
        extrainfodesc = opts.get('extrainfodesc', [])

        # create an instance of CScan
        if space == 'real':
            self._gScan = CCScan(self,
                                 waypointGenerator=self._waypoint_generator,
                                 periodGenerator=self._period_generator,
                                 moveables=moveables, env=env,
                                 constraints=constrains, extrainfodesc=extrainfodesc)

        elif space == 'reciprocal':
            for moveable in moveables:
                moveable.moveable.elements = moveable.moveable.getPoolData()['elements']

            self._gScan = HklCScan(self,
                                   waypointGenerator=self._waypoint_generator,
                                   periodGenerator=self._period_generator,
                                   moveables=moveables, env=env,
                                   constraints=constrains, extrainfodesc=extrainfodesc)
        else:
            raise RuntimeError('Unknown space {}!'.format(space))

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
            # for motor, position in zip(self._gScan._physical_moveables, self.original_positions):
            #     motor.Position = position
            self._gScan._physical_motion.move(self.original_positions)

    # ----------------------------------------------------------------------
    def _parse_motors(self, motor, start_pos, end_pos):
        try:
            _motor_proxy = PyTango.DeviceProxy(PyTango.DeviceProxy(motor.getName()).TangoDevice)
            if _motor_proxy.info().dev_class in ['VmExecutor', ]:
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

                    if self.debug_mode:
                        self.debug('tango_motors {}'.format(tango_motors))
                        self.debug('channel_names {}'.format(channel_names))

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
class aNcscan(Macro, scancl):

    def run(self, *args):
        if self.do_scan:
            for step in self._gScan.step_scan():
                yield step

    def getCommand(self):
        return self._get_command('a')
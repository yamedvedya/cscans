'''
The purpose of these scripts is perform the continuous scans with minimum measurement overhead.
Synchronization is achieved by a software.

Author yury.matveev@desy.de
'''


__all__ = ['dcscan', 'acscan', 'd2cscan', 'a2cscan','cscan_senv']

# general python imports
from imp import reload
import numpy as np

# Sardana imports
from sardana.macroserver.macro import Macro, Type
from sardana.macroserver.macros.hkl import _diffrac

# cscan imports, always reloaded to track changes; ORDER IS IMPORTANT!!!!
import cscans.cscan_constants; reload(cscans.cscan_constants)
import cscans.cscan_axillary_functions; reload(cscans.cscan_axillary_functions)
import cscans.cscan_data_workers; reload(cscans.cscan_data_workers)
import cscans.cscan_data_collector; reload(cscans.cscan_data_collector)
import cscans.cscan_ccscan; reload(cscans.cscan_ccscan)
import cscans.cscan_hklcscan; reload(cscans.cscan_hklcscan)
import cscans.cscan_scancl; reload(cscans.cscan_scancl)
# import cscans.cscan_movement_monitor; reload(cscans.cscan_movement_monitor)

from cscans.cscan_scancl import aNcscan
from cscans.cscan_constants import *

# ----------------------------------------------------------------------
#       "Normal" a- and d- scans
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
class acscan(aNcscan):

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

        self._prepare('ascan', 'real', [motor], np.array([start_pos], dtype='d'), np.array([final_pos], dtype='d'),
                      nb_steps, integ_time, **opts)

# ----------------------------------------------------------------------
class dcscan(aNcscan):

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

        self._prepare('dscan', 'real', [motor], np.array([start_pos], dtype='d'), np.array([final_pos], dtype='d'),
                      nb_steps, integ_time, **opts)


# ----------------------------------------------------------------------
class a2cscan(aNcscan):
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

        self._prepare('ascan', 'real', [motor1, motor2], np.array([start_pos1, start_pos2], dtype='d'),
                      np.array([final_pos1, final_pos2], dtype='d'),
                      nb_steps, integ_time, **opts)

# ----------------------------------------------------------------------
class d2cscan(aNcscan):
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

        self._prepare('dscan', 'real', [motor1, motor2], np.array([start_pos1, start_pos2], dtype='d'),
                      np.array([final_pos1, final_pos2], dtype='d'),
                      nb_steps, integ_time, **opts)

# ----------------------------------------------------------------------
#       Reciprocal a- and d- scans
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
class hcscan(aNcscan, _diffrac):

    """ Performs a continuous scan taking current Active Measurement Group
        In case if the Lambda configured - set it to

    """

    # this is used to indicate other codes that the macro is a scan
    hints = {'scan': 'hscan', 'allowsHooks': ('pre-scan', 'pre-move',
                                               'post-move', 'pre-acq',
                                               'post-acq',
                                               'post-scan')}

    env = ['ActiveMntGrp']

    param_def = [
        ['start_pos',       Type.Float,     None,   'Scan start position'],
        ['final_pos',       Type.Float,     None,   'Scan final position'],
        ['nb_steps',        Type.Integer,   None,   'Nb of steps'],
        ['integ_time',      Type.Float,     None,   'Integration time'],
    ]


    def prepare(self, start_pos, final_pos, nb_steps, integ_time, **opts):

        self.name = 'hscan'
        _diffrac.prepare(self)

        self._prepare('ascan', 'reciprocal', [self.h_device], np.array([start_pos], dtype='d'),
                      np.array([final_pos], dtype='d'), nb_steps, integ_time, **opts)


# ----------------------------------------------------------------------
#       Time scan
# ----------------------------------------------------------------------
class ctscan(aNcscan):
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

        self._prepare('ascan', 'real', [motor], np.array([0], dtype='d'),
                      np.array([total_time], dtype='d'), int(total_time/integ_time) + 1, integ_time, **opts)

    def getCommand(self):
        return 'ascan {} 0 {} {} {}'.format(DUMMY_MOTOR, self.final_pos[0], self.nsteps, self.integ_time)


# ----------------------------------------------------------------------
#                       Auxiliary class to set environment
# ----------------------------------------------------------------------

class cscan_senv(Macro):
    """ Sets default environment variables """

    def run(self):
        self.setEnv("LambdaDevice", "p23/testlambda/testlambda")
        self.setEnv("LambdaOnlineAnalysis", "p23/testlambda/testlambda")
        self.setEnv("AttenuatorProxy", "p23/testlambda/testlambda")
        self.setEnv("cscan_sync", True)
        self.setEnv("cscan_timeme", False)
        self.setEnv("cscan_debug", False)


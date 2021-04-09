'''
The purpose of these scripts is perform the continuous scans with minimum measurement overhead.
Synchronization is achieved by a software.

Author yury.matveev@desy.de
'''


__all__ = ['dcscan', 'acscan', 'd2cscan', 'a2cscan',
           'hcscan', 'hdcscan', 'kcscan', 'kdcscan',
           'lcscan', 'ldcscan', 'hklcscan', 'hkldcscan',
           'ctscan','cscan_senv']

# general python imports
import sys
if sys.version_info.major >= 3:
    from importlib import reload
else:
    from imp import reload

import PyTango
import numpy as np
import os
import sys

sys.path.append(os.path.dirname(__file__))

# Sardana imports
from sardana.macroserver.macro import Macro, Type
from sardana.macroserver.macros.hkl import _diffrac

# cscans imports, always reloaded to track changes; ORDER IS IMPORTANT!!!!
import cs_scancl
import cs_pilc_workers
import cs_axillary_functions
import cs_data_workers
import cs_data_collector
import cs_hklcscan
import cs_movement
import cs_ccscan
import cs_constants

reload(cs_constants)
reload(cs_axillary_functions)
reload(cs_movement)
reload(cs_pilc_workers)
reload(cs_data_workers)
reload(cs_data_collector)
reload(cs_ccscan)
reload(cs_hklcscan)
reload(cs_scancl)

from cs_scancl import aNcscan
from cs_constants import *

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

        _reload()

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

        _reload()

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

        _reload()

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

        _reload()

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

        _reload()

        self.name = 'hscan'

        _diffrac.prepare(self)

        self._prepare('ascan', 'reciprocal', [self.h_device], np.array([start_pos], dtype='d'),
                      np.array([final_pos], dtype='d'), nb_steps, integ_time, **opts)

# ----------------------------------------------------------------------
class hdcscan(aNcscan, _diffrac):

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

        _reload()

        self.name = 'hscan'

        _diffrac.prepare(self)

        self._prepare('dscan', 'reciprocal', [self.h_device], np.array([start_pos], dtype='d'),
                      np.array([final_pos], dtype='d'), nb_steps, integ_time, **opts)

# ----------------------------------------------------------------------
class lcscan(aNcscan, _diffrac):

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

        _reload()

        self.name = 'lscan'

        _diffrac.prepare(self)

        self._prepare('ascan', 'reciprocal', [self.l_device], np.array([start_pos], dtype='d'),
                      np.array([final_pos], dtype='d'), nb_steps, integ_time, **opts)

# ----------------------------------------------------------------------
class ldcscan(aNcscan, _diffrac):

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

        _reload()

        self.name = 'lscan'

        _diffrac.prepare(self)

        self._prepare('dscan', 'reciprocal', [self.l_device], np.array([start_pos], dtype='d'),
                      np.array([final_pos], dtype='d'), nb_steps, integ_time, **opts)

# ----------------------------------------------------------------------
class kcscan(aNcscan, _diffrac):

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

        self.name = 'kscan'
        _diffrac.prepare(self)

        self._prepare('ascan', 'reciprocal', [self.k_device], np.array([start_pos], dtype='d'),
                      np.array([final_pos], dtype='d'), nb_steps, integ_time, **opts)

# ----------------------------------------------------------------------
class kdcscan(aNcscan, _diffrac):

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

        _reload()

        self.name = 'kscan'

        _diffrac.prepare(self)

        self._prepare('dscan', 'reciprocal', [self.k_device], np.array([start_pos], dtype='d'),
                      np.array([final_pos], dtype='d'), nb_steps, integ_time, **opts)

# ----------------------------------------------------------------------
class hklcscan(aNcscan, _diffrac):

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
        ['h_start_pos',     Type.Float,     None,   'H scan start position'],
        ['h_stop_pos',      Type.Float,     None,   'H scan stop position'],
        ['k_start_pos',     Type.Float,     None,   'K scan start position'],
        ['k_stop_pos',      Type.Float,     None,   'K scan stop position'],
        ['l_start_pos',     Type.Float,     None,   'L scan start position'],
        ['l_stop_pos',      Type.Float,     None,   'L scan stop position'],
        ['nb_steps',        Type.Integer,   None,   'Nb of steps'],
        ['integ_time',      Type.Float,     None,   'Integration time'],
    ]


    def prepare(self, h_start_pos, h_stop_pos, k_start_pos, k_stop_pos,
                l_start_pos, l_stop_pos, nb_steps, integ_time, **opts):

        _reload()

        self.name = 'hklscan'

        _diffrac.prepare(self)

        self._prepare('ascan', 'reciprocal', [self.h_device, self.k_device, self.l_device],
                      np.array([h_start_pos, k_start_pos, l_start_pos], dtype='d'),
                      np.array([h_stop_pos, k_stop_pos, l_stop_pos], dtype='d'), nb_steps, integ_time, **opts)

# ----------------------------------------------------------------------
class hkldcscan(aNcscan, _diffrac):

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
        ['h_start_pos',     Type.Float,     None,   'H scan start position'],
        ['h_stop_pos',      Type.Float,     None,   'H scan stop position'],
        ['k_start_pos',     Type.Float,     None,   'K scan start position'],
        ['k_stop_pos',      Type.Float,     None,   'K scan stop position'],
        ['l_start_pos',     Type.Float,     None,   'L scan start position'],
        ['l_stop_pos',      Type.Float,     None,   'L scan stop position'],
        ['nb_steps',        Type.Integer,   None,   'Nb of steps'],
        ['integ_time',      Type.Float,     None,   'Integration time'],
    ]


    def prepare(self, h_start_pos, h_stop_pos, k_start_pos, k_stop_pos,
                l_start_pos, l_stop_pos, nb_steps, integ_time, **opts):

        _reload()

        self.name = 'hklscan'

        _diffrac.prepare(self)

        self._prepare('dscan', 'reciprocal', [self.h_device, self.k_device, self.l_device],
                      np.array([h_start_pos, k_start_pos, l_start_pos], dtype='d'),
                      np.array([h_stop_pos, k_stop_pos, l_stop_pos], dtype='d'), nb_steps, integ_time, **opts)

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

        _reload()

        self.name = 'ctscan'

        motor = self.getMotor(DUMMY_MOTOR)
        motor.Acceleration = 0
        motor.Deceleration = 0

        self._steps = int(total_time/integ_time)

        self._prepare('tscan', 'real', [motor], np.array([0], dtype='d'),
                      np.array([total_time], dtype='d'), int(total_time/integ_time) + 1, integ_time, **opts)

    def getCommand(self):
        return 'ascan {} 0 {} {} {}'.format(DUMMY_MOTOR, self.final_pos[0], self.nsteps, self.integ_time)


# ----------------------------------------------------------------------
#                       Auxiliary class to set environment
# ----------------------------------------------------------------------

class cscan_senv(Macro):
    """ Sets default environment variables """

    def run(self):
        self.setEnv("LambdaDevice", "p23/lambdactrl/01")
        self.setEnv("LambdaOnlineAnalysis", "p23/lambdaonlineanalysis/oh.01")
        self.setEnv("AttenuatorProxy", "p23/vmexecutor/attenuatorposition")
        self.setEnv("cscan_sync", True)
        self.setEnv("cscan_timeme", False)
        self.setEnv("cscan_debug", False)
        self.setEnv("cscan_monitor", False)


class cscan_set_pilc(Macro):
    """ Sets default environment variables """

    def run(self):
        PyTango.DeviceProxy('p23/pilc/exp.01').WriteIOCard([0x03, 0x01, 0x03])
        PyTango.DeviceProxy('p23/pilc/exp.03').WriteIOCard([0x01, 0x01, 0x03])
        PyTango.DeviceProxy('p23/pilc/exp.03').WriteIOCard([0x05, 0x01, 0x03])
        PyTango.DeviceProxy('p23/pilc/exp.05').WriteIOCard([0x03, 0x01, 0x03])

# ----------------------------------------------------------------------
#                       Auxiliary function
# ----------------------------------------------------------------------

def _reload():
    reload(cs_constants)
    reload(cs_axillary_functions)
    reload(cs_movement)
    reload(cs_pilc_workers)
    reload(cs_data_workers)
    reload(cs_data_collector)
    reload(cs_ccscan)
    reload(cs_hklcscan)
    reload(cs_scancl)

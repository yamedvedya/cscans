#!/usr/bin/env python

# ----------------------------------------------------------------------
# Author:        yury.matveev@desy.de
# ----------------------------------------------------------------------

#Note: this script uses ScanDir and ScanFile from the MacroServer

from sardana.macroserver.macro import *

import sys; sys.path.append(r"/home/p23user/sardanaMacros/")
from agilent_libs.agilent_backend import SMU
import agilent_libs.agilent_language as smu_lang
import agilent_libs.waveform_generator as wg


HOST = "192.168.132.44"
PORT = 5025
COMPLIANCE = 1e-6

ENABLED_CHANNELS = [1, 2]
ENABLED_PLOTS = ('IT', 'VT', 'IV')

__all__ = ['reset_agilent', 'run_agilent', 'read_data']

# ----------------------------------------------------------------------
#                       This macro resets SMU
# ----------------------------------------------------------------------
class reset_agilent(Macro):
    def run(self):
        smu_lang.SMU = SMU(HOST, PORT)
        smu_lang.reset_SMU()

# ----------------------------------------------------------------------
#                       This macro runs SMU
# ----------------------------------------------------------------------
class run_agilent(Macro):

    param_def = [["pulse_voltage",  Type.Float,     None,   'Voltage value at pulse plato'],
                 ["pulse_rise",     Type.Float,     None,   'Pulse rise and descend time'],
                 ["pulse_hold",     Type.Float,     None,   'Pulse hold time'],
                 ["hold_voltage",   Type.Float,     None,   'Hold voltage'],
                 ["sampling",       Type.Float,     None,   'Sampling period'],
                 ]

    def prepare(self, pulse_voltage, pulse_rise, pulse_hold, hold_voltage, sampling):

        self.smu = SMU(HOST, PORT)
        self.smu.check_for_errors()

        self.smu_lang.SMU = self.smu
        self.smu_lang.clear_SMU()
        self.smu_lang.init_traces()

        # sampling period, sec
        fs = sampling

        wf = wg.init_wf(fs)
        # wavefore, len(wavefore) = gen_pulse(wf, rise time, hold time, max voltage, base voltage)
        wf, len_wf = wg.gen_pulse(wf, 10*fs, fs, 0.4, 0)

        # wavefore, len(wavefore) = gen_space(wf, hold time , base voltage)
        wf, len_wf = wg.gen_space(wf, fs, 0)

        for channel in ENABLED_CHANNELS:
            self.smu_lang.set_compliance(channel, COMPLIANCE)
            # smu_lang.set_range(1, compliance)
            self.smu_lang.set_auto_range(channel)

        self.smu_lang.set_channel_list_sweep(ENABLED_CHANNELS[0], wf["vseries"], fs)
        if len(ENABLED_CHANNELS) > 1:
            self.smu_lang.set_channel_spot(ENABLED_CHANNELS[1], 0, len_wf, fs)

    def run(self, *args):
        self.smu_lang.force_frigger(ENABLED_CHANNELS)

# ----------------------------------------------------------------------
#                       This macro gets data from SMU and saves it
# ----------------------------------------------------------------------

class read_data(Macro):
    env = ('ScanDir', 'ScanFile')

    def run(self):
        smu = SMU(HOST, PORT)
        smu_lang.SMU = smu

        smu.wait_till_complete(15)
        codes, descriptions = smu.check_for_errors()
        if codes[0] != 0:
            for code, desc in zip(codes, descriptions):
                self.output('Got an error {}: {}'.format(code, desc))
        else:
            res = smu_lang.get_traces([1, 2])
            smu_lang.save_smu_data(res, self.getEnv('ScanFile'), self.getEnv('ScanDir'))
            smu_lang.plot_smu_data(res, channels=ENABLED_CHANNELS, plots=ENABLED_PLOTS, macro_handle=self)
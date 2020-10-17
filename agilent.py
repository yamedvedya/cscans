# ----------------------------------------------------------------------
# Author:        yury.matveev@desy.de
# ----------------------------------------------------------------------

#Note: this script uses ScanDir and ScanFile from the MacroServer

from sardana.macroserver.macro import *


import sys
sys.path.append(r"/home/p23user/sardanaMacros/")

from agilent_libs.agilent_backend import SMU
import agilent_libs.agilent_language as smu_lang
import agilent_libs.waveform_generator as wg
from agilent_libs import sweep
import numpy as np
import os

#from imp import reload
#reload(agilent_libs.agilent_language)
#reload(smu_lang)


HOST = "192.168.132.44"
PORT = 5025

ENABLED_CHANNELS = [1,2]
ENABLED_PLOTS = ('IT', 'VT', 'IV')

__all__ = ['reset_agilent', 'run_agilent', 'read_agilent']

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
    param_def = [
                 ["sampling",           Type.Float,     None,           'smallest interval in wafe function'],
                 ["compliance",         Type.Float,     None,           'maximum read out current'],
                 ["hold_voltage",       Type.Float,     None,           'final voltage'],
                 ["hold_time",          Type.Float,     None,           'dwell time at target voltage'],
                 ["pulse_voltage",      Type.Float,     np.nan,         'pulse voltage'],
                 ["pulse_rise",         Type.Float,     np.nan,         'pulse rise time'],
                 ]


    def prepare(self, sampling, compliance, hold_voltage, hold_time, pulse_voltage, pulse_rise):
        if smu_lang.SMU is None:
            self.smu = SMU(HOST, PORT)
            self.smu.check_for_errors()

            smu_lang.SMU = self.smu
        else:
            self.smu = smu_lang.SMU

        smu_lang.reset_SMU()
        smu_lang.abort_SMU()
        smu_lang.clear_SMU()
        smu_lang.init_traces() # only for the first time ??

        # sampling period, sec
        wf = wg.init_wf(sampling)


        if pulse_voltage==np.nan:
            # wavefore, len(wavefore) = gen_pulse(wf, rise time, hold time, max voltage, base voltage)
            wf, len_wf = wg.gen_pulse(wf, pulse_rise, hold_time, pulse_voltage, hold_voltage)
            self.output("Applying pulse...")
        else:
            # wavefore, len(wavefore) = gen_space(wf, hold time , base voltage)
            wf, len_wf = wg.gen_space(wf, hold_time, hold_voltage)
            self.output("Probing...")

        for channel in ENABLED_CHANNELS:
            smu_lang.set_compliance(channel, compliance)
            # smu_lang.set_range(1, compliance)
            smu_lang.set_auto_range(channel)

        smu_lang.set_channel_list_sweep(ENABLED_CHANNELS[0], wf["vseries"], sampling)
        if len(ENABLED_CHANNELS) > 1:
            smu_lang.set_channel_spot(ENABLED_CHANNELS[1], 0, len_wf, sampling)

    def run(self, *args):
        smu_lang.force_frigger(ENABLED_CHANNELS)

# ----------------------------------------------------------------------
#                       This macro gets data from SMU and saves it
# ----------------------------------------------------------------------

class read_agilent(Macro):
    env = ('ScanDir', 'ScanFile')
    param_def = [
                 ["wait_time",       Type.Float,     10.,   'Wait time for result'],
                 ]

    def run(self, wait_time):
        if smu_lang.SMU is None:
            self.output("init SMU...")
            smu = SMU(HOST, PORT)
            smu_lang.SMU = smu
        else:
            smu = smu_lang.SMU

        smu.wait_till_complete(wait_time)
        codes, descriptions = smu.check_for_errors()
        if codes[0] != 0:
            for code, desc in zip(codes, descriptions):
                self.output('Got an error {}: {}'.format(code, desc))
        else:
            res = smu_lang.get_traces(ENABLED_CHANNELS)
            scanname = "%s_%05i"%(os.path.splitext(self.getEnv('ScanFile')[0])[0], self.getEnv('ScanID'))
            smu_lang.save_smu_data_spock(res, self.getEnv('ScanDir'), scanname, self.getEnv('Meas_point_index_number'))
            #smu_lang.plot_smu_data(res, channels=ENABLED_CHANNELS, plots=ENABLED_PLOTS, macro_handle=self)



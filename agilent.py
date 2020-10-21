# ----------------------------------------------------------------------
# Author:        yury.matveev@desy.de
# ----------------------------------------------------------------------

#Note: this script uses ScanDir and ScanFile from the MacroServer

from sardana.macroserver.macro import *


import sys
sys.path.append(r"/home/p23user/sardanaMacros/")


#from agilent_libs import agilent_backend
from agilent_libs.agilent_backend import SMU
import agilent_libs.agilent_language as smu_lang
import agilent_libs.waveform_generator as wg
from agilent_libs import sweep

import numpy as np
import os
import time
from imp import reload

##reload(agilent_libs.agilent_language)
reload(smu_lang)
reload(sweep)
#reload(agilent_backend)


HOST = "192.168.132.44"
PORT = 5025

ENABLED_CHANNELS = [1,2]
ENABLED_PLOTS = ('IT', 'VT', 'IV')

__all__ = ['agilent_reset', 'agilent_run', 'agilent_read', 'agilent_sweep_pulsed']

# ----------------------------------------------------------------------
#                       This macro resets SMU
# ----------------------------------------------------------------------
class agilent_reset(Macro):
    def run(self):
        smu_lang.SMU = SMU(HOST, PORT)
        smu_lang.reset_SMU()
        smu_lang.init_traces() # only for the first time ??
# ----------------------------------------------------------------------
#                       This macro runs SMU
# ----------------------------------------------------------------------


class agilent_run(Macro):
    param_def = [
                 ["sampling",           Type.Float,     None,           'smallest time interval in wafe function'],
                 ["compliance",         Type.Float,     None,           'maximum read out current'],
                 ["hold_voltage",       Type.Float,     None,           'final voltage'],
                 ["hold_time",          Type.Float,     None,           'dwell time at target voltage'],
                 ["pulse_voltage",      Type.Float,     np.nan,         'pulse voltage'],
                 ["pulse_rise",         Type.Float,     np.nan,         'pulse rise time'],
                 ]


    def prepare(self, sampling, compliance, hold_voltage, hold_time, pulse_voltage, pulse_rise):
        if smu_lang.SMU is None:
            self.output("connection to agilent...")
            self.smu = SMU(HOST, PORT)
            self.smu.check_for_errors()

            smu_lang.SMU = self.smu
        else:
            self.smu = smu_lang.SMU

        smu_lang.abort_SMU()
        smu_lang.clear_SMU()


        # sampling period, sec
        wf = wg.init_wf(sampling)

        if np.isnan((pulse_voltage, pulse_rise)).any():
            # wavefore, len(wavefore) = gen_space(wf, hold time , base voltage)
            wf, len_wf = wg.gen_space(wf, hold_time, hold_voltage)
            self.output("Probing...")
        else:
            # wavefore, len(wavefore) = gen_pulse(wf, rise time, hold time, max voltage, base voltage)
            wf, len_wf = wg.gen_pulse(wf, pulse_rise, hold_time, pulse_voltage, hold_voltage)
            self.output("Applying pulse...")

        for channel in ENABLED_CHANNELS:
            smu_lang.set_compliance(channel, compliance)
            smu_lang.set_range(1, compliance)
            #smu_lang.set_auto_range(channel)

        smu_lang.set_channel_list_sweep(ENABLED_CHANNELS[0], wf["vseries"], sampling)
        if len(ENABLED_CHANNELS) > 1:
            smu_lang.set_channel_spot(ENABLED_CHANNELS[1], 0, len_wf, sampling)

    def run(self, *args):
        smu_lang.force_frigger(ENABLED_CHANNELS)








class agilent_sweep(Macro):
    param_def = [
                 ["sampling",           Type.Float,     None,           'smallest time interval in wafe function'],
                 ["compliance",         Type.Float,     None,           'maximum read out current'],
                 ["voltage1",           Type.Float,     None,           'first peak voltage'],
                 ["voltage2",           Type.Float,     None,           'second peak voltage'],
                 ["tpoints",            Type.Float,     None,           'points per loop'],
                 ["iterations",         Type.Integer,   1,              'number of loops'],
                 ]


    tdead = 1
    def prepare(self, sampling, compliance, voltage1, voltage2, tpoints, iterations):

        sweep_par = dict(
            voltage1=voltage1,
            voltage2=voltage2,
            points_per_sweep=tpoints,
            start_at_zero=True,
            stop_at_zero=True,
            iterations=iterations
        )

        if smu_lang.SMU is None:
            self.output("connection to agilent...")
            self.smu = SMU(HOST, PORT)
            self.smu.check_for_errors()

            smu_lang.SMU = self.smu
        else:
            self.smu = smu_lang.SMU

        smu_lang.abort_SMU()
        smu_lang.clear_SMU()


        # sampling period, sec
        wf = wg.init_wf(sampling)

        wf, len_wf = sweep.static_scan(wf, **sweep_par)
        est_measure_time = len_wf * sampling
        self.output("ETA sweep: %.1f"%est_measure_time)


        for channel in ENABLED_CHANNELS:
            smu_lang.set_compliance(channel, compliance)
            smu_lang.set_range(1, compliance)
            #smu_lang.set_auto_range(channel)

        smu_lang.set_channel_list_sweep(ENABLED_CHANNELS[0], wf["vseries"], sampling)
        if len(ENABLED_CHANNELS) > 1:
            smu_lang.set_channel_spot(ENABLED_CHANNELS[1], 0, len_wf, sampling)

    def run(self, *args):
        smu_lang.force_frigger(ENABLED_CHANNELS)








# ----------------------------------------------------------------------
#                       This macro gets data from SMU and saves it
# ----------------------------------------------------------------------

class agilent_read(Macro):
    env = ('ScanDir', 'ScanFile')
    param_def = [
                 ["wait_time",       Type.Float,     10.,   'Wait time for result'],
                 ]

    def run(self, wait_time):
        if smu_lang.SMU is None:
            self.output("connection to agilent...")
            smu = SMU(HOST, PORT)
            smu_lang.SMU = smu
        else:
            smu = smu_lang.SMU

        T0 = time.time()
        smu.wait_till_complete(wait_time)
        #self.output("Time needed: %f"%(time.time() - T0))

        try:
            #res = smu_lang.get_traces(ENABLED_CHANNELS)
            res = smu_lang.get_traces([1])
            scanname = "%s_%05i"%(os.path.splitext(self.getEnv('ScanFile')[0])[0], self.getEnv('ScanID'))
            smu_lang.save_smu_data_spock(res, self.getEnv('ScanDir'), scanname, self.getEnv('Meas_point_index_number'))
        except Exception as emsg:
            codes, descriptions = smu.check_for_errors()
            if codes[0] != 0:
                for code, desc in zip(codes, descriptions):
                    self.output('Got an error {}: {}'.format(code, desc))
            self.output('Err-Msg: %s'%emsg)
            #raise



class agilent_sweep_pulsed(Macro):
    sampling = 0.01

    compliance_pulse = 5e-2
    compliance_LRS = 2e-5
    compliance_HRS = 2e-6

    vprobe = 0.2
    vpulse_down = -5.8
    vpulse_up = 1.7

    tprobe = 1
    tdead = 0.5

    def read(self, waittime):
        self.execMacro("agilent_read %f"%(waittime+self.tdead))

    def run(self):

        profile_HRS = (self.sampling, self.compliance_HRS, self.vprobe, self.tprobe)
        profile_LRS = (self.sampling, self.compliance_LRS, self.vprobe, self.tprobe)
        profile_up =    (self.sampling, self.compliance_pulse, self.vprobe, self.sampling, self.vpulse_up,   self.sampling)
        profile_down =  (self.sampling, self.compliance_pulse, self.vprobe, self.sampling, self.vpulse_down, self.sampling)

        cmd_HRS = "agilent_run %f %f %f %f"%profile_HRS
        cmd_LRS = "agilent_run %f %f %f %f"%profile_LRS
        cmd_up = "agilent_run %f %f %f %f %f %f"%profile_up
        cmd_down = "agilent_run %f %f %f %f %f %f"%profile_down


        self.execMacro(cmd_LRS)
        self.read(self.tprobe)

        for i in range(1):
            self.execMacro(cmd_down)
            self.read(self.sampling*1)
            self.execMacro(cmd_HRS)
            self.read(self.tprobe)

            self.execMacro(cmd_up)
            self.read(self.sampling*1)
            self.execMacro(cmd_LRS)
            self.read(self.tprobe)











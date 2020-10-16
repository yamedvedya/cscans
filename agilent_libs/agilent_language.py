#!/usr/bin/env python

# ----------------------------------------------------------------------
# Author:        yury.matveev@desy.de
# ----------------------------------------------------------------------

import numpy as np
import os
import matplotlib.pyplot as plt

SMU = None

# ----------------------------------------------------------------------
def clear_SMU():

    SMU.write_command(":TRAC1:CLE")
    SMU.write_command(":TRAC2:CLE")

# ----------------------------------------------------------------------
def init_setup_SMU():
    SMU.write_command(":OUTP:LOW GRO")
    SMU.write_command(":OUTP:OFF:MODE NORM")
    SMU.write_command(":OUTP:PROT ON")
    SMU.write_command(":OUTP:HCAP OFF")
    SMU.write_command(":FORM:ELEM:SENS VOLT, CURR, TIME")
    SMU.write_command(":FORM:SREG ASC")

# ----------------------------------------------------------------------
def abort_SMU():
    SMU.write_command(":ABOR:ALL (@1,2)")

# ----------------------------------------------------------------------
def reset_SMU():
    SMU.write_command("*RST")

# ----------------------------------------------------------------------
def set_channel_voltage(channel, voltage, compliance = 1e-3):
    SMU.write_command(":SOUR{:d}:FUNC:MODE VOLT".format(channel))
    SMU.write_command(":SOUR{:d}:VOLT:MODE FIX".format(channel))
    SMU.write_command(":SOUR{:d}:VOLT {:E}".format(channel, voltage))

# ----------------------------------------------------------------------
def set_continuous(channel, voltage, refresh = 1e-1):
    abort_SMU()
    dissable_channel(1)
    dissable_channel(2)
    SMU.check_for_errors()
    set_channel_sampling(channel, voltage, 2147483647, refresh)
    force_frigger(channel)

# ----------------------------------------------------------------------
def set_continuous_both(voltage1, voltage2, refresh = 1e-1):
    abort_SMU()
    set_channel_sampling(1, voltage1, 2147483647, refresh)
    set_channel_sampling(2, voltage2, 2147483647, refresh)
    SMU.check_for_errors()
    init_traces()
    force_frigger()

# ----------------------------------------------------------------------
def force_frigger(channels = ''):

    if channels:
        if isinstance(channels, int):
            SMU.write_command(":INITIATE:IMMEDIATE:ALL (@{:d})".format(channels))
        else:
            SMU.write_command(":INITIATE:IMMEDIATE:ALL (@1,2)")
    else:
        SMU.write_command(":INITIATE:IMMEDIATE:ALL (@1,2)")

# ----------------------------------------------------------------------
def set_compliance(channel, compliance):
    SMU.write_command(":sense{:d}:current:protection {:E}".format(channel, compliance))

#----------------------------------------------------------------------
def set_auto_range(channel, mode='res', threshold=80):
    SMU.write_command(":sense{:d}:current:range:auto:mode {}".format(channel, mode))
    SMU.write_command(":sense{:d}:current:range:auto:threshold {:d}".format(channel, threshold))

# ----------------------------------------------------------------------
def set_range(channel, range):
    SMU.write_command(":sense{:d}:current:range:upp {:E}".format(channel, range))

# ----------------------------------------------------------------------
def set_channel_list_sweep(channel, vals, fs):

    SMU.write_command(":source{:d}:voltage:mode list".format(channel))
    SMU.write_command(':source{:d}:voltage:range:auto off'.format(channel))
    SMU.write_command(':source{:d}:voltage:range 2e1'.format(channel))
    SMU.write_command(':source{:d}:voltage {:E}'.format(channel, vals[-1]))
    SMU.write_command(':source{:d}:function:mode voltage'.format(channel))
    SMU.write_command(':source{:d}:function:shape dc'.format(channel))

    cmd = ":source{:d}:list:voltage".format(channel)

    for val in vals:
        cmd += ' {:E},'.format(val)
    cmd = cmd[0:-1]
    SMU.write_command(cmd)

    SMU.write_command(':trigger{:d}:all:count {:d}'.format(channel, len(vals)))
    SMU.write_command(':trigger{:d}:all:timer {:E}'.format(channel, fs))
    SMU.write_command(':trigger{:d}:all:source timer'.format(channel))

# ----------------------------------------------------------------------
def set_channel_spot(channel, v, npts, fs):

    SMU.write_command(':source{:d}:voltage:mode fix'.format(channel))
    SMU.write_command(':source{:d}:voltage:range:auto off'.format(channel))
    SMU.write_command(':source{:d}:voltage:range 2e1'.format(channel))
    SMU.write_command(':source{:d}:function:mode voltage'.format(channel))
    SMU.write_command(':source{:d}:function:shape dc'.format(channel))
    SMU.write_command(':source{:d}:voltage {:E}'.format(channel, v))

    SMU.write_command(':trigger{:d}:all:count {:d}'.format(channel, npts))
    SMU.write_command(':trigger{:d}:all:timer {:E}'.format(channel, fs))
    SMU.write_command(':trigger{:d}:all:source timer'.format(channel))

# ----------------------------------------------------------------------
def set_channel_sampling(channel, v, npts, fs):
    SMU.write_command(':source{:d}:function:mode voltage'.format(channel))
    SMU.write_command(':source{:d}:voltage:range:auto on'.format(channel))
    SMU.write_command(':source{:d}:voltage {:E}'.format(channel, v))
    SMU.write_command(':source{:d}:voltage:trig {:E}'.format(channel, v))
    SMU.write_command(':source{:d}:function:shape dc'.format(channel))

    SMU.write_command(':trigger:immediate (@{:d})'.format(channel))
    SMU.write_command(':trigger{:d}:all:count {:d}'.format(channel, npts))
    SMU.write_command(':trigger{:d}:all:timer {:E}'.format(channel, fs))
    SMU.write_command(':trigger{:d}:all:source timer'.format(channel))

# ----------------------------------------------------------------------
def set_staircase_sweep(channel, vStart, vStop, step, stepTime=1e-3):

    npts = np.ceil(np.abs((vStop-vStart)/(step))) + 1
    wf = np.append(np.linspace(vStart, vStop, npts), np.linspace(vStop, vStart, npts)[1:])

    set_channel_list_sweep(channel, wf, stepTime)

    return int(npts)

# ----------------------------------------------------------------------
def inf_trigger(channel):

    SMU.write_command(':trigger{:d}:all:count INF'.format(channel))

# ----------------------------------------------------------------------
def dissable_channel(channel):
    SMU.write_command(':outp{:d} OFF'.format(channel))

# ----------------------------------------------------------------------
def init_traces():

    SMU.write_command(':format:elements:sense volt,curr,time,stat,sour')

    for channel in [1, 2]:

        SMU.write_command(':trace{:d}:feed:control nev'.format(channel))
        SMU.write_command(':trace{:d}:feed sense'.format(channel))
        SMU.write_command(':trace{:d}:clear'.format(channel))
        SMU.write_command(':trace{:d}:feed:control next'.format(channel))
        SMU.write_command(':trace{:d}:tstamp:format abs'.format(channel))

# ----------------------------------------------------------------------
def get_traces(channels):

    results = {"channels": channels,
               "data": []}

    for channel in channels:

        vstr = np.array(SMU.write_read_command(':trace{:d}:data?'.format(channel)).split(','))
        vals = vstr.astype(np.float)

        result = {"voltage": vals[0::5],
                  "current": vals[1::5],
                  "time": vals[2::5],
                  "status": vals[3::5],
                  "source": vals[4::5]}

        results["data"].append(result)

    return results

# ----------------------------------------------------------------------
def save_smu_data(data, scan_name=None, folder=None):

    if folder is None:
        folder = os.getcwd()

    if scan_name is None:
        last_num = 1
        file_list = os.listdir(folder)
        for file in file_list:
            name, ext = os.path.splitext(file)
            if ext == '.txt':
                try:
                    last_num = int(name.split('scan_')[1])
                except:
                    pass
        scan_name = 'scan_{}'.format(last_num + 1)
    else:
        scan_name = ''.join(os.path.splitext(scan_name)[:-1])

    full_file_name = os.path.join(folder, scan_name + '.txt')

    npts = 0
    field_names = []

    for channel in range(len(data["channels"])):
        field_names = ['ch_{}_{}'.format(channel+1, field) for field in list(data["data"][channel].keys())]
        npts = len(data["data"][channel]['source'])

    data_to_save = np.zeros((npts, len(field_names)))
    for ind, field_name in enumerate(field_names):
        _, channel, field = field_name.split('_')
        data_to_save[:, ind] = data["data"][int(channel)-1][field]

    np.savetxt(full_file_name, data_to_save, delimiter=',', newline='\n', header=','.join(field_names))

    return full_file_name

# ----------------------------------------------------------------------
def plot_smu_data(data, channels=(1, 2), plots=('IT', 'VT', 'IV'), macro_handle=None):

    fields = {'V': 'voltage',
              'I': 'current',
              'T': 'time'}

    if macro_handle is None:
        fig, axes = plt.subplots(nrows=len(plots), ncols=len(channels))
    else:
        fig, axes = macro_handle.pyplot.subplots(nrows=len(plots), ncols=len(channels))

    axes = np.array(axes).flatten()
    for ch_ind, ch in enumerate(channels):
        for pl_ind, pl in enumerate(plots):
            axes[ch_ind*len(plots) + pl_ind].set_title('CH_{} {}'.format(ch, pl))
            try:
                axes[ch_ind*len(plots) + pl_ind].plot(data["data"][ch-1][fields[pl[1]]],
                                                      data["data"][ch-1][fields[pl[0]]])
            except:
                pass

    if macro_handle is None:
        plt.show()
        plt.ion()

# ----------------------------------------------------------------------
if __name__ == "__main__":
    pass
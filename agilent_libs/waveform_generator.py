#!/usr/bin/env python

# ----------------------------------------------------------------------
# Author:        yury.matveev@desy.de
# ----------------------------------------------------------------------


import numpy as np

# ----------------------------------------------------------------------
def init_wf(fs):

    return {"fs":fs,
          "vseries": np.array(0)}

# ----------------------------------------------------------------------
def gen_sweep(wf, vmin, vmax, numpoints):
    sweep = np.linspace(vmin, vmax, num=numpoints)
    wf["vseries"] = np.append(wf["vseries"], sweep)
    return wf, len(wf["vseries"])

# ----------------------------------------------------------------------
def gen_pulse(wf, rise, hold, voltage, v_base=0):

    tseries = np.array([0, rise, hold, rise])
    tseries = np.cumsum(tseries)
    vseries = np.array([v_base, voltage, voltage, v_base])

    times = np.multiply(range(int(np.ceil(tseries[-1]/wf["fs"]))), wf["fs"])

    volts = np.interp(times, tseries, vseries, v_base, v_base)

    wf["vseries"] = np.append(wf["vseries"], volts)
    return wf, len(wf["vseries"])


# ----------------------------------------------------------------------
def gen_space(wf, time, voltage=0):

    wf["vseries"]= np.append(wf["vseries"], voltage*np.ones(int(np.ceil(time/wf["fs"]))))
    return wf, len(wf["vseries"])

# ----------------------------------------------------------------------
if __name__ == "__main__":
    pass

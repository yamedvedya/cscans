import PyTango
import time

from cs_constants import *


# ----------------------------------------------------------------------
def setup_detector(detectors, analysises, macro, pilc_scan, integ_time):
    """
    Here we setup Lambda: first we check that Lambda and LambdaOnlineAnalysis are not running,
    if yes - trying to stop them (within TIMEOUT_DETECTORS)
    """

    for detector in detectors.values():
        _detector_proxy = PyTango.DeviceProxy(detector)
        if _detector_proxy.State() != PyTango.DevState.ON:
            _detector_proxy.StopAcq()
            _time_out = time.time()
            while _detector_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_DETECTORS:
                time.sleep(0.1)
                macro.checkPoint()

            if _detector_proxy.State() != PyTango.DevState.ON:
                macro.output(_detector_proxy.State())
                raise RuntimeError('Cannot stop {}'.format(detector))

        if 'lambda' in detector:
            _detector_proxy.TriggerMode = 2
            _detector_proxy.FrameNumbers = max(macro.nsteps, 1000)
            if pilc_scan:
                _detector_proxy.ShutterTime = int(integ_time * 1000) - PILC_TRIGGER_TIME - PILC_DETECTOR_DELAY
            else:
                _detector_proxy.ShutterTime = int(integ_time * 1000)

            _detector_proxy.StartAcq()

        elif 'p300' in detector:
            #_detector_proxy.TriggerMode = 3 TEMP: TODO!!!
            _detector_proxy.NbFrames = macro.nsteps
            if pilc_scan:
                _detector_proxy.ExposureTime = integ_time - (PILC_TRIGGER_TIME - PILC_DETECTOR_DELAY)/1000
                _detector_proxy.ExposurePeriod = integ_time - (PILC_TRIGGER_TIME - PILC_DETECTOR_DELAY) / 1000 + 0.03
            else:
                _detector_proxy.ExposureTime = integ_time
                _detector_proxy.ExposurePeriod = integ_time + 0.03

            _detector_proxy.StartStandardAcq()

        else:
            raise RuntimeError('Unknown detector')

    _time_out = time.time()
    while _detector_proxy.State() not in [PyTango.DevState.MOVING, PyTango.DevState.RUNNING] \
            and time.time() - _time_out < TIMEOUT_DETECTORS:
        time.sleep(0.1)
        macro.checkPoint()

    if _detector_proxy.State() not in [PyTango.DevState.MOVING, PyTango.DevState.RUNNING]:
        macro.output(f'{detector} state: {_detector_proxy.State()}')
        raise RuntimeError(f'Cannot start {detector}')

    macro.report_debug(f'{detector} state after setup: {_detector_proxy.State()}')

    for analysis in analysises:
        _analysis_proxy = PyTango.DeviceProxy(analysis)

        if _analysis_proxy.State() == PyTango.DevState.MOVING:
            _analysis_proxy.StopAnalysis()

        _analysis_proxy.StartAnalysis()

        _time_out = time.time()
        while _analysis_proxy.State() not in [PyTango.DevState.MOVING, PyTango.DevState.RUNNING] \
                and time.time() - _time_out < TIMEOUT_DETECTORS:
            time.sleep(0.1)
            macro.checkPoint()

        if _analysis_proxy.State() != PyTango.DevState.MOVING:
            raise RuntimeError(f'Cannot start {detector}')

        macro.report_debug(f'{detector} state after setup: {_analysis_proxy.State()}')


# ----------------------------------------------------------------------
def stop_detector(detector, macro):
    macro.report_debug('Stopping {} Analysis'.format(detector))
    if detector == 'lmbd':
        if LAMBDA_MODE == 'ASAPO':
            _analysis_proxy = PyTango.DeviceProxy(macro.getEnv('LambdaASAPOAnalysis'))
        else:
            _analysis_proxy = PyTango.DeviceProxy(macro.getEnv('LambdaOnlineAnalysis'))
    elif detector == 'p300':
        _analysis_proxy = PyTango.DeviceProxy(macro.getEnv('PilatusAnalysis'))
    else:
        raise RuntimeError('Unknown detector')

    _analysis_proxy.StopAnalysis()
    time.sleep(0.1)

    _time_out = time.time()
    while _analysis_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_DETECTORS:
        time.sleep(0.1)

    if _analysis_proxy.State() != PyTango.DevState.ON:
        macro.output('Cannot stop Analysis!')

    macro.report_debug('Stopping {}'.format(detector))

    if detector == 'lmbd':
        _detector_proxy = PyTango.DeviceProxy(macro.getEnv('LambdaDevice'))
    elif detector == 'p300':
        _detector_proxy = PyTango.DeviceProxy(macro.getEnv('PilatusDevice'))
    else:
        raise RuntimeError('Unknown detector!')

    _detector_proxy.StopAcq()

    _time_out = time.time()
    while _detector_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_DETECTORS:
        time.sleep(0.1)

    if _detector_proxy.State() == PyTango.DevState.ON:
        if detector == 'lmbd':
            _detector_proxy.FrameNumbers = 1
        elif detector == 'p300':
            _detector_proxy.NbFrames = 1

        _detector_proxy.TriggerMode = 0
    else:
        macro.output('Cannot reset {}! Check the settings.'.format(detector))

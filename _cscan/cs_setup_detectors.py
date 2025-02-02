import PyTango
import time

from _cscan.cs_constants import *


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
            _detector_proxy.EnableASAPOStream = True
            if pilc_scan:
                _detector_proxy.ShutterTime = int(integ_time * 1000) - PILC_TRIGGER_TIME - PILC_DETECTOR_DELAY
            else:
                _detector_proxy.ShutterTime = int(integ_time * 1000)

            _detector_proxy.StartAcq()

        elif '1m' in detector:
            _detector_proxy.TriggerMode = 3 #TEMP: TODO!!!
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
            while _analysis_proxy.State() == PyTango.DevState.MOVING and time.time() - _time_out < TIMEOUT_DETECTORS:
                time.sleep(0.1)
                macro.checkPoint()

        if _analysis_proxy.State() != PyTango.DevState.ON:
            macro.output(f'{analysis} state: {_analysis_proxy.State()}')
            raise RuntimeError(f'Cannot stop {analysis}')

        _analysis_proxy.StartAnalysis()

        _time_out = time.time()
        while _analysis_proxy.State() not in [PyTango.DevState.MOVING, PyTango.DevState.RUNNING] \
                and time.time() - _time_out < TIMEOUT_DETECTORS:
            time.sleep(0.1)
            macro.checkPoint()

        if _analysis_proxy.State() != PyTango.DevState.MOVING:
            raise RuntimeError(f'Cannot start {analysis}')

        macro.report_debug(f'{analysis} state after setup: {_analysis_proxy.State()}')


# ----------------------------------------------------------------------
def stop_detector(detectors, analysises, macro):
    for analysis in analysises:
        macro.report_debug(f'Stopping {analysis}')
        _analysis_proxy = PyTango.DeviceProxy(analysis)

        try:
            _analysis_proxy.StopAnalysis()
            time.sleep(0.1)

            _time_out = time.time()
            while _analysis_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_DETECTORS:
                time.sleep(0.1)

            if _analysis_proxy.State() != PyTango.DevState.ON:
                macro.error(f'Cannot stop {analysis}!', exc_info=True)

        except Exception as err:
            macro.error(f'Cannot stop {analysis}: {repr(err)}!', exc_info=True)
            macro.report_debug(f'Cannot stop {analysis}: {repr(err)}!')

    for detector in detectors.values():
        macro.report_debug(f'Stopping {detector}')

        try:
            _detector_proxy = PyTango.DeviceProxy(detector)

            _detector_proxy.StopAcq()

            _time_out = time.time()
            while _detector_proxy.State() != PyTango.DevState.ON and time.time() - _time_out < TIMEOUT_DETECTORS:
                time.sleep(0.1)

            if _detector_proxy.State() == PyTango.DevState.ON:
                if 'lambda' in detector:
                    _detector_proxy.FrameNumbers = 1
                elif 'p1m' in detector:\
                    _detector_proxy.NbFrames = 1

                _detector_proxy.TriggerMode = 0
            else:
                macro.error(f'Cannot reset {detector}! Check the settings.', exc_info=True)
        except Exception as err:
            macro.error(f'Cannot reset {detector}: {repr(err)}!')
            macro.report_debug(f'Cannot reset {detector}: {repr(err)}!')


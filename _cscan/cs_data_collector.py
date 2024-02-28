'''
This class collect data from all data sources and send it to Sardana

Author yury.matveev@desy.de
'''

# general python imports
import time
import sys

if sys.version_info.major >= 3:
    from queue import Empty as empty_queue
else:
    from Queue import Empty as empty_queue

# cscans imports
from _cscan.cs_axillary_functions import ExcThread
from _cscan.cs_constants import *

# ----------------------------------------------------------------------
#
# ----------------------------------------------------------------------

class DataCollectorWorker(object):

    def __init__(self, macro, data_workers, point_trigger, error_queue,
                 extra_columns, motor_names, data):

        self._data_collector_status = 'idle'
        self._last_started_point = -1

        self._macro = macro
        self._data_workers = data_workers
        self._point_trigger = point_trigger

        self._motor_names = motor_names

        self._extra_columns = extra_columns
        self._data = data
        self._step_info = None
        self.last_collected_point = -1
        self._stop_after = 1e15

        self._worker = ExcThread(self._data_collector_loop, 'data_collector', error_queue)
        self.status = 'waiting'
        self._worker.start()

        self._macro.report_debug('Data collector started')

    # ----------------------------------------------------------------------
    def _data_collector_loop(self):
        try:
            self._data_collector_status = 'running'
            while not self._worker.stopped() and self.last_collected_point < self._stop_after:
                try:
                    _last_started_point, _end_time, _motor_positions = self._point_trigger.get(block=False)
                except empty_queue:
                    self.status = 'waiting'
                    time.sleep(REFRESH_PERIOD)
                else:
                    _not_reported = ''
                    self.status = 'collecting'
                    if _last_started_point > self.last_collected_point:
                        self._macro.report_debug("start collecting data for point {}".format(_last_started_point))
                        data_line = {}
                        all_detector_reported = False
                        while not all_detector_reported and not self._worker.stopped():
                            all_detector_reported = True
                            for worker in self._data_workers:
                                if worker.last_collected_point >= _last_started_point:
                                    data_line.update(worker.get_data_for_point(_last_started_point))
                                else:
                                    all_detector_reported *= False
                                    _not_reported = worker.channel_name

                        if all_detector_reported:
                            self._macro.report_debug("data for point {} is collected".format(_last_started_point))

                            self.last_collected_point = _last_started_point

                            for ec in self._extra_columns:
                                data_line[ec.getName()] = ec.read()

                            # Add final moveable positions
                            data_line['point_nb'] = _last_started_point
                            data_line['timestamp'] = _end_time

                            for name, position in zip(self._motor_names, _motor_positions):
                                data_line[name] = position

                            # Add extra data coming in the step['extrainfo'] dictionary
                            if 'extrainfo' in self._step_info:
                                data_line.update(self._step_info['extrainfo'])

                            self._data.addRecord(data_line)
                        else:
                            self._macro.report_debug('Not reported: {}'.format(_not_reported))
                            self._macro.report_debug("datacollected was stopped")
                            if _not_reported == 'Lambda' and self.last_collected_point == -1:
                                self._macro.error('There was no reply from Lambda, check the trigger cable!!!!')

            self.status = 'finished'
            self._macro.report_debug("data collector finished")

        except Exception as err:
            self.status = 'finished'
            self._macro.error('Datacollector error {} {}'.format(err, sys.exc_info()[2].tb_lineno))
            raise err

    # ----------------------------------------------------------------------
    def set_new_step_info(self, step_info):
        self._step_info = step_info

    # ----------------------------------------------------------------------
    def stop(self, integration_time):
        last_point = self.last_collected_point
        _got_time_out = False
        while not _got_time_out and self.status != 'finished':
            _timeout_start_time = time.time()
            while time.time() < _timeout_start_time + TIMEOUT:
                time.sleep(integration_time)
                if self.last_collected_point != last_point or self.status == 'finished':
                    break
            if self.last_collected_point == last_point and self.status != 'finished':
                _got_time_out = True
                break
            else:
                last_point = self.last_collected_point

        if _got_time_out:
            self._macro.report_debug("Cannot stop DataCollector, waits for {}, collected {}".format(
                self._stop_after, self.last_collected_point))

        self._worker.stop()
        while self._worker.status == 'running':
            time.sleep(REFRESH_PERIOD)

    # ----------------------------------------------------------------------
    def set_last_point(self, point_num):
        self._stop_after = point_num

    # ----------------------------------------------------------------------
    def is_finished(self):
        return self._worker.status == 'finished'

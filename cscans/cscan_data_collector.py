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

# cscan imports
from cscans.cscan_axillary_functions import ExcThread
from cscans.cscan_constants import *

# ----------------------------------------------------------------------
#
# ----------------------------------------------------------------------

class DataCollectorWorker(object):

    def __init__(self, macro, data_workers, point_trigger, error_queue,
                 extra_columns, moveables, data):

        self._data_collector_status = 'idle'
        self._last_started_point = -1
        self.acq_start_time = None

        self._macro = macro
        self._data_workers = data_workers
        self._point_trigger = point_trigger

        self._moveables = moveables

        self._extra_columns = extra_columns
        self._data = data
        self._step_info = None
        self.last_collected_point = -1
        self._stop_after = 1e15

        self._worker = ExcThread(self._data_collector_loop, 'data_collector', error_queue)
        self.status = 'waiting'
        self._worker.start()

        if self._macro.debug_mode:
            self._macro.debug('Data collector started')

    # ----------------------------------------------------------------------
    def _data_collector_loop(self):
        try:
            self._data_collector_status = 'running'
            while not self._worker.stopped() and self.last_collected_point < self._stop_after:
                try:
                    _last_started_point, _end_time, _motor_position = self._point_trigger.get(block=False)
                except empty_queue:
                    self.status = 'waiting'
                    time.sleep(0.1)
                else:
                    _not_reported = ''
                    self.status = 'collecting'
                    if _last_started_point > self.last_collected_point:
                        if self._macro.debug_mode:
                            self._macro.debug("start collecting data for point {}".format(_last_started_point))
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
                            if self._macro.debug_mode:
                                self._macro.debug("data for point {} is collected".format(_last_started_point))

                            self.last_collected_point = _last_started_point

                            for ec in self._extra_columns:
                                data_line[ec.getName()] = ec.read()

                            # Add final moveable positions
                            data_line['point_nb'] = _last_started_point
                            data_line['timestamp'] = _end_time - self.acq_start_time
                            for i, m in enumerate(self._moveables):
                                data_line[m.moveable.getName()] = _motor_position[i]

                            # Add extra data coming in the step['extrainfo'] dictionary
                            if 'extrainfo' in self._step_info:
                                data_line.update(self._step_info['extrainfo'])

                            self._data.addRecord(data_line)
                        else:
                            if self._macro.debug_mode:
                                self._macro.debug('Not reported: {}'.format(_not_reported))
                                self._macro.debug("datacollected was stopped")
                                if _not_reported == 'Lambda' and self.last_collected_point == -1:
                                    self._macro.error('There was no reply from Lambda, check the trigger cable!!!!')

            self.status = 'finished'
            if self._macro.debug_mode:
                self._macro.debug("data collector finished")

        except Exception as err:
            self.status = 'finished'
            self._macro.error('Datacollector error {} {}'.format(err, sys.exc_info()[2].tb_lineno))
            raise err

    # ----------------------------------------------------------------------
    def set_new_step_info(self, step_info):
        self._step_info = step_info

    # ----------------------------------------------------------------------
    def set_acq_start_time(self, acq_start_time):

        self.acq_start_time = acq_start_time

    # ----------------------------------------------------------------------
    def stop(self):
        self._worker.stop()
        while self._worker.status != 'finished':
            time.sleep(REFRESH_PERIOD)

    # ----------------------------------------------------------------------
    def set_last_point(self, point_num):
        self._stop_after = point_num

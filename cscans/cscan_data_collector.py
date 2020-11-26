'''
This class collect data from all data sources and send it to Sardana

Author yury.matveev@desy.de
'''

# general python imports
import time

from Queue import Empty as empty_queue

# cscan imports
from cscan_axillary_functions import ExcThread
from cscan_constants import *

# ----------------------------------------------------------------------
#
# ----------------------------------------------------------------------

class DataCollectorWorker(object):

    def __init__(self, macro, data_workers, point_trigger, error_queue,
                 extra_columns, moveables, data):

        self._data_collector_status = 'idle'
        self._last_started_point = -1
        self._macro = macro
        self._data_workers = data_workers
        self._point_trigger = point_trigger

        self._extra_columns = extra_columns
        self._moveables = moveables
        self._data = data
        self._step_info = None

        self._worker = ExcThread(self._data_collector_loop, 'data_collector', error_queue)
        self.status = 'waiting'
        self._worker.start()
        self.last_collected_point = -1

        if debug:
            self._macro.output('Data collector started')

    def _data_collector_loop(self):

        self._data_collector_status = 'running'
        while not self._worker.stopped():
            try:
                _last_started_point, _end_time, _motor_position = self._point_trigger.get(block=False)
            except empty_queue:
                self.status = 'waiting'
                time.sleep(0.1)
            else:
                _not_reported = ''
                self.status = 'collecting'
                if _last_started_point > self.last_collected_point:
                    if debug:
                        self._macro.output("start collecting data for point {}".format(_last_started_point))
                    data_line = {}
                    all_detector_reported = False
                    while not all_detector_reported and not self._worker.stopped():
                        all_detector_reported = True
                        for worker in self._data_workers:
                            if worker.data_buffer.has_key('{:04d}'.format(_last_started_point)):
                                data_line[worker.channel_name] = worker.data_buffer['{:04d}'.format(_last_started_point)]
                            else:
                                all_detector_reported *= False
                                _not_reported = worker.channel_name

                    if not self._worker.stopped():
                        if debug:
                            self._macro.output("data for point {} is collected".format(_last_started_point))

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
                        self.status = 'aborted'
                        if debug:
                            self._macro.output('Not reported: {}'.format(_not_reported))
                            self._macro.output("datacollected was stopped")

        self.status = 'finished'
        if debug:
            self._macro.output("data collector finished")

    def set_new_step_info(self, step_info):
        self._step_info = step_info

    def set_acq_start_time(self, acq_start_time):
        self.acq_start_time = acq_start_time

    def stop(self):
        self._worker.stop()
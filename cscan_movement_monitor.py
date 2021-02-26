# ----------------------------------------------------------------------
# Author:        yury.matveev@desy.de
# ----------------------------------------------------------------------

"""
"""

import PyTango

class Status_Monitor(object):

    POOLING_PERIOD = 5

    attributes = ["status"]

    def __init__(self, proxy=None, address=''):
        super(Status_Monitor, self).__init__()

        if proxy is not None:
            self.device_proxy = proxy
        elif address != '':
            self.device_proxy = PyTango.DeviceProxy(address)
        else:
            raise RuntimeError('Either proxy, either address is needed')

        self._events = []

        self._start_values = dict.fromkeys(self.attributes)
        self._intermediate_values = dict.fromkeys(self.attributes)
        self._in_move = dict.fromkeys(self.attributes, False)
        self._move_completed = dict.fromkeys(self.attributes, False)

        for attribute in self.attributes:
            self._start_values[attribute] = self.device_proxy.read_attribute(attribute).value

    # ----------------------------------------------------------------------
    def __del__(self):
        self.stop_monitor()

    # ----------------------------------------------------------------------
    def start(self):
        for attribute in self.attributes:
            self.device_proxy.poll_attribute(attribute, self.POOLING_PERIOD)
            self._events.append(self.device_proxy.subscribe_event(attribute, PyTango.EventType.CHANGE_EVENT, self.push_event, []))

    # ----------------------------------------------------------------------
    def reset(self):
        for attribute in self.attributes:
            self._start_values[attribute] = self.device_proxy.read_attribute(attribute).value
            self._intermediate_values[attribute] = False
            self._in_move[attribute] = False
            self._move_completed[attribute] = False

    # ----------------------------------------------------------------------
    def push_event(self, event):
        if not event.err:
            name = event.attr_name.split('/')[-1]
            if name in self.attributes:
                if not self._in_move[name] and event.attr_value.value != self._start_values[name]:
                    self._in_move[name] = True
                    self._intermediate_values[name] = event.attr_value.value
                elif self._in_move[name] and event.attr_value.value != self._intermediate_values[name]:
                    self._move_completed[name] = True
        else:
            print(event.errors)
            raise RuntimeError(event.errors)

    # ----------------------------------------------------------------------
    def stop_monitor(self):
        for event in self._events:
            self.device_proxy.unsubscribe_event(event)
        for attribute in self.attributes:
            self.device_proxy.stop_poll_attribute(attribute)

    # ----------------------------------------------------------------------
    def moved(self):
        done = True
        for value in self._move_completed.values():
            done *= value

        return done

    # ----------------------------------------------------------------------
    def in_move(self):
        move = True
        for value in self._in_move.values():
            move *= value

        return move
#!/usr/bin/env python

# ----------------------------------------------------------------------
# Author:        yury.matveev@desy.de
# ----------------------------------------------------------------------

import threading
import sys
import traceback
import socket
import numpy as np
import errno, time
from queue import Queue


class SMU(object):

    DATA_BUFFER_SIZE = 2**22

    SOCKET_TIMEOUT = 3
    RECONNECTION_DELAY = 5

    # ----------------------------------------------------------------------
    def __init__(self, host, port):

        super(SMU, self).__init__()

        self.host = host
        self.port = int(port)

        self.is_connected = False

        self._socket = None
        if not self._connect():
            raise RuntimeError("Cannot connect to SMU")

    # ----------------------------------------------------------------------
    def wait_till_complete(self, maximumTime = 600):
        ans = ''
        timeOut = False
        startTime = time.time()
        while True:
            try:
                self._socket.sendall('*OPC?\n'.encode())
                ans = self._socket.recv(self.DATA_BUFFER_SIZE).decode()
            except socket.error as err:
                if time.time() - startTime > maximumTime:
                    timeOut = True
                    break
            if "\n" in ans:
                while True:
                    try:
                        self._socket.recv(self.DATA_BUFFER_SIZE).decode()
                    except socket.error as err:
                            break
                break

        if not timeOut:
            return True
        else:
            return False

    # ----------------------------------------------------------------------
    def write_command(self, command =''):

        command += "\n"
        self._socket.sendall(str(command).encode())

    # ----------------------------------------------------------------------
    def write_read_command(self, command =''):

        command += "\n"
        self._socket.sendall(str(command).encode())

        ans = ''
        timeOut = False
        while True:
            try:
                ans += self._socket.recv(self.DATA_BUFFER_SIZE).decode()
            except socket.error as err:
                if err.errno != 11:
                    timeOut = True
                    break

            if "\n" in ans:
                break

        if not timeOut:
            return ans
        else:
            raise RuntimeError("The SMU timeout")

    # ----------------------------------------------------------------------
    def _connect(self):
        try:
            # self.log.info("Connecting to SMU ({}:{})".format(self.host,self.port))
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.settimeout(self.SOCKET_TIMEOUT)

            startTimeout = time.time()
            timeOut = False
            while not timeOut and not self.is_connected:
                err = self._socket.connect_ex((self.host, self.port))
                if err == 0 or err == errno.EISCONN:
                    self.is_connected = True
                if time.time() - startTimeout > self.SOCKET_TIMEOUT:
                    timeOut = True

            if self.is_connected:
                print("TCP/IP connection to SMU established!")
            else:
                print("Connection to SMU cannot be established")

            return True

        except (socket.error, Exception) as ex:
            print(ex)
            return False

    # ----------------------------------------------------------------------
    def state(self):

        return self.is_connected

    # ----------------------------------------------------------------------
    def _get_errors(self):
        return self.write_read_command(':SYST:ERR:ALL?')

    # ----------------------------------------------------------------------
    def check_for_errors(self):
        rawAnswer = self._get_errors()

        errors = rawAnswer.split(',')

        errCodes = np.array(errors[::2]).astype(np.int)
        return errCodes, errors[1::2]

# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
class ExcThread(threading.Thread):

    # ----------------------------------------------------------------------
    def __init__(self, target, threadname, errorBucket,  *args):
        threading.Thread.__init__(self, target=target, name=threadname, args=args)
        self._stop_event = threading.Event()
        self.bucket = errorBucket

    # ----------------------------------------------------------------------
    def run(self):
        try:
            if hasattr(self, '_Thread__target'):
                self.ret = self._Thread__target(*self._Thread__args, **self._Thread__kwargs)
            else:
                self.ret = self._target(*self._args, **self._kwargs)
        except Exception as exp:
            print('exception caught in propagating thread:')
            print(exp)
            traceback.print_tb(sys.exc_info()[2])
            self.bucket.put(sys.exc_info())

    # ----------------------------------------------------------------------
    def stop(self):
        self._stop_event.set()

    # ----------------------------------------------------------------------
    def stopped(self):
        return self._stop_event.isSet()

# ----------------------------------------------------------------------
if __name__ == "__main__":
    pass
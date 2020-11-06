#!/usr/bin/env python
'''
File name lms.py

The purpose of these script is remotly control lm cameras.

Author yury.matveev@desy.de
'''

import socket
import errno, time

from sardana.macroserver.macro import *

camera_host = 'hase027m'
camera_port = 23358

fsbt_host = 'hasep23dev'
fsbt_port = 12658

__all__ = ['lm4', 'lm5', 'lm6', 'lm7', 'lm8', "lms_out"]


# ----------------------------------------------------------------------
#                       These classes are called by user
# ----------------------------------------------------------------------

class lm4(Macro):

    """ Switches the camera viewer to LM4

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).write_command('set_camera lm04')

# ----------------------------------------------------------------------
class lm5(Macro):

    """ Switches the camera viewer to LM4

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).write_command('set_camera lm05')


# ----------------------------------------------------------------------
class lm6(Macro):

    """ Switches the camera viewer to LM4

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).write_command('set_camera lm06')


# ----------------------------------------------------------------------
class lm7(Macro):

    """ Switches the camera viewer to LM4

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).write_command('set_camera lm07')


# ----------------------------------------------------------------------
class lm8(Macro):

    """ Switches the camera viewer to LM4

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).write_command('set_camera lm08')


# ----------------------------------------------------------------------
class lms_out(Macro):

    """ Switches the camera viewer to LM4

    """

    def run(self, *args):
        socket = SocketConnection(fsbt_host, fsbt_port)
        list_of_elements = socket.write_read_command('getElementList')



# ----------------------------------------------------------------------
#                       Auxiliary class to set environment
# ----------------------------------------------------------------------

class SocketConnection(object):
    DATA_BUFFER_SIZE = 2 ** 22

    SOCKET_TIMEOUT = 1
    RECONNECTION_DELAY = 5

    # ----------------------------------------------------------------------
    def __init__(self, host, port):

        super(SocketConnection, self).__init__()

        self.host = host
        self.port = int(port)

        self.is_connected = False

        self._socket = None
        if not self._connect():
            raise RuntimeError("Cannot connect to SMU")

    # ----------------------------------------------------------------------
    def wait_till_complete(self, maximumTime=600):
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
    def write_command(self, command=''):

        command += "\n"
        self._socket.sendall(str(command).encode())

    # ----------------------------------------------------------------------
    def write_read_command(self, command=''):

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
            raise RuntimeError("The socket timeout")

    # ----------------------------------------------------------------------
    def _connect(self):
        try:
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
                print("TCP/IP connection established!")
            else:
                print("Connection cannot be established")

            return True

        except (socket.error, Exception) as ex:
            print(ex)
            return False

    # ----------------------------------------------------------------------
    def state(self):

        return self.is_connected

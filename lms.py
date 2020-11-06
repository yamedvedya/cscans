#!/usr/bin/env python
'''
File name lms.py

The purpose of these script is remotly control lm cameras.

Author yury.matveev@desy.de
'''

import socket
import errno, time
import jsom
import StringIO

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
        SocketConnection(camera_host, camera_port).write_command('set_camera LM04')

# ----------------------------------------------------------------------
class lm5(Macro):

    """ Switches the camera viewer to LM4

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).write_command('set_camera LM05')


# ----------------------------------------------------------------------
class lm6(Macro):

    """ Switches the camera viewer to LM4

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).write_command('set_camera LM06')


# ----------------------------------------------------------------------
class lm7(Macro):

    """ Switches the camera viewer to LM4

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).write_command('set_camera LM07')


# ----------------------------------------------------------------------
class lm8(Macro):

    """ Switches the camera viewer to LM4

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).write_command('set_camera LM08')


# ----------------------------------------------------------------------
class lms_out(Macro):

    """ Switches the camera viewer to LM4

    """

    def run(self, *args):
        socket = SocketConnection(fsbt_host, fsbt_port)
        status, list_of_elements = socket.write_read_command('getElementsList')
        if status:
            self.output
            for name, type in list_of_elements.items():
                if type == 'screen':
                    socket.write_command('out {}'.format(name))


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
            raise RuntimeError("Cannot connect")

    # ----------------------------------------------------------------------
    def write_command(self, command=''):

        command += "\n"
        self._socket.sendall(str(command).encode())

    # ----------------------------------------------------------------------
    def write_read_command(self, command=''):

        command += "\n"
        self._socket.sendall(str(command).encode())

        try:
            return self._socket.recv(self.DATA_BUFFER_SIZE).decode()
        except socket.error as err:
            raise RuntimeError("The socket error {}".format(err))

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

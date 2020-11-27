#!/usr/bin/env python
'''
File name lms.py

The purpose of these script is remotely control lm cameras.

Author yury.matveev@desy.de
'''

import socket
import errno, time
import json
from io import StringIO

from sardana.macroserver.macro import *

camera_host = 'hasep23web'
camera_port = 23358

fsbt_host = 'hasep23swt01'
fsbt_port = 12658

__all__ = ['lm4', 'lm5', 'lm6', 'lm7', 'lm8',
           'lm5_out', 'lm6_out', 'lm7_out', 'lm8_out',
           'lm5_in', 'lm6_in', 'lm7_in', 'lm8_in',
           "lms_out"]


# ----------------------------------------------------------------------
#                       These classes are called by user
# ----------------------------------------------------------------------

class lm4(Macro):

    """ Switches the camera viewer to LM4

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).send_cmd('set_camera LM04')

# ----------------------------------------------------------------------
class lm5(Macro):

    """ Switches the camera viewer to LM5

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).send_cmd('set_camera LM05')


# ----------------------------------------------------------------------
class lm6(Macro):

    """ Switches the camera viewer to LM6

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).send_cmd('set_camera LM06')


# ----------------------------------------------------------------------
class lm7(Macro):

    """ Switches the camera viewer to LM7

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).send_cmd('set_camera LM07')


# ----------------------------------------------------------------------
class lm8(Macro):

    """ Switches the camera viewer to LM8

    """

    def run(self, *args):
        SocketConnection(camera_host, camera_port).send_cmd('set_camera LM08')


# ----------------------------------------------------------------------
class lm5_out(Macro):

    """ Move LM6 out

    """

    def run(self, *args):
        SocketConnection(fsbt_host, fsbt_port).send_cmd('out LM5')


# ----------------------------------------------------------------------
class lm5_in(Macro):

    """ Put LM5 in

    """

    def run(self, *args):
        SocketConnection(fsbt_host, fsbt_port).send_cmd('in LM5')


# ----------------------------------------------------------------------
class lm6_out(Macro):

    """ Move LM6 out

    """

    def run(self, *args):
        SocketConnection(fsbt_host, fsbt_port).send_cmd('out LM6')


# ----------------------------------------------------------------------
class lm6_in(Macro):

    """ Put LM6 in

    """

    def run(self, *args):
        SocketConnection(fsbt_host, fsbt_port).send_cmd('in LM6')

# ----------------------------------------------------------------------
class lm7_out(Macro):

    """ Move LM7 out

    """

    def run(self, *args):
        SocketConnection(fsbt_host, fsbt_port).send_cmd('out LM7')

# ----------------------------------------------------------------------
class lm7_in(Macro):

    """ Put LM7 in

    """

    def run(self, *args):
        SocketConnection(fsbt_host, fsbt_port).send_cmd('in LM7')

# ----------------------------------------------------------------------
class lm8_out(Macro):

    """ Move LM8 out

    """

    def run(self, *args):
        SocketConnection(fsbt_host, fsbt_port).send_cmd('out LM8')



# ----------------------------------------------------------------------
class lm8_in(Macro):

    """ Put LM8 in

    """

    def run(self, *args):
        SocketConnection(fsbt_host, fsbt_port).send_cmd('in LM8')

# ----------------------------------------------------------------------
class lms_out(Macro):

    """ Move all LMs out

    """

    def run(self, *args):
        socket = SocketConnection(fsbt_host, fsbt_port)
        status, list_of_elements = socket.send_cmd('getElementsList')
        if status:
            for name, type in list_of_elements.items():
                if type == 'screen':
                    socket.send_cmd('out {}'.format(name))


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
    def send_cmd(self, command=''):

        try:
            self._socket.sendall(str(command).encode())

            start_timeout = time.time()
            time_out = False
            got_answer = False
            ans = ''
            while not time_out and not got_answer:
                try:
                    ans = self._socket.recv(self.DATA_BUFFER_SIZE).decode()
                    got_answer = True
                except socket.error as err:
                    if err.errno != 11:
                        time_out = True
                    if time.time() - start_timeout > self.SOCKET_TIMEOUT:
                        time_out = True

            if not time_out:
                try:
                    return json.load(StringIO(ans))
                except:
                    return False, []

            else:
                self.is_connected = False
                return False, []

        except socket.error as err:
            self.is_connected = False
            return False, []

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
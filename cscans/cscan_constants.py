'''
This file contains all constants, needed by cscans

Author yury.matveev@desy.de
'''

TIMEOUT = 3
TIMEOUT_LAMBDA = 3
REFRESH_PERIOD = 1e-4

DUMMY_MOTOR = 'exp_dmy01'

YES_OPTIONS = ['Yes', 'yes', 'y']
NO_OPTIONS = ['No', 'no', 'n']

COUNTER_RESET_DELAY = 0

HKL_GRID_RESOLUTION = 0.0005

# Super ugly, should be better solution:
counter_names = {'sis3820': 'p23/counter/eh'}
timer_names = {'eh_t01': 'p23/dgg2/eh.01',
               'eh_t02': 'p23/dgg2/eh.02'}

MOTORS_POSITION = 'series' # 'sync' or 'series'

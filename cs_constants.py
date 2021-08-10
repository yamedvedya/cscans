'''
This file contains all constants, needed by cscans

Author yury.matveev@desy.de
'''

# General timeout to interrupt scan
TIMEOUT = 15
# Special timout for non-sync detector
TIMEOUT_DETECTORS = 15
# General delay on loops (to decrease processor load)
REFRESH_PERIOD = 1e-4

#Stupid staff, but:
ADDITIONAL_POINT_DELAY = 35e-3

#TEMP for tests
LAMBDA_MODE = 'ASAPO'

# which position is taken as "point" position
MOTORS_POSITION_LOGIC = 'center' # 'before', 'center' or 'after'

# How frequent the motors positions is save to log
MOTOR_POSITION_REFRESH_PERIOD = 1e-1

# which motor is used for time scan
DUMMY_MOTOR = 'exp_dmy01'

# possible answers to user requests
YES_OPTIONS = ['Yes', 'yes', 'y']
NO_OPTIONS = ['No', 'no', 'n']

# matching columns in diffrac answer to motors
HKL_MOTORS_MAP = {'e6c_h': 0, 'e6c_k': 1, 'e6c_l': 2}

# mesh resolution to convert reciprocal trajectory to real space
HKL_GRID_RESOLUTION = 0.01
# precision of conversion
POSITION_ROUND = 3
# maximum deviation from linear law, after which the movement is considered as non-linear
LIN_MOVE_THRESHOLD = 1e-2

# Super ugly, should be better solution:
COUNTER_NAMES = ('sis3820',)
TIMER_PREFIXES = ('eh_t', 'exp_t')
DETECTOR_NAMES = ('lmbd', 'p300')

PILC_DETECTOR_DELAY = 1 # ms
PILC_TRIGGER_TIME = 0.5 #ms

# PILC addresses:
PILC_TRIGGERS = ['p23/pilctriggergenerator/dev.01', 'p23/pilctriggergenerator/dev.02']
PILC_COUNTER = 'p23/pilcscanslave/exp.03'
PILC_ADC = 'p23/pilcscanslave/exp.04'

PILC_MOTORS_MAP = {'gx': {'device': 0, 'encoder': 1},
                   'gy': {'device': 0, 'encoder': 2},
                   'gz': {'device': 0, 'encoder': 4},
                   'omega': {'device': 0, 'encoder': 3},
                   'delta': {'device': 0, 'encoder': 5},
                   'omega_t': {'device': 1, 'encoder': 1},
                   'gamma': {'device': 1, 'encoder': 2},
                   'phi': {'device': 1, 'encoder': 4},
                   'chi': {'device': 1, 'encoder': 3},
                   'mu': {'device': 1, 'encoder': 5}}

PILC_DETECTOR_MAP = {'eh_c01': {'device': 'CT', 'attribute': 'Counter1Data'}}

PILC_MINIMUM_DISPLACEMENT = 0.01

TMP_FILE = '/tmp/cscan_motor_backup'


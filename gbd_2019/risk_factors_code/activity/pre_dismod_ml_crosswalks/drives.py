import getpass
import sys


class Drives(object):
    """
    A central way to get the home directory of the user and/or the J drive, regardless of operating system.
    """
    def __init__(self):
        if 'linux' in sys.platform:
            self.j = 'FILEPATH'
            self.h = 'FILEPATH'

        elif 'darwin' in sys.platform:
            self.j = 'FILEPATH'
            self.h = 'FILEPATH'

        elif 'win' in sys.platform:
            self.j = 'FILEPATH'
            self.h = 'FILEPATH'

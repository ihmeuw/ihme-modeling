"""
In liu of a more thoughtful logging module, this will store the standard
output from the inpatient pipeline. This is useful because we print a fair
amount of important information to screen. age-sex splitting is probably
the most illustrative example of this.
"""

import datetime
import sys


class ClinfoLogger(object):
    def __init__(self, write_path):
        self.terminal = sys.stdout
        log_start = datetime.datetime.now()
        self.log = open(f"{write_path}_{log_start}.log", "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass
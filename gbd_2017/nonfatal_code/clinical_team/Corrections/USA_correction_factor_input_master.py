
# coding: utf-8

import platform
import sys
import glob
import subprocess
import getpass

user = getpass.getuser()

# Environment:
if platform.system() == "Linux":
    root = "/FILEPATH/j"
else:
    root = "J:"

# get a list of files to process
files = glob.glob(r"filepath")

for f in files:
    location_id = f[-7:-4]
    if location_id == "527":
        print("this is cali, loc id {}".format(location_id))
        slots = 30
    else:
        print("not cali, loc id {}".format(location_id))
        slots = 15
    qsub = qsub

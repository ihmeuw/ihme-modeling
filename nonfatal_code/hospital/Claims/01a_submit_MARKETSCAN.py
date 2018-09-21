
# coding: utf-8

"""
Script to submit parallel python jobs to process Phil Claims data
"""

import platform
import subprocess
import time
import pandas as pd
import numpy as np
import os.path



if platform.system() == "Linux":
    root = "FILEPATH"
    repo = "FILEPATH"
else:
    root = "FILEPATH"
    repo = "FILEPATH"

start = time.time()

datasets = ['ccae2012', 'ccae2010', 'mdcr2012', 'mdcr2010', 'ccae2000', 'mdcr2000']
# for measure in ['inc', 'prev']:
for dataset in ['ccae', 'mdcr']:
    for year in [2012, 2010, 2000]:
        # for age in np.arange(0, 101, 1):
        for age in np.arange(100, -1, -1):
            for sex in [2, 1]:
                filepath = "FILEPATH"
                if os.path.exists(FILEPATH):
                    if dataset == 'mdcr' or year == 2000 or (dataset == "ccae" and age < 21):
                        qsub = "qsub -P proj_hospital -N " + dataset[0:2] + str(year) + "_" + str(age)\
                        + " -pe multi_slot 6 -l mem_free=12g -o FILEPATH -e FILEPATH "
                        # print(qsub)
                        subprocess.call(qsub, shell=True)
                    else:
                        qsub = "qsub -P proj_hospital -N " + dataset[0:2] + str(year) + "_" + str(age)\
                        + " -pe multi_slot 10 -l mem_free=20g -o FILEPATH -e FILEPATH "\
                        + repo + "FILEPATH" + repo
                        # print(qsub)
                        subprocess.call(qsub, shell=True)

import pandas as pd
import numpy as np
import os
from db_tools import ezfuncs
from db_tools import query_tools
from db_queries import get_population
from db_tools.ezfuncs import query
from subprocess import Popen, PIPE
import time
import getpass
import subprocess


def construct_qsub(prioritization_run_id, cause_id):
    user = getpass.getuser()
    errout_path = "FILEPATH"
    python_shell = "FILEPATH"
    code_path = "FILEPATH"
    qsub_string = (
        "qsub -N {name} -l m_mem_free=170G -l fthread=25 -l archive=True -P proj_shocks"
        " -e {error_path} -q all.q {shell} {code_path}"
        " --cause_id {cause_id} --run_id {run_id}".format(
            name="save_{}_{}".format(prioritization_run_id, cause_id), error_path=errout_path,
            shell=python_shell, code_path=code_path, cause_id=cause_id, run_id=prioritization_run_id))
    return qsub_string


if __name__ == "__main__":
    causes = [302, 332, 337, 338, 341, 345, 357, 387, 408, 695,
              699, 703, 707, 711, 725, 726, 727, 729, 842, 854, 945, ]
    prioritization_run_id = 48

    for cause_id in causes:
        print("launching {}".format(cause_id))
        print()
        qsub_string = construct_qsub(prioritization_run_id, cause_id)
        qsub = subprocess.Popen(qsub_string,
                                shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        (stdout, stderr) = qsub.communicate()

import subprocess
import os
import time
import sys
import configparser
import ast
from ml_crosswalk.drives import Drives
import glob


def predict_draws_in_parallel(config, topic, version, algorithm, estimator,
                                                  estimand, estimators_object):
    """

    :param config:
    :param head_dir:
    :param topic:
    :param predict_unseen:
    :return:
    """
    error_dir = "datasets"

    head_dir = config[topic]['head_dir']
    if not os.path.isdir(os.path.join(head_dir, error_dir)):
        os.mkdir(os.path.join(head_dir, error_dir))

    shell_file = Drives().h + 'FILEPATH/python_mro.sh'

    path = str(config[topic]['path_to_original_draws_files'])
    print(path)
    all_files = glob.glob(Drives().j + path + "/*.csv")

    for i in range(3):  # all_files
        print(all_files[i])
        cmd = ["qsub", "-P", "proj_ensemble",
               "-pe", "multi_slot", "2",  # number of slots
               "-N", "subproc_{}_{}_{}_{}".format(topic, version[0:4], algorithm, i),
               "-e", "FILEPATH",
               "-o", "FILEPATH",
               shell_file,
               Drives().h + 'FILEPATH/predict_on_draws.py',
               str(all_files[i]), topic,
               estimator, estimand, estimators_object, version]
        proc = subprocess.Popen(cmd)
        proc.communicate()
        # subprocess.call(cmd)
    return []


if __name__ == "__main__":
    args = sys.argv[1:]
    topic = args[0]
    version = args[1]
    algorithm = args[2]

    config = configparser.ConfigParser()
    config.read(Drives().h + 'FILEPATH/config.ini')

    head_dir = Drives().j + config[topic]['head_dir']

    if not os.path.exists(os.path.join(head_dir, "output")):
        os.mkdir(os.path.join(head_dir, "output"))
    files = predict_draws_in_parallel(config, topic, version, algorithm)

    while True:
        f = list()
        for file in files:
            f.append(os.path.isfile(os.path.join(head_dir, "output", file)))
        if all(f):
            break
        else:
            time.sleep(15)
            print("--- {} files missing ---".format(sum([1 for x in f if not x])))

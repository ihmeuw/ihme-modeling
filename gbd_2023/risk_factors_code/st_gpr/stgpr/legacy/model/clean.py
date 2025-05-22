import os
import shutil
import sys

import pandas as pd

import stgpr_schema
from stgpr_helpers import columns

from stgpr.legacy.st_gpr import helpers as hlp
from stgpr.lib import constants, utils

######################## Functions #######################################################


def clean_up(run_type, holdout_num):

    st_temps = ["st_temp_{}".format(holdout_num)]
    gpr_temps = ["gpr_temp_{}".format(holdout_num)]

    rake_temps = ["rake_temp_{}".format(holdout_num)]
    if holdout_num == 0:
        rake_mean_temps = ["rake_means_temp_{}".format(holdout_num)]
    else:
        rake_mean_temps = []

    full = st_temps + gpr_temps + rake_temps + rake_mean_temps

    settings = stgpr_schema.get_settings()
    output_root = settings.output_root_format.format(stgpr_version_id=run_id)
    for x in full:
        shutil.rmtree("{}/{}".format(output_root, x))

    # last get rid of the two temp CSVs for stage1, if another holdout
    # has not already deleted
    stupid_files = ["{}/prepped.csv".format(output_root)]

    for i in stupid_files:
        try:
            os.remove(i)
        except OSError:
            pass

    print("all clean")


if __name__ == "__main__":
    run_id = int(sys.argv[1])
    run_type = sys.argv[2]
    holdout_num = int(sys.argv[3])
    draws = int(sys.argv[4])

    # make sure rmse jobs completed before erasing stuff
    msg = "No fit stats! Not gonna clean til that is sorted."
    settings = stgpr_schema.get_settings()
    output_root = settings.output_root_format.format(stgpr_version_id=run_id)
    assert os.path.exists("{}/fit_stats.csv".format(output_root)), msg

    param_set = utils.get_best_parameter_set(run_id)

    # clean out temp folders
    clean_up(run_type, holdout_num)

    if (draws > 0) & (holdout_num == 0):
        # remove temporary directory draws_temp_0/{param_set} after moving non-draw files
        inpath = f"{output_root}/draws_temp_0/{param_set}"
        outpath = f"{output_root}/draws_temp_0"
        files = os.listdir(inpath)
        for f in files:
            # if file is not a {location}.csv draw file, move to final location. This acts as
            # a safeguard and may not be necessary
            try:
                location_id = int(f.split(".")[0])
            except ValueError:
                os.rename(f"{inpath}/{f}", f"{outpath}/{f}")
        shutil.rmtree(inpath)

    fin = pd.DataFrame()
    fin.to_csv("{}/model_complete.csv".format(output_root))
    print("Model complete. You win!")

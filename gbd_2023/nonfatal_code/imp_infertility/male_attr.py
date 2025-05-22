import os
import argparse
import pandas as pd
import json
import random
import math
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from job_utils import draws, parsers


##############################################################################
# class to calculate attribution
##############################################################################

class AttributeMale(draws.SquareImport):

    def __init__(self, me_map, decomp_step, **kwargs):
        # super init
        super(AttributeMale, self).__init__(**kwargs)

        # store data by me in this dict key=me_id, val=dataframe
        self.me_map = me_map
        self.me_dict = {}

        # import every input
        for v in me_map.values():
            inputs = v.get("srcs", {})
            for me_id in inputs.values():
                self.me_dict[me_id] = self.import_square(
                    gopher_what={"modelable_entity_id": me_id},
                    source="epi",
                    decomp_step=decomp_step)

    def remove_negatives(self, df):
        positive = df.copy()
        positive[positive<0.] = 0.
        assert (positive < 0.0).any().any() == False
        return positive

    def calc_residual(self):

        # compile keys
        env_prim_key = self.me_map["env"]["srcs"]["prim"]
        env_sec_key = self.me_map["env"]["srcs"]["sec"]
        kline_key = self.me_map["kline"]["srcs"]["meid"]
        idio_prim_key = self.me_map["idio"]["trgs"]["prim"]
        idio_sec_key = self.me_map["idio"]["trgs"]["sec"]
        cong_uro_key = self.me_map["cong_uro"]["srcs"]["meid"]

        # sum up klinefelter
        kline = self.me_dict[kline_key]

        #sum up congenital urogenital
        cong_uro = self.me_dict[cong_uro_key] 

        #Proportion
        mean = 0.0909
        std_error = 0.0388

        proportion = random.gauss(mean, std_error)
        if proportion < 0:
            proportion = 0

        # subtract klinefleter and congenital urogenital from total primary
        idio_prim = self.me_dict[env_prim_key] - (kline) - (cong_uro * proportion)
        idio_prim = self.remove_negatives(idio_prim)
        self.me_dict[idio_prim_key] = idio_prim
        self.me_dict[idio_sec_key] = self.me_dict[env_sec_key]

##############################################################################
# function to run attribution
##############################################################################


def male_attribution(me_map, location_id, out_dir, decomp_step):

    # declare calculation dimensions
    dim = AttributeMale.default_idx_dmnsns
    dim["location_id"] = location_id
    dim["age_group_id"] = [8,9,10,11,12,13,14]
    dim["measure_id"] = [5]
    dim["sex_id"] = [1]

    # run attribution
    attributer = AttributeMale(me_map=me_map, decomp_step=decomp_step,
        idx_dmnsns=dim)
    attributer.calc_residual()

    # save results to disk
    for mapper in me_map.values():
        outputs = mapper.get("trgs", {})
        for me_id in outputs.values():
            fname = str(location_id[0]) + ".h5"
            out_df = attributer.me_dict[me_id].reset_index()
            out_df.to_hdf(os.path.join(out_dir, str(me_id), fname),
                key="draws", format="table", data_columns=list(dim.keys()))

##############################################################################
# when called as a script
##############################################################################

if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("--map_name", help=("nested dictionary to pull out of"
                        " infertility map"), required=True, type=str)
    parser.add_argument("--out_dir", help="root directory to save stuff",
                        required=True)
    parser.add_argument("--location_id", help="which year to use",
                        type=parsers.int_parser, nargs="*", required=True)
    parser.add_argument("--decomp_step", required=True,
                        help="the decompostion step associated with this data",
                        type=str)
    args = vars(parser.parse_args())

    # import JSON file with me_id source and target information 
    code_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(code_dir,'infertility_map.json')
    with open(filepath, 'r') as f:
        imap = json.load(f)
    me_map = imap[args["map_name"]]

    # call function
    male_attribution(me_map=me_map, out_dir=args["out_dir"],
                     location_id=args["location_id"],
                     decomp_step=args["decomp_step"])

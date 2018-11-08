from __future__ import division
import os
import numpy as np
import pandas as pd
import argparse
import random
import copy
import re
from job_utils import draws, parsers


class Gynecological(draws.SquareImport):

    def __init__(self, me_map, **kwargs):
        # super init
        super(Gynecological, self).__init__(**kwargs)

        # store data in this dict 
        # key=modelable_entity_id
        self.me_map = me_map
        self.me_dict = {}

        #import every input and create a dictionary of dataframes
        for mapper_key, mapper in me_map.items():
            inputs = mapper.get("srcs",{})
            for src_key, me_id in inputs.items():
                if src_key == "prop":
                    print("Inside prop, retrieving meid {}".format(me_id))
                    dim_prop = copy.deepcopy(self.idx_dmnsns)
                    dim_prop["measure_id"] = [18] # proportion
                    prop = draws.SquareImport(idx_dmnsns=dim_prop)
                    self.me_dict[me_id] = prop.import_square(
                        gopher_what={"modelable_entity_id": me_id},
                        source="epi")
                else:
                    print("Inside else, retrieving meid {}".format(me_id))
                    self.me_dict[me_id] = self.import_square(
                        gopher_what={"modelable_entity_id": me_id},
                        source="epi")

    def draw_beta(self, mean, lower, upper):
        np.random.seed()
        sd = (upper - lower) / (2 * 1.96)
        sample_size = mean * (1 - mean) / sd ** 2
        alpha = mean * sample_size
        beta = (1 - mean) * sample_size
        draws = np.random.beta(alpha, beta, size=1000)
        return draws

    def calc_asymp_uf(self):
        draws = self.draw_beta(mean=.5,lower=.2,upper=.7)
        symp_key = self.me_map["uterine_fibroids"]["srcs"]["symp"]
        asymp_key = self.me_map["uterine_fibroids"]["trgs"]["asymp"]
        # Total uterine fibroids is symptomatic + asymptomatic
        # We have symptomatic data from dismod and proportion information
        # from literature (NID 341497). 
        # Use those two things to back calculate asymptomatic uterine fibroids.
        symp = self.me_dict[symp_key].copy()
        tot_uf = symp / draws
        asymp = tot_uf - symp
        self.me_dict[asymp_key] = asymp

    def adjust_pms(self):
        '''adjusts pms cases for pregnancy prevalence and incidence'''
        pms_key = self.me_map["pms"]["srcs"]["tot"]
        prop_key = self.me_map["pms"]["srcs"]["prop"]
        adj_key = self.me_map["pms"]["trgs"]["adj"]

        # broadcast prop to preferred shape
        broadcast_over = self.idx_dmnsns.keys()
        broadcast_over.remove('measure_id')
        prop = self.me_dict[prop_key].copy()
        prop = prop.reset_index()[broadcast_over + self.draw_cols]
        prop = pd.merge(self.index_df.reset_index(), prop, on=broadcast_over,
                      how="left").set_index(self.idx_dmnsns.keys())

        pms = self.me_dict[pms_key].copy()
        adjusted = pms * (1-prop)
        
        self.me_dict[adj_key] = adjusted

##############################################################################
# function to run core code
##############################################################################

def gyne_data(me_map, location_id, out_dir):
    # retrieve default dimensions needed to initialize 
    # draws.SquareImport class, then change as needed
    dim = Gynecological.default_idx_dmnsns

    # production dimensions
    dim["location_id"] = [location_id]
    dim["sex_id"] = [2]

    # initialize instance of Gynecological class
    gyne = Gynecological(me_map=me_map, idx_dmnsns=dim)
    gyne.calc_asymp_uf()
    gyne.adjust_pms()
    
    for mapper_key, mapper in me_map.items():
        outputs = mapper.get("trgs", {})
        for imp, me_id in outputs.items():
            fname = str(location_id) + ".h5"
            out_df = gyne.me_dict[me_id].reset_index()
            out_df.to_hdf(os.path.join(out_dir, str(me_id), fname), 
                key="draws", format="table", data_columns=dim.keys())

##############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("me_map", help="json style dictionary of me_ids", 
        type=parsers.json_parser)
    parser.add_argument("location_id", help="which year to use", type=int)
    parser.add_argument("out_dir", help="root directory to save stuff")
    args = vars(parser.parse_args())
    
    # call function
    gyne_data(me_map=args["me_map"], location_id=args["location_id"], 
        out_dir=args["out_dir"])

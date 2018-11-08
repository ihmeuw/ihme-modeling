from __future__ import division
import os
import numpy as np
import pandas as pd
import argparse
import copy
import re
from job_utils import draws, parsers


class Split(draws.SquareImport):

    def __init__(self, me_map, **kwargs):
        # super init
        super(Split, self).__init__(**kwargs)

        # store data by me in this dict 
        # key=me_id
        self.me_map = me_map
        self.me_dict = {}

        #import every input and create a dictionary of dataframes
        for mapper_key, mapper in me_map.items():
            inputs = mapper.get("srcs",{})
            for src_key, me_id in inputs.items():
                if src_key == "profound_prop":
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

    def iodine_split(self):
        '''Splits 'Intellectual disability due to iodine deficiency, 
        adjusted' into 'Profound intellectual disability due to iodine 
        deficiency unsqueezed' and 'Severe intellectual disability due to 
        iodine deficiency unsqueezed' '''
        id_key = self.me_map["iod"]["srcs"]["tot"]
        prop_key = self.me_map["iod"]["srcs"]["profound_prop"]
        prof_key = self.me_map["iod"]["trgs"]["profound"]
        sev_key = self.me_map["iod"]["trgs"]["severe"]

        # broadcast prop to preferred shape
        broadcast_over = self.idx_dmnsns.keys()
        broadcast_over.remove('measure_id')
        prop = self.me_dict[prop_key].copy()
        prop = prop.reset_index()[broadcast_over + self.draw_cols]
        prop = pd.merge(self.index_df.reset_index(), prop, on=broadcast_over,
                      how="left").set_index(self.idx_dmnsns.keys())

        profound = self.me_dict[id_key] * prop
        severe = self.me_dict[id_key] * (1-prop)
        
        self.me_dict[prof_key] = profound
        self.me_dict[sev_key] = severe

##############################################################################
# function to run code
##############################################################################

def run(me_map, out_dir, year_id):
    # retrieve default dimensions needed to initialize draws.SquareImport class, 
    # then change as needed
    dim = Split.default_idx_dmnsns

    # production dimensions
    dim["year_id"] = [year_id]
    dim["measure_id"] = [5]

    # initialize instance of Congenital class
    iod = Split(me_map=me_map, idx_dmnsns=dim)
    iod.iodine_split()
    
    for mapper_key, mapper in me_map.items():
        outputs = mapper.get("trgs", {})
        for imp, me_id in outputs.items():
            fname = str(year_id) + ".h5"
            out_df = iod.me_dict[me_id].reset_index()
            out_df.to_hdf(os.path.join(out_dir, str(me_id), fname), 
                key="draws", format="table", data_columns=dim.keys())

##############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("--me_map", help="json style dictionary of me_ids", 
        type=parsers.json_parser)
    parser.add_argument("--out_dir", help="root directory to save stuff")
    parser.add_argument("--year_id", help="which year to use", type=int)
    args = vars(parser.parse_args())
    
    # call function
    run(me_map=args["me_map"], out_dir=args["out_dir"], year_id=args["year_id"])

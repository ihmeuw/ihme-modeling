from __future__ import division
import os
import numpy as np
import pandas as pd
import argparse
from job_utils import draws, parsers
import copy
import subprocess
from db_queries import get_demographics, get_location_metadata 
from ihme_dimensions import dimensionality, gbdize


class Iodine(draws.SquareImport):

    def __init__(self, me_map, **kwargs):
        # super init
        super(Iodine, self).__init__(**kwargs)

        # store data by me in this dict 
        # key=me_id, val=dataframe
        self.me_map = me_map
        self.me_dict = {}

        for mapper_key, mapper in me_map.items():
            input_meid = mapper.get("srcs")
            output_meid = mapper.get("trgs")
            self.me_dict[output_meid] = self.import_square(
                    gopher_what={"modelable_entity_id": input_meid},
                    source="epi")

    def copy_and_backfill(self):
        prof_id_cret_old = self.me_map["cretinism"]["srcs"]
        old = self.me_dict[prof_id_cret_old].reset_index()
        
        # Handle year differences between gbd2016 and gbd2017
        old.loc[old.year_id==2016,'year_id'] = 2017
        # Handle Saudia Arabia
        loc_meta = get_location_metadata(location_set_id=35, gbd_round_id=4)
        saudia_id = 152
        saudia_sub_nats = loc_meta.loc[loc_meta.parent_id==saudia_id,'location_id'].tolist()
        saudi_arabia = old.loc[old.location_id.isin(saudia_sub_nats),:]
        saudi_arabia.loc[:,'location_id'] = saudia_id
        saudi_arabia = saudi_arabia.drop_duplicates(keep='first')
        old = pd.concat([old,saudi_arabia],axis=0)
        
        # Handle other location differences between gbd2016 and gbd2017
        data_cols = self.draw_cols
        data_dct = {'data_cols': data_cols}
        index_cols = list(set(old.columns) - set(data_cols))
        index_cols.remove('location_id')
        demo = get_demographics(gbd_team='epi', gbd_round_id=5)
        index_dct = {
            tuple(index_cols): list(set(
                tuple(x) for x in old[index_cols].values)),
            'location_id': demo['location_id']
        }
        gbdizer = gbdize.GBDizeDataFrame(
            dimensionality.DataFrameDimensions(index_dct, data_dct))
        new = gbdizer.fill_location_from_nearest_parent(old, location_set_id=35, 
            gbd_round_id=5)
        prof_id_cret_new = self.me_map["cretinism"]["trgs"]
        self.me_dict[prof_id_cret_new] = new

##############################################################################
# function to run core code
##############################################################################

def run(me_map, year_id, out_dir):
    
    # retrieve default dimensions needed to initialize 
    # draws.SquareImport class, then change as needed
    dim = Iodine.default_idx_dmnsns

    # production dimensions
    if year_id==2017:
        dim["year_id"] = [2016]
    else:
        dim["year_id"] = [year_id]
    dim["measure_id"] = [18]

    # initialize instance of Iodine class
    iodine = Iodine(me_map=me_map, idx_dmnsns=dim)
    iodine.copy_and_backfill()

    for mapper_key, mapper in me_map.items():
        output_meid = mapper.get("trgs", {})
        fname = str(year_id) + ".h5"
        out_df = iodine.me_dict[output_meid].reset_index()
        out_df.to_hdf(os.path.join(out_dir, str(output_meid), fname), 
            key="draws", format="table", data_columns=dim.keys())

##############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("me_map", help="json style dictionary of me_ids", 
        type=parsers.json_parser)
    parser.add_argument("out_dir", help="root directory to save stuff")
    parser.add_argument("year_id", help="which year to use", 
        type=parsers.int_parser)
    args = vars(parser.parse_args())
    
    # call function
    run(me_map=args["me_map"], out_dir=args["out_dir"], year_id=args["year_id"])
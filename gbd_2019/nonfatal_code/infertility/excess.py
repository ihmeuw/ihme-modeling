import os
import pandas as pd
import argparse
from job_utils import draws, parsers
import copy

##############################################################################
# class to split excess
##############################################################################


class RedistExcess(draws.SquareImport):

    def __init__(self, excess_id, redist_map, **kwargs):
        # super init
        super(RedistExcess, self).__init__(**kwargs)

        # store data by me in this dict 
        # key=me_id, val=dataframe
        self.excess_id = excess_id
        self.redist_map = redist_map
        self.me_dict = {}

        # import every input
        for me_id in redist_map.keys() + [excess_id]:
            self.me_dict[me_id] = self.import_square(
                gopher_what={"modelable_entity_id": me_id},
                source="epi")


    def redistribute_excess(self):

        # sum up inputs
        sigma_props = pd.DataFrame()
        for me_id in self.redist_map.keys():
            sigma_props = pd.concat([sigma_props, self.me_dict[me_id]])
        sigma_props = sigma_props.groupby(level=self.idx_dmnsns.keys()).sum()

        # calculate excess.
        # redist + excess * (redist/sigma_props)
        for split_id, split_excess_id in self.redist_map.items():
            split = (self.me_dict[self.excess_id] * self.me_dict[split_id] /
                     sigma_props)
            split.fillna(value=0, inplace=True)
            split_excess = self.me_dict[split_id] + split
            self.me_dict[split_excess_id] = split_excess


##############################################################################
# function to run excess redistribution
##############################################################################


def redist_excess(excess_id, redist_map, copy_map, year_id, out_dir):

    # declare calculation dimensions
    dim = RedistExcess.default_idx_dmnsns
    dim["year_id"] = year_id
    dim["measure_id"] = [5]
    dim["sex_id"] = [2]

    # run redistribution
    redistributer = RedistExcess(excess_id, redist_map, idx_dmnsns=dim)
    redistributer.redistribute_excess()

    # copy dimensions of redistributer instance then update measure_id
    # so we can copy over incidence draws as directed in copy_map 
    dim_inc = copy.deepcopy(redistributer.idx_dmnsns)
    dim_inc["measure_id"] = [6] # incidence
    inc = draws.SquareImport(idx_dmnsns=dim_inc)
    inc_dict = {}

    for me_id in copy_map.keys():
        inc_dict[copy_map[me_id]] = inc.import_square(
        gopher_what={"modelable_entity_id": me_id},
        source="epi")

    # save to disk
    for me_id in redist_map.values():
        fname = str(year_id[0]) + ".h5"
        out_df = redistributer.me_dict[me_id].reset_index()
        if me_id in copy_map.values():
            inc_df = inc_dict[me_id].reset_index()
            out_df = pd.concat([out_df, inc_df])
        out_df.to_hdf(os.path.join(out_dir, str(me_id), fname), key="draws",
            format="table", data_columns=dim.keys())

##############################################################################
# when called as a script
##############################################################################

if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("--excess_id", help="me_id of infertility excess",
                        required=True, type=int)
    parser.add_argument("--redist_map", required=True,
                        help="json style string map of sources and targets",
                        type=parsers.json_parser)
    parser.add_argument("--copy_map", required=True,
                        help="json style string map of sources and targets",
                        type=parsers.json_parser)
    parser.add_argument("--out_dir", help="root directory to save stuff",
                        required=True)
    parser.add_argument("--year_id", help="which location to use",
                        type=parsers.int_parser, nargs="*", required=True)
    args = vars(parser.parse_args())

    # call function
    redist_excess(excess_id=args["excess_id"], out_dir=args["out_dir"],
                  year_id=args["year_id"], redist_map=args["redist_map"],
                  copy_map=args["copy_map"])


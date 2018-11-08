
import pandas as pd
import numpy as np
import os
import glob
import argparse
import sys

reload(sys)
sys.setdefaultencoding('utf8')


def combine_denominators(in_dir, out_dir, loc_list):
    #check that all the data is there
    print(loc_list)
    allFiles = glob.glob(os.path.join(in_dir, 
        "neonate_denom_with_mort_rate_gbd2017_*.csv"))
    loc_files = []
    for filename in allFiles:
        search = '_'
        underscore = [i for i, ltr in enumerate(filename) if ltr == '_']
        ext_idx = filename.index('.csv')
        loc_string = filename[underscore[-1]+1:ext_idx] 
        loc_files.append(int(loc_string))
    missing = set(loc_list) - set(loc_files)
    assert set(loc_files)==set(loc_list), "The following locations are missing: {}".format(str(missing))
    
    #combine all the data into one dataframe
    aggregate = pd.DataFrame()
    df_list = []
    for loc in loc_list:
        df = pd.read_csv(os.path.join(in_dir, 
            "neonate_denom_with_mort_rate_gbd2017_{l}.csv".format(l=loc)))
        df_list.append(df)

    aggregate = pd.concat(df_list)
    aggregate.to_csv(os.path.join(out_dir,
        "neonate_denom_with_mort_rate_gbd2017.csv"),
        index=False, encoding='utf8')


if __name__ == "__main__":
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("in_dir", help="the directory where the files are held")
    parser.add_argument("out_dir", 
        help="the directory where the output file should go")
    parser.add_argument('--loc_list', type=int, nargs='*', 
        help="the list of locations we are combining")
    args = vars(parser.parse_args())

    combine_denominators(in_dir=args["in_dir"], out_dir=args["out_dir"], 
        loc_list=args["loc_list"])
import pandas as pd
import numpy as np
import os
import glob
import argparse

def aggregate_causes(temp_dir, out_dir, fname, bundle_num):
    aggregate = pd.DataFrame()
    allFiles = glob.glob(os.path.join(temp_dir, "*.csv"))
    df_list = []
    for file_ in allFiles:
        df = pd.read_csv(file_)
        df_list.append(df)
        print file_

    aggregate = pd.concat(df_list)
    aggregate.to_csv(os.path.join(out_dir, fname),index=False,encoding='utf-8')
    assert len(aggregate.bundle_id.tolist()) == bundle_num


if __name__ == "__main__":
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("temp_dir", help="directory from which to grab stuff")
    parser.add_argument("out_dir", help="directory where final csv will go")
    parser.add_argument("fname", help="name of final csv file")
    parser.add_argument("bundle_num", 
        help="the number of bundle paths we should be aggregating", type=int)
    args = vars(parser.parse_args())

    aggregate_causes(temp_dir=args["temp_dir"], out_dir=args["out_dir"], 
        fname=args["fname"], bundle_num=args["bundle_num"])
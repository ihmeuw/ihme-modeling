import argparse
import glob
import pandas as pd

from HALE_summary import calc_summary


def output_summary(location, tmp, out_dir, summ_cols, wide, pct_change):
    
    file_list = glob.glob('{}/{}_*.csv'.format(tmp, location))

    draws = []
    for f in file_list:
        year_draws = pd.read_csv(f)
        draws.append(year_draws)
    draws = pd.concat(draws)
    
    calc_summary(draws, summ_cols, out_dir, location, wide, pct_change)


def run_summary(lt_tmp, lt_dir, yld_tmp, yld_dir, hale_tmp, hale_dir,
                location, pct_change):
    lt_summ_cols = ['mx', 'ax', 'qx', 'dx', 'Tx', 'nLx', 'lx', 'Ex']
    lt_args = (location, lt_tmp, lt_dir, lt_summ_cols, False, pct_change)

    yld_summ_cols = ['yld_rate']
    yld_args = (location, yld_tmp, yld_dir, yld_summ_cols, False, pct_change)

    hale_summ_cols = ['HALE']
    hale_args = (location, hale_tmp, hale_dir, hale_summ_cols, True,
                 pct_change)

    for args in [lt_args, yld_args, hale_args]:
        output_summary(*args)


if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--lt_tmp",
            help="lt tmp directory",
            default="DIRECTORY",
            type=str)
    parser.add_argument(
            "--lt_dir",
            help="lt summary directory",
            default="DIRECTORY",
            type=str)
    parser.add_argument(
            "--yld_tmp",
            help="yld tmp directory",
            default="DIRECTORY",
            type=str)
    parser.add_argument(
            "--yld_dir",
            help="yld summary directory",
            default="DIRECTORY",
            type=str)
    parser.add_argument(
            "--hale_tmp",
            help="HALE tmp directory",
            default="DIRECTORY",
            type=str)
    parser.add_argument(
            "--hale_dir",
            help="HALE summary directory",
            default="DIRECTORY",
            type=str)
    parser.add_argument(
            "--location",
            help="location",
            default=7,
            type=int)
    parser.add_argument(
            "--no_pct",
            help="don't calc pct change",
            action="store_false")

    args = parser.parse_args()
    lt_tmp = args.lt_tmp
    lt_dir = args.lt_dir
    yld_tmp = args.yld_tmp
    yld_dir = args.yld_dir
    hale_tmp = args.hale_tmp
    hale_dir = args.hale_dir
    location = args.location
    pct_change = args.no_pct

    run_summary(lt_tmp, lt_dir, yld_tmp, yld_dir, hale_tmp, hale_dir, location,
                pct_change)
import reshape
import argparse
import numpy as np
import os
import pandas as pd
from glob import glob


##############################
# CONFIG
##############################
indir = "{FILEPATH}"
outdir = "{FILEPATH}"
subtypes_file = (
    '/{FILEPATH}/priors/in_out_meid_map.xlsx')
outmap = pd.read_excel(subtypes_file, 'out_meids')

############################################################
############################################################
############################################################


# For each location...
def write_standard_files(location):

    anemia_files = glob("%s/%s_*.h5" % (indir, location))

    dfs = []
    for f in anemia_files:
        print 'Reading %s' % (f)
        df = pd.read_hdf(f, 'solution')
        dfs.append(df)
    dfs = pd.concat(dfs)
    dfs['draw'] = dfs.draw.astype(int)

    for hs in ['mild', 'moderate', 'severe']:

        print 'Running %s. Reshaping wide.' % (hs)
        wide_df = dfs[['location_id', 'year_id', 'sex_id', 'age_group_id',
                       'draw', 'subtype'] + [hs]]
        wide_df = reshape.wide(
                wide_df, value_var=hs, variable='draw', stub='draw_')

        # Resample up to 1000 draws... making sure not to sample null draws
        num_draws = wide_df.filter(like='draw').columns.size
        rand_sample = np.random.randint(num_draws, size=1000-num_draws)
        for i, j in enumerate(range(num_draws, 1000)):
            wide_df['draw_%s' % j] = wide_df['draw_%s' % rand_sample[i]]

        for subtype in wide_df.subtype.unique():
            print 'Subtype: %s' % (subtype)
            meid = outmap.ix[outmap.report_group == subtype,
                             'modelable_entity_id_%s' % hs].values[0]

            hs_dir = "%s/%s" % (outdir, meid)
            try:
                os.makedirs(hs_dir)
            except:
                pass
            wide_df[wide_df.subtype == subtype].to_hdf(
                    '%s/{MEASURE ID}_%s.h5' % (hs_dir, location),
                    'draws',
                    mode='w',
                    data_columns=[
                        'location_id', 'year_id', 'age_group_id', 'sex_id'],
                    format='table')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("location", help="location to write", type=str)
    args = parser.parse_args()
    write_standard_files(args.location)

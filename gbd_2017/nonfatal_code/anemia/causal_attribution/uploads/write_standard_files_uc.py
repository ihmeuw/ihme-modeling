import reshape
import argparse
import numpy as np
import os
import pandas as pd
from glob import glob

from get_draws.api import get_draws

##############################
# CONFIG
##############################
indir = "FILEPATH"
outdir = "FILEPATH"
subtypes_file = (
    'FILEPATH/in_out_meid_map.xlsx')
outmap = pd.read_excel(subtypes_file, 'out_meids')
inmap = pd.read_excel(subtypes_file, 'in_meids')

############################################################
############################################################
############################################################
draw_labs = []
for i in range(1000):
    draw_labs.append('draw_{i}'.format(i=i))

def make_uc_df(df):
    num = df._get_numeric_data()
    num[num < 0] =0
    df.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    df['agg'] = df[draw_labs].mean(axis=1)
    df = df[draw_labs].div(df['agg'], axis=0)
    df.fillna(0, inplace=True)
    return df

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

    for hs in [('mild', 10489), ('moderate', 10490), ('severe', 10491)]:

        print 'Running %s. Reshaping wide.' % (hs[0])
        wide_df = dfs[['location_id', 'year_id', 'sex_id', 'age_group_id',
                       'draw', 'subtype'] + [hs[0]]]
        wide_df = reshape.wide(
                wide_df, value_var=hs[0], variable='draw', stub='draw_')

        wide_df.fillna(0, inplace=True)
        num = wide_df._get_numeric_data()
        num[num < 0] = 0

        # Resample up to 1000 draws... making sure not to sample null draws
        num_draws = wide_df.filter(like='draw').columns.size
        rand_sample = np.random.randint(num_draws, size=1000-num_draws)
        for i, j in enumerate(range(num_draws, 1000)):
            wide_df['draw_%s' % j] = wide_df['draw_%s' % rand_sample[i]]

        for subtype in wide_df.subtype.unique():
            print 'Subtype: %s' % (subtype)
            meid = outmap.ix[outmap.report_group == subtype,
                             'modelable_entity_id_%s' % hs[0]].values[0]
            sub_df = wide_df[wide_df.subtype == subtype]
            sub_df.drop(['subtype'], axis=1, inplace=True)
            sub_df.set_index(['location_id', 'year_id', 'sex_id', 'age_group_id'], inplace=True)

            env_draws = get_draws('modelable_entity_id', hs[1], source='epi', location_id=location, gbd_round_id=5)
            env_uc = make_uc_df(env_draws)
            sub_df = pd.DataFrame(sub_df.values*env_uc.values, columns=draw_labs, index=sub_df.index)


            measid_inst = inmap.loc[inmap['report_group'] == subtype, 'use_incidence'].iloc[0]
            if measid_inst == 1:
                measid = 6
            else:
                measid = 5
            in_meid = inmap.loc[inmap['report_group'] == subtype, 'modelable_entity_id'].iloc[0]
            if in_meid != 'RESIDUAL':
                try:
                    me_draws = get_draws('modelable_entity_id', in_meid, source='epi', location_id=location, measure_id=measid, gbd_round_id=5)
                    me_uc = make_uc_df(me_draws)
                    sub_df = pd.DataFrame(sub_df.values*me_uc.values, columns=draw_labs, index=sub_df.index)
                except:
                    sub_df = sub_df

            hs_dir = "%s/%s" % (outdir, meid)
            try:
                os.makedirs(hs_dir)
            except:
                pass
            sub_df = sub_df.reset_index()
            sub_df.fillna(0, inplace=True)
            num = sub_df._get_numeric_data()
            num[num < 0] = 0
            sub_df.to_hdf(
                    '%s/5_%s.h5' % (hs_dir, location),
                    'draws',
                    mode='w',
                    data_columns=[
                        'location_id', 'year_id', 'age_group_id', 'sex_id'],
                    format='table')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("location", help="location to write", type=int)
    args = parser.parse_args()
    write_standard_files(args.location)

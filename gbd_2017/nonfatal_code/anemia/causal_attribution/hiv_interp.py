import pandas as pd
import numpy as np
import argparse
import os

from chronos.interpolate import interpolate

meids = [16317, 16318, 16319, 16320]

draws = []
for i in range(1000):
     draws.append('draw_{}'.format(i))

outdir = FILEPATH

def year_chunk_interp(start_year, end_year, locid, meid):
    df = interpolate(
        gbd_id_type = 'modelable_entity_id', 
        gbd_id=meid, 
        source='epi', 
        measure_id=18,
        location_id=locid, 
        reporting_year_start=start_year, 
        reporting_year_end=end_year, 
        status='best')
    if start_year != 1990:
        df = df[df.year_id != start_year]
    df = df[['location_id', 'year_id', 'age_group_id', 'sex_id']+draws]
    df['year_id'] = df['year_id'].astype(int)
    df.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    return df


def run_interpolations(locid):
    for meid in meids:
        interp_dir = "{0}/{1}/{2}/".format(outdir, 'hiv_prop_interp', meid)
        try:
            os.makedirs(interp_dir)
        except:
            pass
        year_sets = ((1990, 1995), (1995, 2000), (2000, 2005), (2005, 2010), (2010, 2017))
        dfs = []
        for (y1, y2) in year_sets:
            df = year_chunk_interp(y1, y2, locid, meid)
            dfs.append(df)
        dfs = pd.concat(dfs)
        dfs.reset_index(inplace=True)
        dfs.to_hdf(interp_dir + '{}.h5'.format(locid), key='draws', format='table',
                   data_columns=['location_id', 'year_id', 'age_group_id', 'sex_id'])
        print 'interpolated {0} for {1}'.format(meid, locid)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("locid", help="location id to use", type=int)
    args = parser.parse_args()
    run_interpolations(args.locid)
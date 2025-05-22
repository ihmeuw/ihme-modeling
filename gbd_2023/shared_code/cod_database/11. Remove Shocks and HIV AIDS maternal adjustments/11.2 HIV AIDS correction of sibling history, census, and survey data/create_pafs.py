import pandas as pd
import sys
import os
from get_draws.api import get_draws

this_dir = "FILEPATH"
repo_dir = "FILEPATH"
sys.path.append(repo_dir)
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders import get_current_location_hierarchy, cod_timestamp
CONF = Configurator('standard')

RELEASE_ID = 16

def main(year):
    year = int(year)
    current_date = cod_timestamp()[0:10]
    out_dir = "FILEPATH"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    diag_out_dir = os.path.join(out_dir, 'diagnostics')
    if not os.path.exists(diag_out_dir):
        os.makedirs(diag_out_dir)

    loc_df = get_current_location_hierarchy(
        location_set_version_id=CONF.get_id('location_set_version')
    ).query('most_detailed == 1')
    locations = loc_df['location_id'].to_list()
    columns = ['age_group_id'] + ['draw_' + str(x) for x in range(0, 1000)]
    rr_cols = []
    for i in range(0, 1000):
        rr_cols.append('rr_%d' % i)

    all_diag = pd.DataFrame()

    rr = pd.read_stata("FILEPATH")
    rr.drop('dummy', axis=1, inplace=True)
    rr = rr.reindex(range(0, 9))
    rr['age_group_id'] = range(7, 16)
    rr.fillna(method='ffill', axis=0, inplace=True)
    rr.set_index('age_group_id', inplace=True)
    cols = rr.columns

    for idx, i in enumerate(cols):
        rr.rename(columns={'%s' % i: 'draw_%s' % idx}, inplace=True)

    all_pafs = pd.DataFrame()
    for geo in locations:
        hiv_prev = get_draws(
            gbd_id_type='modelable_entity_id', gbd_id=9313, source='epi',
            sex_id=2, location_id=geo, year_id=year, status='best',
            age_group_id=range(7, 16), release_id=RELEASE_ID
        )

        hiv_prev = hiv_prev[columns]

        hiv_prev.set_index('age_group_id', inplace=True)

        hiv_prev.fillna(0, inplace=True)

        if (hiv_prev.values.any() < 0):
            raise ValueError(
                "ERROR"
            )
            continue
        elif (hiv_prev.values.any() > 1):
            raise ValueError(
                "ERROR"
            )
            continue

        pafs = (hiv_prev * (rr - 1)) / ((hiv_prev * (rr - 1)) + 1)

        if (pafs.values.any() < 0):
            raise ValueError(
                "ERROR")
            continue
        elif (pafs.values.any() > 1):
            raise ValueError(
                "ERROR")
            continue

        pafs['location_id'] = geo
        pafs['year'] = year
        all_pafs = all_pafs.append(pafs)

        diagnostics = pafs.copy()
        diagnostics['paf_mean'] = diagnostics.mean(axis=1).to_frame()
        if diagnostics.paf_mean.any() >= .01:
            diagnostics['add'] = 0
        else:
            diagnostics['add'] = 1
        diagnostics['location_id'] = geo

        diagnostics.reset_index(inplace=True)
        all_diag = all_diag.append(diagnostics)

    all_pafs.fillna(0, inplace=True)
    all_pafs.reset_index(inplace=True)
    all_pafs.to_hdf("FILEPATH")

    all_diag.to_csv("FILEPATH")


if __name__ == "__main__":
    year = sys.argv[1]
    main(year)

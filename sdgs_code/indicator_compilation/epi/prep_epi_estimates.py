import pandas as pd
import os
import sys

from transmogrifier.draw_ops import interpolate

from getpass import getuser

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry


def collapse_demog(df, id_var, id):
    """Convert prevalence to cases"""
    pops = qry.get_pops(both_sexes=False)
    df = df.merge(pops, how = 'left', on = ['location_id','age_group_id','sex_id','year_id'])

    draws = [col for col in df.columns if 'draw_' in col]
    id_cols = dw.EPI_GROUP_COLS
    # make sex 3 to collapse to both
    df[id_var] = id
    # convert to cases by multiplying each draw by the population value
    df = pd.concat([df[id_cols],
                    df[draws].apply(lambda x: x * df['population']),
                    df['population']
                    ], axis=1
                   )
    # sum sexes together
    df = df.groupby(id_cols, as_index=False)[draws + ['population']].sum()
    # Turn back to proportion
    df = pd.concat([
        df[id_cols],
        df[draws].apply(lambda x: x / df['population'])
    ], axis=1
    )
    return df

def age_standardize(df):
    """ Age standardize. """

    age_weights = qry.get_age_weights(4)
    age_weights = age_weights[['age_group_id', 'age_group_weight_value']]

    df = df.merge(age_weights, on=['age_group_id'], how='left')
    assert df.age_group_weight_value.notnull().values.all(), 'age weights merg'

    # make weighted product and sum
    df = pd.concat(
        [
            df[dw.EPI_GROUP_COLS],
            df[dw.DRAW_COLS].apply(lambda x: x * df['age_group_weight_value'])
        ],
        axis=1
    )
    df['age_group_id'] = 27
    df = df.groupby(dw.EPI_GROUP_COLS, as_index=False)[dw.DRAW_COLS].sum()
    return df


def epi_data_grabber(id, measure, locs):
    """Get draws call for DisMod models"""
    if id == 1620116202:
        df1 = interpolate(gbd_id_field='modelable_entity_id', gbd_id=16201,
                         source='epi',
                         reporting_year_start=1990, reporting_year_end=2016,
                         location_ids=locs, age_group_ids=[], sex_ids=[],
                         measure_ids=[measure])
        df2 = interpolate(gbd_id_field='modelable_entity_id', gbd_id=16202,
                         source='epi',
                         reporting_year_start=1990, reporting_year_end=2016,
                         location_ids=locs, age_group_ids=[], sex_ids=[],
                         measure_ids=[measure])
        df = df1.append(df2)
        df['modelable_entity_id'] = id
        del df1, df2
    elif id == 10556:
        df = interpolate(gbd_id_field='modelable_entity_id', gbd_id=id,
                         source='epi', version=dw.CHILD_STUNTING_MODEL,
                         reporting_year_start=1990, reporting_year_end=2016,
                         location_ids=locs, age_group_ids=[], sex_ids=[],
                         measure_ids=[measure])
    elif id == 10558:
        df = interpolate(gbd_id_field='modelable_entity_id', gbd_id=id,
                         source='epi', version=dw.CHILD_WASTING_MODEL,
                         reporting_year_start=1990, reporting_year_end=2016,
                         location_ids=locs, age_group_ids=[], sex_ids=[],
                         measure_ids=[measure])
    else:
        df = interpolate(gbd_id_field='modelable_entity_id', gbd_id=id,
                         source='epi',
                         reporting_year_start=1990, reporting_year_end=2016,
                         location_ids=locs, age_group_ids=[], sex_ids=[],
                         measure_ids=[measure])
    df = df.query('age_group_id in {} and sex_id in [1, 2]'.format(range(2, 21) + range(30, 33) + [235]))
    if measure == 18:
        df['metric_id'] = 2
    else:
        df['metric_id'] = 3 # This will not always be true, update if add something other than prev/inc
    if len(df.sex_id.unique() > 1):
        df = collapse_demog(df, 'sex_id', 3)
    if id == 10344:
        df = df.query('age_group_id == 5') # just keep 1-4 for childhood obesity(?)
    if id in [10556, 10558]:
        df = df.query('age_group_id in [2, 3, 4, 5]') # just keep under-5 for stunting and wasting
        df = collapse_demog(df, 'age_group_id', 1)
    if id in [10820, 10475]:
        df = df.query('age_group_id in age_group_id >= 8') # just keep 15+ for lifetime sexual violence indicators
        df = collapse_demog(df, 'age_group_id', 29)
    if id == 10817 or id in [10829, 10830, 10831, 10832, 10833, 10834]:
        df = age_standardize(df)
    if id == 1620116202:
        df = df.query('age_group_id in [9, 10]') # just keep 20-29 for CSA
        df = collapse_demog(df, 'age_group_id', 202)
    assert len(df.age_group_id.unique() == 1), 'multiple age groups remain'
    df.to_hdf(dw.EPI_DIR + "/{}.h5".format(id),
              key="data",
              format="table", data_columns=['location_id', 'year_id', 'age_group_id'])


if __name__ == "__main__":
    """Store epi draws"""
    try:
        if not os.path.exists(dw.EPI_DIR):
            os.makedirs(dw.EPI_DIR)
    except OSError:
        pass
    locsdf = qry.get_sdg_reporting_locations()
    locs = list(locsdf['location_id'])
    for id, measure in zip(dw.EPI_ME_IDS, dw.EPI_MEASURES):
        print("ID: {}".format(id))
        print("Measure: {}".format(measure))
        epi_data_grabber(id, measure, locs)

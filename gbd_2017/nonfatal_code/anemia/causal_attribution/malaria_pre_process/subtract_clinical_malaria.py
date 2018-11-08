import pandas as pd
import argparse
from get_draws.api import get_draws

#Parallelized by location
def get_the_draws(meid, location):
    df = get_draws(
        'modelable_entity_id',
        meid,
        'epi',
        location_id=location,
        measure_id=5,
        metric_id=3,
        gbd_round_id = 5)
    df.drop(['measure_id', 'metric_id', 'modelable_entity_id', 'model_version_id'], axis=1, inplace=True)
    df = df.sort_values(by=['location_id', 'year_id', 'age_group_id', 'sex_id'])
    df.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    return df

def make_sum_parasitemia(df1, df2):
    total = df1.add(df2)
    return total

def make_prop(numerator, denominator):
    df = numerator.div(denominator, fill_value=0)
    df = df.fillna(0)
    return df

def make_parasitemia_noclin(parasitemia, clindf, prop, out_me, lid):
    propclin = clindf * prop
    df = parasitemia.subtract(propclin)
    num = df._get_numeric_data()
    num[num < 0] = 0
    df.to_csv('FILEPATH/{0}/{1}.csv'.format(out_me, lid))

def make_pfpr_pvpr_draws(location):
    pfpr = get_the_draws(3263, location)
    pvpr = get_the_draws(18682, location)
    clinical_malaria = get_the_draws(3055, location)
    sum_parasitemia = make_sum_parasitemia(pfpr, pvpr)
    pf_prop = make_prop(pfpr, sum_parasitemia)
    pv_prop = make_prop(pvpr, sum_parasitemia)
    make_parasitemia_noclin(pfpr, clinical_malaria, pf_prop, 19390, location)
    make_parasitemia_noclin(pvpr, clinical_malaria, pv_prop, 19394, location)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("location", help="location id to use", type=int)
    args = parser.parse_args()
    make_pfpr_pvpr_draws(args.location)
from argparse import ArgumentParser, Namespace
import os
import pandas as pd

from get_draws.api import get_draws


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument('--location_id', type=int)
    parser.add_argument('--gbd_round_id', type=int)
    parser.add_argument('--decomp_step', type=str)
    parser.add_argument('--out_dir', type=str)
    return parser.parse_args()


def get_the_draws(modelable_entity_id, location_id, gbd_round_id,
                  decomp_step) -> pd.DataFrame:
    """ Parallelized by location """
    df = get_draws('modelable_entity_id', modelable_entity_id, 'epi',
                   location_id=location_id, measure_id=5, metric_id=3,
                   gbd_round_id=gbd_round_id, decomp_step=decomp_step)
    df.drop(['measure_id', 'metric_id', 'modelable_entity_id',
             'model_version_id'], axis=1, inplace=True)
    df = df.sort_values(by=['location_id', 'year_id', 'age_group_id', 'sex_id'])
    df.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    return df


def make_sum_parasitemia(df1, df2) -> pd.DataFrame:
    total = df1.add(df2)
    return total


def make_prop(numerator, denominator) -> pd.DataFrame:
    df = numerator.div(denominator, fill_value=0)
    df = df.fillna(0)
    return df


def make_parasitemia_noclin(parasitemia, clindf, prop, out_me, lid,
                            out_dir) -> None:
    propclin = clindf * prop
    df = parasitemia.subtract(propclin)
    num = df._get_numeric_data()
    num[num < 0] = 0
    me_out_dir = 'FILEPATH'
    if not os.path.isdir(me_out_dir):
        os.system('FILEPATH')
    df.to_csv("FILEPATH")


def make_pfpr_pvpr_draws(location_id, gbd_round_id, decomp_step,
                         out_dir) -> None:
    pfpr = get_the_draws(3263, location_id, gbd_round_id, decomp_step)
    pvpr = get_the_draws(18682, location_id, gbd_round_id, decomp_step)
    clinical_malaria = get_the_draws(3055, location_id, gbd_round_id, decomp_step)
    sum_parasitemia = make_sum_parasitemia(pfpr, pvpr)
    pf_prop = make_prop(pfpr, sum_parasitemia)
    pv_prop = make_prop(pvpr, sum_parasitemia)
    make_parasitemia_noclin(pfpr, clinical_malaria, pf_prop, 19390, location_id, out_dir)
    make_parasitemia_noclin(pvpr, clinical_malaria, pv_prop, 19394, location_id, out_dir)


if __name__ == "__main__":
    args = parse_args()
    make_pfpr_pvpr_draws(
        location_id=args.location_id,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step,
        out_dir=args.out_dir)

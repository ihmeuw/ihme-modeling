"""
split_interp.py is run on one sex-modelable_entity_id. It outputs a hdf
of draws from the given arguments to chronos interpolate from 1980 to
the last esimation year in the given gbd_round.

Command Line Arguments:
    --gbd_id (int): modelable_entity_id to be provided to chronos.interpolate
    --proportion_measure_id (int): measure_id to be provided to
        chronos.interpolate. Used in the case where a given me_id proportion
        uses a different measure other than the default, 
    --sex_id (int): passed to chronos.interpolate
    --gbd_round_id (int): passed to chronos.interpolate
    --intermediate_dir (str): passed to chronos.interpolate
    --decomp_step (str, default=None): passed to chronos.interpolate
"""

import argparse
import os

from cluster_utils.io import makedirs_safely
from gbd.estimation_years import estimation_years_from_gbd_round_id

from chronos.interpolate import interpolate


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--gbd_id', type=int)
    parser.add_argument('--proportion_measure_id', type=int)
    parser.add_argument('--sex_id', type=int)
    parser.add_argument('--gbd_round_id', type=int)
    parser.add_argument('--intermediate_dir', type=str)
    parser.add_argument('--decomp_step', type=str, default=None)

    args = parser.parse_args()
    return (args.gbd_id, args.proportion_measure_id, args.gbd_round_id,
            args.sex_id, args.intermediate_dir, args.decomp_step)


def main():
    (gbd_id, measure_id, gbd_round_id, sex_id,
     outdir, decomp_step) = parse_arguments()

    if not os.path.exists(outdir):
        makedirs_safely(outdir)

    end_year = _get_end_year_argument_from_gbd_round(gbd_round_id)

    df = interpolate(gbd_id=gbd_id,
                     gbd_id_type='modelable_entity_id',
                     source='epi',
                     measure_id=measure_id,
                     reporting_year_start=1980,
                     reporting_year_end=end_year,
                     sex_id=sex_id,
                     gbd_round_id=gbd_round_id,
                     decomp_step=decomp_step,
                     num_workers=30)

    id_cols = [col for col in df.columns if col.endswith('_id')]
    for col in id_cols:
        df[col] = df[col].astype('int64')

    df.to_hdf(
        os.path.join(outdir, 'interp_{}_{}.h5'.format(gbd_id, sex_id)),
        key='draws',
        mode='w',
        format='table',
        data_columns=['location_id', 'year_id', 'age_group_id', 'sex_id']
    )


def _get_end_year_argument_from_gbd_round(gbd_round_id: int) -> int:
    """
    We want to extrapolate for all estimation years, so we return the last
    estimation year in the given gbd_round.
    """
    return sorted(estimation_years_from_gbd_round_id(gbd_round_id))[-1]


if __name__ == '__main__':
    main()

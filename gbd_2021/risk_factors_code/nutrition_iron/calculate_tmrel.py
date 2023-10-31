import pandas as pd
import scipy.stats as sp
import os
from pathlib import Path
from argparse import ArgumentParser, Namespace
from get_draws.api import get_draws
from db_queries import get_demographics


years = [1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022]
ages = [2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 34, 32, 235, 238, 388, 389]
sexes = [1, 2]

run_dir = 'FILEPATH'


outmap = pd.read_excel('FILEPATH/in_out_meid_map_2020.xlsx', 'out_meids')
id_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'subtype']
fe_responsive = outmap['subtype'].loc[outmap['iron_responsive']==1].tolist()
fe_non_responsive = outmap['subtype'].loc[outmap['iron_responsive']==0].tolist()

hb_shifts = pd.read_csv('FILEPATH/hb_shifts_2020.csv')

print(fe_responsive)

def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--loc_id", help="location to use", type=int)
    parser.add_argument("--gbd_round_id", type = int)
    parser.add_argument("--decomp_step", type = str)
    return parser.parse_args()


def get_mean_hemog(loc_id, gbd_round_id, decomp_step):
    exp_df = get_draws('modelable_entity_id', 10487, 'epi', location_id=loc_id, year_id=years, age_group_id = ages, sex_id = sexes,
    gbd_round_id=gbd_round_id, decomp_step=decomp_step)

    exp_df.drop(['measure_id', 'metric_id', 'model_version_id', 'modelable_entity_id'], axis=1, inplace=True)

    exp_df.sort_values(by=['age_group_id', 'location_id', 'sex_id', 'year_id'], inplace=True)
    return(exp_df)


def get_prevalence_draws(location, meid, gbd_round_id, decomp_step):
    measure = 5

    prevalence_draws = get_draws('modelable_entity_id', meid, 'epi', location_id=location, year_id=years, age_group_id = ages, sex_id = sexes, gbd_round_id=gbd_round_id, decomp_step=decomp_step, measure_id = measure)
    prevalence_draws.drop(['measure_id', 'metric_id', 'model_version_id', 'modelable_entity_id'], axis=1, inplace=True)
    prevalence_draws.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)

    return prevalence_draws

def get_out_meids(report_group):
    df = outmap[outmap['subtype']==report_group]
    print(df.head())

    return (df.iloc[0]['meid_mild'],
            df.iloc[0]['meid_moderate'],
            df.iloc[0]['meid_severe'])

def get_subtype_prevalence(location, report_group, gbd_round_id, decomp_step):
    out_ids = get_out_meids(report_group)
    mild = get_prevalence_draws(location, out_ids[0], gbd_round_id, decomp_step)
    mild['severity'] = "mild"
    moderate = get_prevalence_draws(location, out_ids[1], gbd_round_id, decomp_step)
    moderate['severity'] = "moderate"
    severe = get_prevalence_draws(location, out_ids[2], gbd_round_id, decomp_step)
    severe['severity'] = "severe"

    mild_mod = mild.add(moderate)
    total = mild_mod.add(severe)

    total = total.reset_index()
    total['subtype'] = report_group
    cause_name = str(report_group)

    return total

def compile_report_group_totals(location, gbd_round_id, decomp_step):
    totals = []
    for rg in fe_responsive:
        total = get_subtype_prevalence(location, rg, gbd_round_id, decomp_step)
        totals.append(total)
        print("Totaled {}.".format(rg))
    totals = pd.concat(totals, axis=0)
    totals = totals[totals['year_id'].isin([1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022])]
    return totals

def make_shiftprev_draws(location, gbd_round_id, decomp_step, mean_df):
    prevdf = compile_report_group_totals(location, gbd_round_id, decomp_step)

    shift_df = prevdf.merge(hb_shifts, on = ['sex_id', "subtype"])

    shift_df.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    shiftprev = shift_df[['draw_%s' % d for d in range(1000)]].multiply((shift_df['mean_hb_shift']), axis="index")

    shiftprev.reset_index()

    grouped_df = shiftprev.groupby(['age_group_id', 'location_id', 'sex_id', 'year_id']).sum().reset_index()

    all_locs = grouped_df['location_id'].tolist()
    all_sexes = grouped_df['sex_id'].tolist()
    all_ages = grouped_df['age_group_id'].tolist()
    all_years = grouped_df['year_id'].tolist()

    grouped_df.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    mean_df.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)

    final_df = grouped_df + mean_df

    final_df['location_id'] = all_locs
    final_df['sex_id'] = all_sexes
    final_df['age_group_id'] = all_ages
    final_df['year_id'] = all_years
    print(final_df['location_id'])

    print(mean_df.head())
    return final_df


if __name__ == "__main__":
    args = parse_args()
    mean_hemog = get_mean_hemog(
        loc_id = args.loc_id,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step)
    shiftprev_df = make_shiftprev_draws(
        location = args.loc_id,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step,
        mean_df = mean_hemog)
    out_dir = os.path.join(run_dir, 'iron_deficiency', 'tmrel')
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    shiftprev_df.to_csv(f'{out_dir}/{args.loc_id}.csv', index = False)

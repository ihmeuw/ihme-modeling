from argparse import ArgumentParser, Namespace
import datetime
import pandas as pd
from get_draws.api import get_draws
from glob import glob


# Setting file locations
#indir = "FILEPATH"
outmap = pd.read_excel('FILEPATH', 'out_meids')
id_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'subtype']
fe_responsive = outmap['subtype'].loc[outmap['iron_responsive']==1].tolist()
fe_non_responsive = outmap['subtype'].loc[outmap['iron_responsive']==0].tolist()
every_subtype = outmap['subtype'].tolist()
all_but_ni = outmap['subtype'].tolist()
all_but_ni.remove(u'nutrition_iron')

def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--location", help="location id to use", type=int)
    parser.add_argument("--indir", help="input directory for ideal hgb", type=str)
    parser.add_argument("--year_id", nargs='*', type=int)
    parser.add_argument("--gbd_round_id", type=int)
    parser.add_argument("--decomp_step", type=str)
    return parser.parse_args()


# For each location...
def get_tmrel_draws(location):
    ideal_hgb_file=('FILEPATH')
    tmrel_draws = pd.read_hdf(
        ideal_hgb_file,
        where="location_id=={l}".format(
            l=location), index=False)
    renames = {'tmrel_%s' % d: 'draw_%s' % d for d in range(1000)}
    tmrel_draws.rename(columns=renames, inplace=True)
    tmrel_draws.sort_values(by=['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    tmrel_draws.drop(['risk', 'parameter'], axis=1, inplace=True)
    tmrel_draws.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    return tmrel_draws

def get_prevalence_draws(location, meid, year_id, gbd_round_id, decomp_step):
    prevalence_draws = get_draws('modelable_entity_id', meid, 'epi', location_id=location, year_id=year_id, gbd_round_id=gbd_round_id, decomp_step=decomp_step)
    prevalence_draws.drop(['measure_id', 'metric_id', 'model_version_id', 'modelable_entity_id'], axis=1, inplace=True)
    prevalence_draws.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    return prevalence_draws

def get_out_meids(report_group):
    df = outmap[outmap['subtype']==report_group]
    return (df.iloc[0]['modelable_entity_id_mild'],
            df.iloc[0]['modelable_entity_id_moderate'],
            df.iloc[0]['modelable_entity_id_severe'])

def get_subtype_prevalence(location, report_group, year_id, gbd_round_id, decomp_step):
    out_ids = get_out_meids(report_group)
    mild = get_prevalence_draws(location, out_ids[0], year_id, gbd_round_id, decomp_step)
    moderate = get_prevalence_draws(location, out_ids[1], year_id, gbd_round_id, decomp_step)
    severe = get_prevalence_draws(location, out_ids[2], year_id, gbd_round_id, decomp_step)
    total = mild + moderate + severe
    total = total.reset_index()
    total['subtype'] = report_group
    return total

def compile_report_group_totals(location, year_id, gbd_round_id, decomp_step):
    totals = []
    for rg in outmap.subtype.unique():
        total = get_subtype_prevalence(location, rg, year_id, gbd_round_id, decomp_step)
        totals.append(total)
        print("Totaled {}.".format(rg))
    totals = pd.concat(totals, axis=0)
    totals = totals[totals['year_id'].isin([1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019])]
    totals.set_index(id_cols, inplace=True)
    return totals

def make_shiftprev_draws(location, year_id, gbd_round_id, decomp_step, norm_df):
    prevdf = compile_report_group_totals(location, year_id, gbd_round_id, decomp_step)
    mean_hgb = get_draws('modelable_entity_id', 10487, 'epi', location_id=location, year_id=year_id, gbd_round_id=gbd_round_id, decomp_step=decomp_step)
    mean_hgb.sort_values(by=['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    mean_hgb.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    shiftprev = prevdf[['draw_%s' % d for d in range(1000)]].multiply((norm_df - mean_hgb), axis="index")
    shiftprev = shiftprev.reset_index()
    return shiftprev

def make_filtered_df(shiftprev, filter_list):
    df = shiftprev[shiftprev['subtype'].isin(filter_list)]
    df = df.groupby(['location_id', 'year_id', 'age_group_id', 'sex_id']).sum()
    return df

def subtract_from_counterfactual(tmrel_df, shiftprev_df):
    expdf = tmrel_df.subtract(shiftprev_df)
    return expdf

def calculate_exposures(shiftprev, filter_list, tmrel_df, rei, location):
    shiftprev_df = make_filtered_df(shiftprev, filter_list)
    expdf = subtract_from_counterfactual(tmrel_df, shiftprev_df)
    expdf['parameter'] = 'mean'
    expdf['measure_id'] = 19
    expdf['risk'] = rei
    expdf['metric_id'] = 3
    expdf.drop(['modelable_entity_id'], axis=1, inplace=True)
    expdf.to_csv('FILEPATH')

def populate_exposures(location, year_id, gbd_round_id, decomp_step):
    tmrel_df = get_tmrel_draws(location)
    total_shiftprev = make_shiftprev_draws(location, year_id, gbd_round_id, decomp_step, norm_df=tmrel_df)
#    calculate_exposures(total_shiftprev, fe_responsive, tmrel_df, "low_hgb_iron_responsive", location)
#    calculate_exposures(total_shiftprev, fe_non_responsive, tmrel_df, "other_iron", location)
#    calculate_exposures(total_shiftprev, every_subtype, tmrel_df, "low_hgb", location)
    calculate_exposures(total_shiftprev, all_but_ni, tmrel_df, "nutrition_iron", location)


if __name__ == "__main__":
    args = parse_args()
    populate_exposures(
        location=args.location,
        year_id=args.year_id,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step)

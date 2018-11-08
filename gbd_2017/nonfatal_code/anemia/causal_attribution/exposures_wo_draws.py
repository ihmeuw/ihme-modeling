import argparse
import pandas as pd
from get_draws.api import get_draws
from glob import glob

# Setting file locations
indir = FILEPATH
drawdir = FILEPATH
exp_dir = FILEPATH
outmap = pd.read_excel(FILEPATH/in_out_meid_map.xlsx, 'out_meids')
fe_non_responsive = outmap['report_group'].loc[outmap['iron_responsive']==0].tolist()
every_subtype = outmap['report_group'].tolist()


# For each location...
def get_counterfactual_draws(location):
    ideal_hgb_file=(FILEPATH)
    counterfactual_draws = pd.read_hdf(
        ideal_hgb_file,
        where="location_id=={l}".format(
            l=location), index=False)
    renames = {'tmrel_%s' % d: 'draw_%s' % d for d in range(1000)}
    counterfactual_draws.rename(columns=renames, inplace=True)
    counterfactual_draws.sort_values(by=['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    counterfactual_draws.drop(['risk', 'parameter'], axis=1, inplace=True)
    counterfactual_draws.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    return counterfactual_draws

def get_prevalence_draws(location, meid):
    prevalence_draws = pd.read_hdf('{0}/{1}/5_{2}.h5'.format(drawdir, meid, location))
    prevalence_draws.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'], inplace=True)
    return prevalence_draws

def combine_shift_files(location):
    anemia_files = glob("%s/%s_*.h5" % (indir, location))
    dfs = []
    for f in anemia_files:
        df = pd.read_hdf(f)
        df = df[df.draw == 0]
        df.drop(['draw', 'mild', 'moderate', 'severe', 'prior_hb_shift', 'prior_subtype_prop'],
                axis=1,
                inplace=True
                )
        dfs.append(df)
    dfs_appended = pd.concat(dfs, axis=0)
    dfs_appended.set_index(id_cols, inplace=True)
    return dfs_appended

def get_out_meids(report_group):
    df = outmap[outmap['report_group']==report_group]
    return (df.iloc[0]['modelable_entity_id_mild'],
            df.iloc[0]['modelable_entity_id_moderate'],
            df.iloc[0]['modelable_entity_id_severe'])

def get_subtype_prevalence(location, report_group):
    out_ids = get_out_meids(report_group)
    mild = get_prevalence_draws(location, out_ids[0])
    moderate = get_prevalence_draws(location, out_ids[1])
    severe = get_prevalence_draws(location, out_ids[2])
    total = mild + moderate + severe
    total = total.reset_index()
    total['subtype'] = report_group
    return total

def compile_report_group_totals(location):
    totals = []
    for rg in outmap.report_group.unique():
        total = get_subtype_prevalence(location, rg)
        totals.append(total)
        print "Totaled {}.".format(rg)
    totals = pd.concat(totals, axis=0)
    totals.set_index(id_cols, inplace=True)
    return totals

def make_shiftprev_draws(location):
    prevdf = compile_report_group_totals(location)
    shiftdf = combine_shift_files(location)
    shiftprev = prevdf[['draw_%s' % d for d in range(1000)]].multiply(shiftdf['observed_hb_shift'], axis='index')
    shiftprev = shiftprev.reset_index()
    return shiftprev

def make_filtered_df(shiftprev, filter_list):
    df = shiftprev[shiftprev['subtype'].isin(filter_list)]
    df = df.groupby(['location_id', 'year_id', 'age_group_id', 'sex_id']).sum()
    return df

def subtract_from_counterfactual(counterfactual_df, shiftprev_df):
    expdf = counterfactual_df.subtract(shiftprev_df)
    return expdf

def calculate_exposures(shiftprev, filter_list, counterfactual_df, rei, location):
    shiftprev_df = make_filtered_df(shiftprev, filter_list)
    expdf = subtract_from_counterfactual(counterfactual_df, shiftprev_df)
    expdf['parameter'] = 'mean'
    expdf['measure_id'] = 19
    expdf['risk'] = rei
    expdf.to_csv('FILEPATH/{0}/{1}.csv'.format(rei, location))


def populate_exposures(location):
    counterfactual_df = get_counterfactual_draws(location)
    total_shiftprev = make_shiftprev_draws(location)
    calculate_exposures(total_shiftprev, fe_non_responsive, counterfactual_df, "other_iron", location)
    calculate_exposures(total_shiftprev, every_subtype, counterfactual_df, "low_hgb", location)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("location", help="location id to use", type=int)
    args = parser.parse_args()
    populate_exposures(args.location)
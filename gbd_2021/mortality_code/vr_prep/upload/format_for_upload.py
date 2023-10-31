"""
This script reads in all compiled mortality empirical deaths data and converts
it from DATUM format into standard GBD format.
"""

import sys
import getpass
import pandas as pd
from db_queries import get_location_metadata
sys.path.append("".format(getpass.getuser()))
from convert_datum_to_bearbones import convert_datum_to_bearbones

# Globals / passed in arguments
NEW_RUN_ID = sys.argv[1]
GBD_ROUND_ID = sys.argv[2]
OUTPUT_FOLDER = "".format(NEW_RUN_ID)


def main():
    df = pd.read_stata("".format(OUTPUT_FOLDER))
    final = convert_datum_to_bearbones(df)
    
    # Special case: USA 1972 VR Data is not representative of the population for this one NID. Scale all age groups appropriately
    final.loc[(final.nid == '157276') & (final.year_id==1972), 'deaths'] = final['deaths'] * 2

    final["precise_year"] = final["year_id"]

    # Split off norway data and rescale
    locs = get_location_metadata(35, gbd_round_id=7)
    norway_locs = locs.loc[locs.ihme_loc_id.str.contains("NOR")].location_id

    nor_df = final.loc[(final.location_id.isin(norway_locs)) & (final.outlier==0) & (final.year_id < 2018)] # No national data for 2018
    nor_scaled = rescale_norway(nor_df)

    # Concatenate back together
    final = pd.concat([final.iloc[final.index.difference(nor_df.index), :], nor_scaled], sort=True)
    
    # replace ukr 
    final = get_UKR_aggregations(final)

    final = final.rename(columns={'deaths': 'mean',
                          'source': 'detailed_source'})

    print("Saving data...")
    data_save_file = OUTPUT_FOLDER + ""
    final.to_csv(data_save_file, index=False)
    print("Finished.\nData written to \n{}.".format(data_save_file))

def rescale_norway(nor_df):

    """
    """
    nat = nor_df.loc[(nor_df.location_id==90) & (nor_df.deaths > 0)]
    subnat = nor_df.loc[(nor_df.location_id!=90) & (nor_df.deaths > 0)]

    # Scalars computation
    scalars = subnat.groupby(['age_group_id', 'year_id', 'sex_id']).deaths.sum().reset_index()
    scalars = pd.merge(scalars, nat, on=['age_group_id', 'year_id', 'sex_id'], suffixes=['_subnat', '_hmd'])
    scalars['scaling_factor'] = scalars['deaths_hmd'] / scalars['deaths_subnat']
    scalars = scalars[['age_group_id', 'year_id', 'sex_id', 'scaling_factor']]

    # Merge on scalars to subnationals and scale
    subnat_scaled = pd.merge(subnat, scalars, on=['age_group_id', 'year_id', 'sex_id'])
    subnat_scaled['deaths'] = subnat_scaled['deaths'] * subnat_scaled['scaling_factor']

    # Append onto Norway national and return
    output = pd.concat([subnat_scaled.drop(columns="scaling_factor"), nat], sort=True)

    return(output)

def get_UKR_aggregations(final):
    """
    """
    # create dataset of UKR subnationals
    # 50559: Ukraine (without Crimea and Sevastopol), 44934: Crimea, 44939: Sevastopol
    ukr_subnats = final.loc[(final["location_id"].isin([50559, 44934, 44939])) & (final["outlier"] == 0)].copy()
    # count subnational locations for each location - year - source_type_id
    ukr_subnats["subnat_count"] = ukr_subnats.groupby(by=["year_id", "source_type_id"])['location_id'].transform("nunique")
    # keep data with all subnats present
    ukr_subnats = ukr_subnats[ukr_subnats.subnat_count == 3]
    # set parent ID
    ukr_subnats["parent_id"] = 63

    # Aggregate subnationals
    # Make sure nids/sources aren't being dropped
    def safe_agg(values):
        """Returns 1st value in list of unique values in a column"""
        values = values.unique().tolist()
        return(values[0])

    # aggregation up to the next location level.
    ukr_subnats = ukr_subnats.groupby(["age_group_id","sex_id", "source_type_id", "year_id", "precise_year", "parent_id"], as_index=False).agg({"deaths": sum, "nid": safe_agg, "source": safe_agg, "outlier": safe_agg, "underlying_nid": safe_agg})
    # Replace the old location_id with the parent_id
    ukr_subnats.rename(columns={"parent_id": "location_id"}, inplace=True)

    # Subset to only all ages and all sexes
    ukr_subnats_compare = ukr_subnats[(ukr_subnats["age_group_id"] == 22) & (ukr_subnats["sex_id"] == 3)]
    ukr_subnats_compare = ukr_subnats_compare.rename(columns={"deaths": "deaths_agg"})
    # pull national level comparison from unoutliered data
    ukr_nats_compare = final[(final["location_id"] == 63) & (final["age_group_id"] == 22) & (final["sex_id"] == 3) & (final["outlier"] == 0)]
    ukr_nats_compare = ukr_nats_compare[ukr_nats_compare["year_id"].isin(list(ukr_subnats.year_id.unique()))]
    # Combine and find years we should be using aggregate from 
    ukr_compare = pd.merge(ukr_nats_compare, ukr_subnats_compare, how = "left", left_on = ['age_group_id', 'sex_id', 'year_id'], right_on = ['age_group_id', 'sex_id', 'year_id'])
    use_agg_years = ukr_compare[ukr_compare['deaths_agg'] > ukr_compare['deaths']]['year_id']
    use_agg_years = list(use_agg_years)
    use_agg_years.append(2020)

    # outlier 2017 for all UKR nat and subnat
    final.loc[(final['year_id'] == 2017) & (final['location_id'].isin([63, 50559, 44934, 44939])), 'outlier'] = 1
    # outlier year to replace UKR national
    final.loc[(final['year_id'].isin(use_agg_years)) & (final['location_id'] == 63), 'outlier'] = 1
    # Add aggregate years 
    ukr_subnats_replace = ukr_subnats[(ukr_subnats['year_id'].isin(use_agg_years))]
    final = final.append(ukr_subnats_replace)

    return(final)


if __name__ == "__main__":
    main()

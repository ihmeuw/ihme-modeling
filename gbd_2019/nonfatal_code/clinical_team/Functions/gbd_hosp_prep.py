"""
Clinical informatics functions
must run on the GBD environment or a clone
"""
import pandas as pd
import numpy as np
import platform
import getpass
import datetime
import sys
import warnings
from db_queries import get_cause_metadata, get_population, get_covariate_estimates, get_location_metadata
from db_tools.ezfuncs import query
import os

user = getpass.getuser()
prep_path = r"FILEPATH".format(user)
sys.path.append(prep_path)
import hosp_prep

if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"


def get_sample_size(df, gbd_round_id, decomp_step, fix_group237=False):
    """
    This function attaches sample size to hospital data.  It's for sources that
    should have fully covered populations, so sample size is just population.
    Checks if age_group_id is a column that exists and if not, it attaches it.

    Parameters
        df: Pandas DataFrame
            contains the data that you want to add sample_size to.  Will add
            pop to every row.
    """

    if 'year_id' in df.columns:
        df.rename(columns = {'year_id' : 'year_start'}, inplace = True)
        df['year_end'] = df['year_start']





    if 'age_group_id' not in df.columns:

        age_group = hosp_prep.get_hospital_age_groups()


        pre = df.shape[0]
        df = df.merge(age_group, how='left', on=['age_start', 'age_end'])
        assert df.shape[0] == pre, "number of rows changed during merge"
        assert df.age_group_id.notnull().all(), ("age_group_id is missing "
            "for some rows")


    pop = get_population(age_group_id=list(df.age_group_id.unique()),
                         location_id=list(df.location_id.unique()),
                         sex_id=[1,2],
                         year_id=list(df.year_start.unique()),
                         gbd_round_id=gbd_round_id, decomp_step=decomp_step)

    if fix_group237:


        fix_pop = get_population(age_group_id=[235, 32],
                                location_id=list(df.location_id.unique()),
                                sex_id=[1,2],
                                year_id=list(df.year_start.unique()),
                                gbd_round_id=gbd_round_id, decomp_step=decomp_step)
        pre = fix_pop.shape[0]
        fix_pop['age_group_id'] = 237
        fix_pop = fix_pop.groupby(fix_pop.columns.drop('population').tolist()).agg({'population': 'sum'}).reset_index()
        assert pre/2 == fix_pop.shape[0]

        pop = pd.concat([pop, fix_pop], sort=False, ignore_index=True)


    pop.rename(columns={'year_id': 'year_start'}, inplace=True)
    pop['year_end'] = pop['year_start']
    pop.drop("run_id", axis=1, inplace=True)

    demography = ['location_id', 'year_start', 'year_end',
                  'age_group_id', 'sex_id']


    pre_shape = df.shape[0]
    df = df.merge(pop, how='left', on=demography)
    assert pre_shape == df.shape[0], "number of rows don't match after merge"
    assert df.population.notnull().all(),\
        "population is missing for some rows. look at this df! \n {}".\
            format(df.loc[df.population.isnull(), demography].drop_duplicates())

    return(df)

def get_bundle_cause_info(df):



    cause_id_info = query("SQL",
                          conn_def='epi')

    acause_info = query("SQL",
                        conn_def="shared")

    acause_info = acause_info.merge(cause_id_info, how="left", on="cause_id")



    rei_id_info = query("SQL",
                        conn_def='epi')

    rei_info = query("SQL",
                     conn_def='epi')

    rei_info = rei_info.merge(rei_id_info, how="left", on="rei_id")



    acause_info.rename(columns={'cause_id': 'cause_rei_id',
                                'acause': 'acause_rei'}, inplace=True)

    rei_info.rename(columns={'rei_id': 'cause_rei_id',
                             'rei': 'acause_rei'}, inplace=True)


    folder_info = pd.concat([acause_info, rei_info], sort=False)


    folder_info = folder_info.dropna(subset=['bundle_id'])


    folder_info.drop("cause_rei_id", axis=1, inplace=True)


    folder_info.drop_duplicates(inplace=True)


    folder_info.rename(columns={'acause_rei': 'bundle_acause_rei'},
                       inplace=True)



    pre = df.shape[0]
    df = df.merge(folder_info, how="left", on="bundle_id")
    assert pre == df.shape[0]

    return(df)

def find_mapping_diff(map_vers, id_list=[]):
    """
    Give it a list of bundle ids to see how the map changed between GBD 2015
    and GBD 2016
    """

    my_query = "SQL".\
        format(id_var=','.join(str(id_var) for id_var in id_list))
    me_bid_map = query(my_query, conn_def='epi')
    me_bid_map.drop("modelable_entity_name", axis=1, inplace=True)
    me_bid_map.rename(columns={"bundle_name": "name"}, inplace=True)
    me_bid_map

    old_inc = pd.read_stata(r"FILENAME"
                            r"FILENAME"
                            r"FILEPATH")
    old_inc.drop("me_id2", axis=1, inplace=True)
    old_inc.rename(columns={'me_id1': 'modelable_entity_id'}, inplace=True)
    old_inc['measure'] = 'inc'

    old_prev = pd.read_stata(r"FILENAME"
                             r"FILENAME"
                             r"FILEPATH")
    old_prev.drop("me_id2", axis=1, inplace=True)
    old_prev.rename(columns={'me_id1': 'modelable_entity_id'}, inplace=True)
    old_prev['measure'] = 'prev'

    old = pd.concat([old_inc, old_prev], sort=False)
    old = old[old.modelable_entity_id.isin(me_bid_map.modelable_entity_id.unique())]

    old = old.merge(me_bid_map[['modelable_entity_id', 'name']], how='left',
                    on='modelable_entity_id')

    old['icd_code'] = old.icd_code.str.replace("\W", "")

    current = pd.read_stata(r"FILENAME"
                            r"FILEPATH".format(map_vers))
    current = current[current.bundle_id.isin(me_bid_map.bundle_id.unique())]
    current = current.merge(me_bid_map[['bundle_id', 'name']], how='left',
                            on='bundle_id')

    for me_id in me_bid_map.modelable_entity_id.unique():
        print("\n")
        bid = me_bid_map[me_bid_map.modelable_entity_id == me_id].\
            bundle_id.unique()[0]
        name = me_bid_map[me_bid_map.modelable_entity_id == me_id].\
            name.unique()[0]
        print("For me_id {}, bundle {}, {}:".format(str(me_id), bid, name))
        diff_backwards = set(current[current.bundle_id == bid].icd_code) -\
            set(old[old.modelable_entity_id == me_id].icd_code)
        print("the difference current - old is : {}".format(diff_backwards))
        diff_forwards = set(old[old.modelable_entity_id == me_id].icd_code) -\
            set(current[current.bundle_id == bid].icd_code)
        print("the difference old - current is : {}".format(diff_forwards))

def bundle_location(bundle_list):
    """
    give it a list of bundle ids, and it will return filepath for each bundle_id
    """

    file_list = []

    for bundle in bundle_list:
        cause_rei_info = query("""SQL""".format(bundle),
                               conn_def="DATABASE")

        if cause_rei_info.size == 0:
            file_list.append("Bundle {} is not present in the database".format(int(bundle)))
            continue


        if cause_rei_info.rei_id.notnull().all():
            acause_rei_id = cause_rei_info.iloc[0,1]
            acause_rei_name = str(query("""SQL""".format(acause_rei_id),
                                        conn_def="shared").iloc[0,0])

        if cause_rei_info.cause_id.notnull().all():
            acause_rei_id = cause_rei_info.iloc[0,0]
            acause_rei_name = str(query("""SQL""".format(acause_rei_id),
                                        conn_def="shared").iloc[0,0])



        writedir = (r"FILENAME"
                    r"FILENAME".format(acause_rei_name, int(bundle)))


        file_list.append(writedir)
    return(file_list)


def verify_missing(bundle, locs, age, sex, years,
                    run_id, map_path=None):
    """
    pass a clean map, a bundle and set of demographic info and the func
    will return the data if it exists and a print statement if not
    """
    if map_path == None:
        map_path = "FILEPATH".format(run_id)

    if type(locs) == int:
        locs = [locs]
    if type(years) == int:
        locs = [years]

    loc_source = pd.read_csv(r"FILENAME"
                             r"FILEPATH")
    loc_source = loc_source[loc_source.location_id.isin(locs)]
    df_list = []
    for source in loc_source.source.unique():
        for year in years:
            try:
                df = pd.read_hdf(r"FILENAME"
                                 r"FILENAME" + source + "_" +
                                 str(year) + "FILEPATH", key='df')
                df_list.append(df)
                del df
            except:
                print("couldn't read in " + source + str(year))

    data = pd.concat(df_list, sort=False)
    del df_list
    amap = pd.read_csv(map_path)
    amap.rename({'icg_name': 'icg_id'}, axis = 1, inplace = True)
    assert hosp_prep.verify_current_map(amap)

    amap = amap.query("bundle_id == @bundle")

    data = data[(data.cause_code.isin(amap.cause_code))]
    data = data.query("age_start == @age & sex_id == @sex")
    if data.shape[0] == 0:
        print("uuhhh, yeah. there's no data here")
        print("age {} sex {} bundle {} years {} locations {}".format(age, sex, bundle, years, locs))
    else:
        return(data)
def store_locations(df):

    loc_years = df[['year_start', 'location_id', 'source',
                    'facility_id']].drop_duplicates()

    loc_years = loc_years.merge(query("""SQL""", conn_def='shared'),
                                how="left", on='location_id')

    locs = query("SQL",
                 conn_def='shared')

    loc_years = loc_years.merge(locs, how='left', on='location_id')

    locs.rename(columns={'location_id': 'location_parent_id',
                         'location_name': 'location_parent_name'}, inplace=True)

    loc_years = loc_years.merge(locs, how='left', on='location_parent_id')

    loc_years['facility_id'] = 'inpatient'

    loc_years.rename(columns={'year_start': 'year'}, inplace=True)


    loc_years.loc[loc_years.path_to_top_parent.str.contains("4749"),
                  ['location_parent_id', 'location_parent_name']] = [4749,
                                                                     "England"]


    loc_years.loc[loc_years.location_id == 4749,
                  ['location_parent_id', 'location_parent_name']] = [1,
                                                                     "Global"]

    loc_years.to_excel(r"FILENAME"
                       r"FILEPATH", index=False)

    locs = sorted(loc_years.location_name.unique())
    loc_ids = sorted(loc_years.location_id.unique())
    location_names = "{loc}".format(loc=", ".join(loc for loc in locs))
    location_ids = "{ids}".format(ids=', '.join(str(ids) for ids in loc_ids))
    text = open(r"FILEPATH", "w")
    text.write(location_names.encode('utf-8'))
    text.write(location_ids)
    text.close()

def all_group_id_start_end_switcher(df, remove_cols=True, ignore_nulls=False):
    """
    Takes a dataframe with age start/end OR age group ID and switches from one
    to the other

    Args:
        df: (Pandas DataFrame) data to swich age labelling
        remove_cols: (bool)  If True, will drop the column that was switched
            from
        ignore_nulls: (bool)  If True, assertions about missing ages
            will be ignored.  Not a good idea to use in production but is useful
            for when you just need to quickly see what ages you have.
    """

    if sum([w in ['age_start', 'age_end', 'age_group_id'] for w in df.columns]) == 3:
        assert False,\
        "All age columns are present, unclear which output is desired. "\
        "Simply drop the columns you don't want"


    elif sum([w in ['age_start', 'age_end'] for w in df.columns]) == 2:
        merge_on = ['age_start', 'age_end']
        switch_to = ['age_group_id']




    elif 'age_group_id' in df.columns:
        merge_on = ['age_group_id']
        switch_to = ['age_start', 'age_end']
    else:
        assert False, "Age columns not present or named incorrectly"


    ages = hosp_prep.get_hospital_age_groups()


    age_set = "hospital"
    for m in merge_on:
        ages_unique = ages[merge_on].drop_duplicates()
        df_unique = df[merge_on].drop_duplicates()

        if ages_unique.shape[0] != df_unique.shape[0]:
            age_set = 'non_hospital'
        elif (ages_unique[m].sort_values().reset_index(drop=True) != df_unique[m].sort_values().reset_index(drop=True)).all():
            age_set = 'non_hospital'

    if age_set == 'non_hospital':



        ages = query("""SQL""",
                       conn_def="shared")
        ages.rename(columns={"age_group_years_start": "age_start",
                                      "age_group_years_end": "age_end"},
                                      inplace=True)



        if 'age_end' in merge_on:

            df.loc[df['age_end'] == 100, 'age_end'] = 125

        duped_ages = [294, 308, 27, 161, 38, 301]
        ages = ages[~ages.age_group_id.isin(duped_ages)]

    dupes = ages[ages.duplicated(['age_start', 'age_end'], keep=False)].sort_values('age_start')


    pre = df.shape[0]
    df = df.merge(ages, how='left', on=merge_on)
    assert pre == df.shape[0],\
        "Rows were duplicated, probably from these ages \n{}".format(dupes)


    if not ignore_nulls:
        for s in switch_to:
            assert df[s].isnull().sum() == 0, ("{} contains missing values from "
                "the merge. The values with Nulls are {}".format(s, df.loc[df[s].isnull(), merge_on].drop_duplicates().sort_values(by=merge_on)))

    if remove_cols:

        df.drop(merge_on, axis=1, inplace=True)
    return(df)


def map_to_country(df):
    """
    much of our location data is subnational, but we sometimes want to tally things by country
    this function will map from any subnational location id to its parent country
    """
    pre = df.shape[0]
    cols = df.shape[1]

    locs = get_location_metadata(location_set_id=35)
    countries = locs.loc[locs.location_type == 'admin0', ['location_id', 'location_ascii_name']].copy()
    countries.columns = ["merge_loc", "country_name"]

    df = df.merge(locs[['location_id', 'path_to_top_parent']], how='left', on='location_id')
    df = pd.concat([df, df.path_to_top_parent.str.split(",", expand=True)], axis=1)


    if df[3].isnull().any():
        warnings.warn("There are locations missing from the loc set 35 hierarchy, I'm going to break")

    df['merge_loc'] = df[3].astype(int)
    df = df.merge(countries, how='left', on='merge_loc')


    to_drop = ['path_to_top_parent', 0, 1, 2, 3, 4, 5, 6, 'merge_loc']

    to_drop = [d for d in to_drop if d in df.columns]
    df.drop(to_drop, axis=1, inplace=True)


    assert df.shape[0] == pre
    assert df.shape[1] == cols + 1
    assert df.country_name.isnull().sum() == 0,\
        "Something went wrong {}".format(df[df.country_name.isnull()])
    return df

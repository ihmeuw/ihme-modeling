import numpy as np
import pandas as pd
from db_tools import ezfuncs
from db_queries import get_cause_metadata
from db_queries import get_location_metadata

def split_out_by_cause_type(df):
    causes = get_cause_metadata(cause_set_id=4, release_id=16)
    original_shape = df.copy().shape[0]
    original_deaths = df.copy()['best'].sum()
    war_causes = [945]
    war_shock_causes = list(causes[causes['parent_id'].isin(war_causes)]['cause_id'])
    war_df = df[df['cause_id'].isin(war_shock_causes + war_causes)]
    codem_causes_no_detail = [302, 345, 408, 703]
    codem_shock_causes = list(causes[causes['parent_id'].isin(codem_causes_no_detail)]['cause_id'])
    codem_shock_causes += [335, 357, 387, 695, 699, 703, 707, 711, 727, 842, 854, 724, 689, 341, 693, 725, 338, 337]
    codem_df = df[df['cause_id'].isin(codem_shock_causes + codem_causes_no_detail)]
    non_codem_causes = [729]
    non_codem_shock_causes = list(causes[causes['parent_id'].isin(non_codem_causes)]['cause_id'])
    non_codem_df = df[df['cause_id'].isin(non_codem_shock_causes + non_codem_causes)]
    assert original_shape == (war_df.shape[0] + codem_df.shape[0] + non_codem_df.shape[0])
    assert np.isclose(original_deaths, (war_df.best.sum() + codem_df.best.sum() + non_codem_df.best.sum()))
    return war_df, codem_df, non_codem_df


def get_rank(df, conn_def):
    source_rank_query = """
    SELECT source_id, rank
    FROM ADDRESS
    """
    source_ranks = ezfuncs.query(source_rank_query, conn_def=conn_def)
    df_shape = df.copy().shape[0]
    df_deaths = df.copy()['best'].sum()
    df = pd.merge(df, source_ranks, on="source_id", how="left")
    assert df_shape == df.shape[0], "columns got duplicated due to rank merge"
    assert df_deaths == df['best'].sum(), "deaths changed due to rank merge"
    df['rank'] = df['rank'].where(~(df['source_id'] == 44),
                                            other=3)
    return df

def apply_vr_rich_codem_overrides(rich_codem):
    rich_codem.loc[((rich_codem['location_id'] == 4926) &
                (rich_codem['cause_id'] == 707) &
                (rich_codem['source_id'] == 1) &
                (rich_codem['year_id'] == 1980)), "source_id"] = -1
    rich_codem.loc[((rich_codem['location_id'] == 68) &
                (rich_codem['cause_id'] == 707) &
                (rich_codem['source_id'] == 1) &
                (rich_codem['year_id'] == 1995)), "source_id"] = -1
    rich_codem.loc[((rich_codem['location_id'] == 128) &
                (rich_codem['cause_id'] == 986) &
                (rich_codem['source_id'] == 1) &
                (rich_codem['year_id'] == 2018)), "source_id"] = -1
    return rich_codem


def expand_star_ratings(stars):
    stars['time_window'] = stars['time_window'].apply(lambda x: x.split("_"))
    stars['time_window'] = stars['time_window'].apply(lambda x: range(int(x[0]), int(x[1])))
    expanded_ratings = pd.DataFrame()
    for index, row in stars.iterrows():
        row_copy = row.copy()
        row_copy = row_copy.rename(columns={"time_window": "year_id"})
        for year in row['time_window']:
            row_copy['year_id'] = year
            expanded_ratings = expanded_ratings.append(row_copy, ignore_index=True)
    return expanded_ratings

def get_stars():
    stars = pd.read_csv((FILEPATH))
    stars = stars[['location_id', 'stars', 'time_window']]
    stars['time_window'] = stars['time_window'].replace("full_time_series", "1980_2023")
    stars = expand_star_ratings(stars)
    stars.drop("time_window", axis=1, inplace=True)
    return stars

def apply_star_ratings(df, stars):
    original_deaths = df.copy().best.sum()
    original_shape = df.copy().shape[0]
    df = pd.merge(left=df, right=stars, how='left', on=['location_id', 'year_id'])
    assert original_deaths == df.best.sum()
    assert original_shape == df.shape[0]
    df.stars = df.stars.fillna(0)
    return df


def subest_df_to_events_with_deaths(df):
    df.event_name.fillna("", inplace=True)
    df = df.query("best > 0")
    return df

def VR_is_highest(row):
    if row['source_id'] == 0:
        if row['best'] == row['highest_deaths']:
            return -2
        else:
            return row['rank']
    else:
        return row['rank']


def Amnesty_is_highest(row):
    if row['source_id'] == 34:
        if row['best'] == row['highest_deaths']:
            return -1
        else:
            return row['rank']
    else:
        return row['rank']


def prioritize_war(df):
    vr_source_id = 0
    df['vr_deaths'] = df['best'] * (df['source_id'] == 0)
    df['vr_deaths'] = df.groupby(['location_id', 'year_id', 'cause_id']
                                 )['vr_deaths'].transform(np.sum)
    df['source_deaths'] = df.groupby(['location_id', 'year_id', 'cause_id', 'source_id']
                                     )['best'].transform(np.sum)
    df['max_source_deaths'] = df.groupby(
        ['location_id', 'year_id', 'cause_id'])['source_deaths'].transform(np.max)
    is_vr_and_trumps = ((df['source_id'] == vr_source_id) &
                        (df['vr_deaths'] >= (df['max_source_deaths']) - .01))
    df['calc_rank'] = df['rank']
    df.loc[is_vr_and_trumps, 'calc_rank'] = -1
    df['lowest_rank'] = df.groupby(['location_id', 'year_id', 'cause_id']
                                   )['calc_rank'].transform(np.min)
    war_prioritized = df.copy().query('calc_rank == lowest_rank')
    war_prioritized['reason_inserted'] = "highest priority"
    war_dropped = df.copy().query('calc_rank != lowest_rank')
    war_dropped['reason_inserted'] = "outpriortiized"
    return war_prioritized, war_dropped

def prioritize_rich_non_codem(no_codem_df):
    rich_no_codem = no_codem_df.copy().query("stars >= 4")
    rich_no_codem['most_source_deaths'] = rich_no_codem.groupby(
        ['location_id', 'year_id', 'cause_id','source_id'])['best'].transform(np.sum)
    rich_no_codem.loc[((rich_no_codem['source_id'] == 0) &
                      (rich_no_codem['best'] == rich_no_codem['most_source_deaths'])),
                      "rank"] = 10.5
    rich_no_codem['lowest_rank'] = rich_no_codem.groupby(
        ['location_id', 'year_id', 'cause_id'])['rank'].transform(np.min)
    rich_no_codem_dropped = rich_no_codem.query("rank != lowest_rank")
    rich_no_codem_dropped['reason_inserted'] = "outpriortiized"
    rich_no_codem_prioritized = rich_no_codem.query("rank == lowest_rank")
    rich_no_codem_prioritized['reason_inserted'] = "highest priority"
    return rich_no_codem_prioritized, rich_no_codem_dropped


def prioritize_poor_non_codem(no_codem_df):
    poor_no_codem = no_codem_df.copy().query("stars <= 3")
    poor_no_codem['source_deaths'] = poor_no_codem.groupby(
        ['location_id', 'year_id', 'source_id', 'cause_id', "rank"])['best'].transform(np.sum)
    poor_no_codem['highest_deaths'] = poor_no_codem.groupby(
        ['location_id', 'year_id', 'cause_id'])['source_deaths'].transform(np.max)
    poor_no_codem['rank'] = poor_no_codem.apply(VR_is_highest, axis=1)
    poor_no_codem['rank'] = poor_no_codem.apply(Amnesty_is_highest, axis=1)
    poor_no_codem['lowest_rank'] = poor_no_codem.groupby(
        ['location_id', 'year_id', 'cause_id'])['rank'].transform(np.min)
    poor_no_codem_dropped = poor_no_codem.query('rank != lowest_rank')
    poor_no_codem_dropped['reason_inserted'] = "outpriortiized"
    poor_no_codem_prioritized = poor_no_codem.query('rank == lowest_rank')
    poor_no_codem_prioritized['reason_inserted'] = "highest priority"
    return poor_no_codem_prioritized, poor_no_codem_dropped

def prioritize_rich_codem(codem_df):
    # only keep VR
    rich_codem = codem_df.copy().query("stars >= 4")

    # overrides
    rich_codem = apply_vr_rich_codem_overrides(rich_codem)

    # keep only VR and Dirty War
    rich_codem_prioritized = rich_codem.query("source_id == [0, 38, -1, 45, 46]")

    rich_codem_prioritized['reason_inserted'] = (
        "Only VR is prioritized in VR rich countries with CODEm models")

    rich_codem_dropped = rich_codem.query("source_id != [0, 38, -1, 45, 46]") # change to VR id
    rich_codem_dropped['reason_inserted'] = (
        "Non VR is dropped in VR rich countries with CODEm models")

    return rich_codem_prioritized, rich_codem_dropped

def prioritize_poor_codem(codem_df):
    poor_codem = codem_df.copy().query("stars <= 3")
    dropped_vr = poor_codem.query("source_id == 0")
    dropped_vr['reason_inserted'] = "no VR in VR poor codem models"
    poor_codem = poor_codem.query("source_id != 0")
    poor_codem['source_deaths'] = poor_codem.groupby(
        ['location_id', 'year_id', 'source_id', 'cause_id', "rank"])['best'].transform(np.sum)
    poor_codem['highest_deaths'] = poor_codem.groupby(
        ['location_id', 'year_id', 'cause_id'])['source_deaths'].transform(np.max)
    poor_codem['rank'] = poor_codem.apply(Amnesty_is_highest, axis=1)
    poor_codem['lowest_rank'] = poor_codem.groupby(
        ['location_id', 'year_id', 'cause_id'])['rank'].transform(np.min)
    poor_codem_dropped = poor_codem.query('rank != lowest_rank')
    poor_codem_dropped['reason_inserted'] = "outpriortized"
    poor_codem_dropped = poor_codem_dropped.append(dropped_vr)
    poor_codem_prioritized = poor_codem.query('rank == lowest_rank')
    poor_codem_prioritized['reason_inserted'] = "highest priority"
    return poor_codem_prioritized, poor_codem_dropped

def prioritize_codem(codem_df):
    rich_codem_prioritized, rich_codem_dropped = prioritize_rich_codem(codem_df)
    poor_codem_prioritized, poor_codem_dropped = prioritize_poor_codem(codem_df)
    dropped = pd.concat([rich_codem_dropped, poor_codem_dropped])
    prioritized = pd.concat([rich_codem_prioritized, poor_codem_prioritized])
    assert codem_df.shape[0] == (prioritized.shape[0] + dropped.shape[0])
    assert np.isclose(codem_df.best.sum(), (prioritized.best.sum() + dropped.best.sum()))
    return prioritized, dropped

def prioritize_non_codem(no_codem_df):
    rich_no_codem_prioritized, rich_no_codem_dropped = prioritize_rich_non_codem(no_codem_df)
    poor_no_codem_prioritized, poor_no_codem_dropped = prioritize_poor_non_codem(no_codem_df)
    dropped = pd.concat([rich_no_codem_dropped, poor_no_codem_dropped])
    prioritized = pd.concat([rich_no_codem_prioritized, poor_no_codem_prioritized])
    assert no_codem_df.shape[0] == (prioritized.shape[0] + dropped.shape[0])
    assert np.isclose(no_codem_df.best.sum(), (prioritized.best.sum() + dropped.best.sum()))
    return prioritized, dropped

def drop_all_Mexico_homicide(prioritized):
    locs = get_location_metadata(location_set_id=21, release_id=16)
    mex_locs = list(locs.query("parent_id == 130")['location_id'])
    homicide_causes = [724, 725, 726, 727 ,855]
    prioritized = prioritized.loc[~(
            (prioritized['location_id'].isin(mex_locs)) & 
            (prioritized['cause_id'].isin(homicide_causes))
    )]
    return prioritized

def drop_all_Brazil_homicide(prioritized):
    locs = get_location_metadata(location_set_id=21, release_id=16)
    mex_locs = list(locs.query("parent_id == 135")['location_id'])
    homicide_causes = [724, 725, 726, 727]
    prioritized = prioritized.loc[~(
            (prioritized['location_id'].isin(mex_locs)) & 
            (prioritized['cause_id'].isin(homicide_causes))
    )]
    return prioritized

def drop_executions_deaths_under_1_years_old(prioritized):
    prioritized = prioritized.loc[~(
                           (prioritized['cause_id'] == 854) &
                           (prioritized['age_group_id'] <= 4)
    )]
    return prioritized

def split_homicide_deaths_by_vr(prioritized):
    weights = pd.read_csv(FILEPATH)
    violence_causes = [725, 726, 727]
    violence_df = prioritized[prioritized['cause_id'] == 724].copy()
    prioritized = prioritized[prioritized['cause_id'] != 724].copy()
    violence_df = violence_df.drop(['cause_id'], axis=1)
    violence_df = pd.merge(left=violence_df, right=weights[['cause_id','location_id','weight']], how='left', on='location_id')
    violence_df['best'] = violence_df['best'] * violence_df['weight']
    violence_df['high'] = violence_df['high'] * violence_df['weight']
    violence_df['low'] = violence_df['low'] * violence_df['weight']
    violence_df = violence_df[["location_id",'year_id','age_group_id','sex_id', 
                               "raw_data_id", "side_split_data_id", "source_id",
                               "source_event_id", "nid", "event_name",
                               'best','high','low','cause_id']]
    prioritized = prioritized.append(violence_df)
    return prioritized

def run_prioritization(df, conn_def):
    df['reason_inserted'] = ""
    total_original_deaths = df.best.copy().sum()
    df = get_rank(df, conn_def)
    df = subest_df_to_events_with_deaths(df)
    war_df, codem_df, no_codem_df = split_out_by_cause_type(df)
    stars = get_stars()
    codem_df = apply_star_ratings(codem_df, stars)
    no_codem_df = apply_star_ratings(no_codem_df, stars)
    codem_prioritized, codem_dropped = prioritize_codem(codem_df)
    no_codem_prioritized, no_codem_dropped = prioritize_non_codem(no_codem_df)
    war_prioritized, war_dropped = prioritize_war(war_df)
    prioritized = pd.concat([codem_prioritized, no_codem_prioritized, war_prioritized])
    dropped = pd.concat([codem_dropped, no_codem_dropped, war_dropped])
    prioritized = split_homicide_deaths_by_vr(prioritized)
    assert np.isclose(total_original_deaths,(prioritized.best.sum() + dropped.best.sum()))
    prioritized = drop_all_Mexico_homicide(prioritized)
    prioritized = drop_all_Brazil_homicide(prioritized)
    prioritized = drop_executions_deaths_under_1_years_old(prioritized)
    return prioritized, dropped

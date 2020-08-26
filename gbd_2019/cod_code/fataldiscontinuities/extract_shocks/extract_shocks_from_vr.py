# 3
import pandas as pd
import numpy as np
from db_tools import ezfuncs

PATH_TO_STAR_RATINGS = ("FILEPATH")


def find_surrounding_non_shock_years(row):
    if row['shock'] == 0:
        return None, None, None, None

    year_id = row['year_id']
    cause_id = row['cause_id']
    location_id = row['location_id']

    next_year_row = vr.query("year_id > {} & cause_id == {} & location_id == {} & shock==0".format(
        year_id, cause_id, location_id)).copy()

    if next_year_row.shape[0] > 0:
        next_year = next_year_row.iloc[0].at['year_id']
        next_years_deaths = next_year_row.iloc[0].at['deaths']
    else:
        next_year = None
        next_years_deaths = None

    previous_year_row = vr.query(
        "year_id < {} & cause_id == {} & location_id == {} & shock==0".
        format(year_id, cause_id, location_id)).copy()
    rows = previous_year_row.shape[0]

    if rows > 0:
        previous_year = previous_year_row.iloc[rows - 1].at['year_id']
        previous_years_deaths = previous_year_row.iloc[rows - 1].at['deaths']
    else:
        previous_year = None
        previous_years_deaths = None

    return (next_year, next_years_deaths, previous_year, previous_years_deaths)


def calculate_base_rate(row):
    next_year = row['next_year']
    next_years_deaths = row['next_years_deaths']
    previous_year = row['previous_year']
    previous_years_deaths = row['previous_years_deaths']

    previous_year_present = not np.isnan(previous_year)
    next_year_present = not np.isnan(next_year)

    if (previous_year_present & next_year_present):
        base_rate = (next_years_deaths + previous_years_deaths) / 2.0
    elif previous_year_present:
        base_rate = previous_years_deaths
    elif next_year_present:
        base_rate = next_years_deaths
    else:
        base_rate = 0

    return base_rate


def get_all_active_shock_years():

    active_shock_query = """
    SELECT * FROM ADDRESS
    WHERE active_status = 1
    """
    df = ezfuncs.query(active_shock_query, conn_def="ADDRESS")

    return df


def extract_shock_deaths(row):

    no_codem_causes = [851, 855, 945, 729, 987, 985, 989, 988, 986, 990]
    if row['vr_rich'] == 1:

        if row['cause_id'] in no_codem_causes:
            extracted_shock = row['deaths']

        else:

            if row['shock'] == 1:
                extracted_shock = row['deaths'] - row['base_rate']

            else:
                extracted_shock = 0

    else:
        if row['cause_id'] in no_codem_causes:
            extracted_shock = row['deaths']

        else:
            extracted_shock = 0

    return max(extracted_shock, 0)


vr = pd.read_csv("FILEPATH")
# read in all shock data to identify shock years
df = get_all_active_shock_years()

# keep only one row for each event
shocks = df[['cause_id', 'location_id', 'year_id']].drop_duplicates()

# create an indicator column
shocks['shock'] = 1

vr = vr.groupby(['location_id', 'cause_id', 'year_id', 'nid'], as_index=False)['deaths'].sum()
# merge both dataframes
og_shape = vr.shape[0]
vr = pd.merge(left=vr, right=shocks, on=['cause_id', 'location_id', 'year_id'], how='left')
assert og_shape == vr.shape[0]
vr = vr.fillna(0)
vr = vr.sort_values(by=['location_id', 'cause_id', 'year_id'])

vr[["next_year", "next_years_deaths", "previous_year", "previous_years_deaths"]] = vr.apply(
    find_surrounding_non_shock_years, axis=1).apply(pd.Series)

vr['base_rate'] = vr.apply(calculate_base_rate, axis=1)

# read in VR star ratings
vr_rich_locs = list(pd.read_csv(PATH_TO_STAR_RATINGS, encoding='utf8')['location_id'])

# identify VR rich locations
vr['vr_rich'] = vr.apply(lambda x: 1 if ((x['location_id'] in vr_rich_locs) &
                         (x['year_id'] >= 1980)) else 0, axis=1)

# extract shock deaths from VR based on type of location-cause
vr['extracted_shock'] = vr.apply(extract_shock_deaths, axis=1)

vr['extracted_shock'] = vr['extracted_shock'].fillna(0)

vr['remaining_deaths'] = vr['deaths'] - vr['extracted_shock']

vr.to_csv("FILEPATH")

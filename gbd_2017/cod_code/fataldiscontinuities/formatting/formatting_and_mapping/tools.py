
import functools
import numpy as np
import pandas as pd
from db_queries import get_location_metadata


def compose(*functions):

    def compose2(f, g):
        return lambda x: f(g(x))

    return functools.reduce(compose2, functions, lambda x: x)


def review_db_contents(shock_db):
    print('There are ' + str(len(shock_db.columns.values)) +
          " columns in the shock db. They are: ")
    print(shock_db.columns.values, '\n\n')
    print("The datasets included are: ")
    print(shock_db.dataset.value_counts(), '\n\n')
    print("Event type counts are: ")
    print(shock_db.event_type.value_counts(), '\n\n')
    len_db = len(shock_db.index)
    print('Total rows: ' + str(len_db))

    # exceptions as a fraction of the data (by number of rows)
    dataset_counts = shock_db.dataset.value_counts(
    ).reset_index().rename(columns={'index': 'dataname'})
    print('exceptions make up ' +
          str(100 *
              dataset_counts.ix[10:, 'dataset'].sum() /
              len_db) +
          ' percent of the data')

    # data completeness stats
    print('\n')
    print('Data Completeness:')
    pct_country = 100 * shock_db.country.notnull().sum() / len(shock_db.index)
    n_location_dataful = len(shock_db.loc[((shock_db.country.notnull()) |
                                           (shock_db.location.notnull()) |
                                           (shock_db.location_id.notnull()) |
                                           (shock_db.iso.notnull()))].index)
    n_nidful = len(shock_db[shock_db.nid.notnull()].index)
    print('Percent of data rows containing country: ' + str(pct_country) + ' %')
    print('Percent of data rows containing country|location|location_id|iso: ' +
          str(100 * n_location_dataful / len_db) + ' %')
    print('Percent of data rows containing NID: ' +
          str(100 * n_nidful / len_db) + ' %')


def format_mixed_yY_dates(col):

    # dd/mm/yy
    col_y = col[col.str.match(r'^\d{1,2}/\d{1,2}/\d{2}$')]
    col_y = pd.to_datetime(col_y, format='%d/%m/%y').dt.strftime('%Y/%m/%d')

    # dd/mm/YYYY
    col_Y = col[col.str.match(r'^\d{1,2}/\d{1,2}/\d{4}$')]
    col_Y = pd.to_datetime(col_Y, format='%d/%m/%Y').dt.strftime('%Y/%m/%d')

    out = pd.concat([col_y, col_Y])
    out = out.sort_index()
    return out


def get_locs(gbd_round_id=5, location_set_id=8, subnat_bearers_only=False):

    locs = get_location_metadata(gbd_round_id=gbd_round_id,
                                 location_set_id=location_set_id
                                 )
    if subnat_bearers_only:
        locs = locs[(locs.level == 3) & (locs.most_detailed == 0)]

    keep_cols = ['location_name', 'location_id', 'ihme_loc_id']
    locs = locs[keep_cols]

    return locs


def split_years_by_date_start_end(df):
    initial_df = df.copy()
    initial_df = initial_df.reset_index()
    total_rows = len(initial_df)

    start_year = []
    end_year = []
    start_mo = []
    end_mo = []


    for k in range (0,total_rows):
        start_year.append(int(initial_df["date_start"][k].split("/")[0]))
        end_year.append(int(initial_df["date_end"][k].split("/")[0]))
        start_mo.append(int(initial_df["date_start"][k].split("/")[1]))
        end_mo.append(int(initial_df["date_end"][k].split("/")[1]))

    initial_df['start_year'] = start_year
    initial_df['end_year'] = end_year
    initial_df['start_mo'] = start_mo
    initial_df['end_mo'] = end_mo

    headers = list(initial_df.columns.values)

    #isolating entries which span multiple years
    #will split and add back to the correct ones at end
    mismatch_yr = initial_df.where(initial_df["start_year"] != initial_df["end_year"])
    mismatch_yr = mismatch_yr[pd.notnull(mismatch_yr.country)]
    correct_yr = initial_df.where(initial_df["start_year"] == initial_df["end_year"])
    correct_yr = correct_yr[pd.notnull(correct_yr.country)]
    mismatch_yr = mismatch_yr.reset_index()

    fixed = pd.DataFrame(columns=headers)

    entries = len(mismatch_yr)
    for k in range(0,entries):
        yr_diff = int(mismatch_yr.loc[k,"end_year"] - mismatch_yr.loc[k,"start_year"] + 1)
        total_mo = (13-mismatch_yr.loc[k,"start_mo"]) + 12*(yr_diff-2) + mismatch_yr.loc[k,"end_mo"]
        best_by_mo = mismatch_yr.loc[k,"best"] / total_mo

        row = mismatch_yr.loc[k,:]
        for j in range(0,yr_diff):
            if j == 0:
                best_death = (13-mismatch_yr.loc[k,"start_mo"])*best_by_mo
            elif j == yr_diff - 1:
                best_death = (mismatch_yr.loc[k,"end_mo"])*best_by_mo
            else:
                best_death = 12*best_by_mo
            row["year"] = row["start_year"] + j
            row["best"] = best_death
            fixed = fixed.append(row)

    #concatenating split entries with the original correct entries
    final = pd.concat([correct_yr,fixed])
    assert np.allclose(initial_df['best'].sum(), final['best'].sum())
    return final
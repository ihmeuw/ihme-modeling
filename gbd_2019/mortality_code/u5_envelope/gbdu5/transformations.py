import numpy as np
import pandas as pd

def reshape_wide(data, index_columns, data_columns, reshape_column):
    # Get reshape IDs
    reshape_ids = data[reshape_column].drop_duplicates().tolist()
    # Cycle through and create slices for each of the reshape IDs
    data_reshaped = []
    for i in reshape_ids:
        keep_columns = index_columns + data_columns
        temp = data.loc[data[reshape_column]==i, keep_columns].copy(deep=True)
        nc = {c: "{}_{}".format(c, i) for c in data_columns}
        temp = temp.rename(columns=nc).set_index(index_columns)
        data_reshaped.append(temp)
    data_reshaped = pd.concat(data_reshaped, axis=1).reset_index()
    return data_reshaped

def ready_to_merge(data, age):
    new_names = {'deaths': 'deaths{}'.format(age), 'pys': 'pys{}'.format(age)}
    keep_almost_cols = ['ihme_loc_id', 'year', 'sim', 'sex', 'deaths', 'pys']
    temp = data.loc[data['age'] == age].copy(deep=True)
    temp = temp[keep_almost_cols]
    temp = temp.rename(columns=new_names)
    return temp

def calculate_annualized_pct_change(data, start_year, end_year, index_cols, data_cols, year_col):
    keep_cols = index_cols + data_cols

    start_data = data.loc[(data[year_col] == start_year)].copy(deep=True)
    end_data = data.loc[(data[year_col] == end_year)].copy(deep=True)

    roc_data = pd.merge(start_data[keep_cols], end_data[keep_cols], on=index_cols, how='left',
                    suffixes=["_{}".format(start_year), "_{}".format(end_year)])
    output_cols = []
    for c in data_cols:
        s = "{}_{}".format(c, start_year)
        e = "{}_{}".format(c, end_year)
        r = "{}_range".format(c)
        o = "{}_roc".format(c)
        t = "{}_test".format(c)
        # Add output columns to the output_cols list
        output_cols.append(o)
        # Calculate annualized rate of change
        roc_data[o] = np.log(roc_data[s]/roc_data[e]) / (end_year - start_year)

    return roc_data[index_cols + output_cols]

def back_calculate(data, reference_year, projection_years, data_cols):
    reference_data = data.loc[(data['year_id'] == reference_year)].copy(deep=True)
    output = []
    for y in projection_years:
        temp = reference_data.copy(deep=True)
        temp['year_id'] = y
        for c in data_cols:
            roc_col = "{}_roc".format(c)
            temp[c] = temp[c] * np.exp((reference_year - y) * temp[roc_col])
        output.append(temp)
    output = pd.concat(output)
    return(output)
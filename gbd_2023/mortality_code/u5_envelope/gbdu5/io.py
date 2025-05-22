import os
import pandas as pd

def read_empirical_life_tables(files, sex_id):
    data = []
    for f in files:
        temp = pd.read_csv(f, delim_whitespace=True, header=1)
        temp = temp[['Year', 'Age', 'lx', 'qx']]
        temp['iso3'] = os.path.basename(f)
        temp['sex_id'] = sex_id
        data.append(temp)
    data = pd.concat(data).reset_index(drop=True)
    data = data.rename(columns={c: c.lower() for c in data.columns})
    data['age'] = data['age'].str.replace("+", "")
    data['age'] = data['age'].astype('int64')
    data['year'] = data['year'].astype('int64')
    data = data.loc[data['lx']!="."]
    data['lx'] = data['lx'].astype('float64')
    return data

def get_sex_data():
    data = pd.DataFrame(
        [["male", 1], ["female", 2], ["both", 3]], columns=['sex', 'sex_id'])
    return data

def get_finalizer_draws(with_shock_number_version,location_id, locs):
    """
    get with shock draws and expand ch to 1, 2, 3, and 4
    """
    lt = pd.read_csv("FILEPATH")
    lt = lt[['age_group_id', 'location_id', 'sex_id', 'year_id', 'draw', 'qx']]
    lt = lt.merge(locs, on = 'location_id')
    lt = lt.rename(columns={'year_id': 'year', 'draw': 'sim'})
    df_sex = pd.DataFrame([["male", 1], ["female", 2], ["both", 3]], columns=['sex', 'sex_id'])
    lt = lt.merge(df_sex, on=['sex_id'])

    lt_chb= lt.loc[lt.age_group_id == 238]
    lt_chb = lt_chb.rename(columns={'qx': 'q_3x2'})

    keep_columns = ['ihme_loc_id', 'sim', 'sex_id', 'year', 'q_3x2']
    df = lt_chb[keep_columns]
    data = []
    for age in range(2, 5):
        temp = df.copy(deep=True)
        temp['age'] = age
        data.append(temp)
    data = pd.concat(data)

    # Only keep the U5 age groups from finalizer
    data_single = lt.loc[lt.age_group_id.isin([2,3,388,389,34,238])]

    # Do some pivoting to mimic the age sex input format.
    data_single.age_group_id = data_single.age_group_id.astype('str')
    data_single=data_single.pivot_table(index=['ihme_loc_id', 'sex_id', 'year', 'sim'], columns='age_group_id', values='qx').reset_index()
    data_single = data_single.rename(columns={'2': 'q_enn', '3': 'q_lnn', '388': "q_pna", '389': 'q_pnb', '238' : 'qx_1', '34' : 'q_3x2'})

    return data, data_single


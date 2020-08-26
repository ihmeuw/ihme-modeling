import os
import pandas as pd

def read_empirical_life_tables(files, sex_id):
    data = []
    for f in files:
        temp = pd.read_table(f, delim_whitespace=True, header=1)
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

    lt_ch= lt.loc[lt.age_group_id == 5]
    lt_ch = lt_ch.rename(columns={'qx': 'q_ch'})

    keep_columns = ['ihme_loc_id', 'sim', 'sex_id', 'year', 'q_ch']
    df = lt_ch[keep_columns]
    data = []
    for age in range(1, 5):
        temp = df.copy(deep=True)
        temp['age'] = age
        data.append(temp)
    data = pd.concat(data)

    data_single = lt.loc[lt.age_group_id.isin([2,3,4,28,5])]
    data_single.age_group_id = data_single.age_group_id.astype('str')
    data_single=data_single.pivot_table(index=['ihme_loc_id', 'sex_id', 'year', 'sim'], columns='age_group_id', values='qx').reset_index()
    data_single = data_single.rename(columns={'2': 'q_enn', '3': 'q_lnn', '4': "q_pnn", '28': 'q_inf', '5': 'q_ch'})
    data_single['q_5'] = 1 - data_single['q_inf'] * data_single['q_ch']
    data_single = data_single[['ihme_loc_id', 'year', 'sex_id', 'sim', 'q_enn', 'q_lnn', 'q_pnn', 'q_inf','q_ch', 'q_5']]

    return data, data_single

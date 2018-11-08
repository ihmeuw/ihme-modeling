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

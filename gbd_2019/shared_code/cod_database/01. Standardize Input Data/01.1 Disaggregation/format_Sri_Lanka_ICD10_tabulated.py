import pandas as pd
import numpy as np
from cod_prep.downloaders.ages import get_ages
from cod_prep.downloaders import get_cause_map
from cod_prep.claude.formatting import finalize_formatting
from cod_prep.claude.configurator import Configurator
from cod_prep.utils import get_adult_age_codebook
from cod_prep.utils import get_infant_age_codebook
from cod_prep.utils import map_gbd2016_disagg_targets
CONF = Configurator('standard')

rdp_path = CONF.get_resource('rdp_frac_path')

ID_COLS = [
    'nid', 'location_id', 'year_id', 'age_group_id', 'sex_id',
    'data_type_id', 'representative_id', 'code_system_id', 'code_id', 'site'
]
INT_COLS = [col for col in ID_COLS if 'id' in col]
VALUE_COL = ['deaths']
FINAL_FORMATTED_COLS = ID_COLS + VALUE_COL
WRITE = False
YEARS = [2007, 2013]


def read_data(year):
    year_to_file = {
        2007: 'FILEPATH',
        2013: 'FILEPATH'
    }
    df = pd.read_excel(f'FILEPATH/{year}/{year_to_file[year]}')
    if year == 2007:
        df = df.drop([f'Unnamed: {col_num}' for col_num in range(80, 83)], axis='columns')
    return df


def clean_df(df):
    # cleaning of unnecessary rows/columns due to formatting of imported excel file
    df.iloc[4, 0] = df.iloc[3, 0]
    df.drop(df.index[[0, 1, 2]], inplace=True)
    df.reset_index(inplace=True, drop=True)

    df.columns = df.iloc[1].tolist()
    df.drop(df.index[[0, 1]], inplace=True)
    df.drop(['Male', 'Female'], axis=1, inplace=True)

    # removing rows that are sums of other rows
    df.columns.values[0] = 'cause_name'
    # aggregates is a list of rows which contain sums of the most detailed rows
    aggregates = ['1-001', '1-026', '1-048', '1-051', '1-055', '1-058',
                  '1-064', '1-072', '1-078', '1-084', '1-087', '1-095', 'Total']
    df['cause_name'] = df['cause_name'].str.lstrip()
    df['value'] = df['cause_name'].str[0:5]
    df = df.loc[~(df.value.isin(aggregates))]

    return df


def read_and_clean_data():
    df = pd.concat(
        [clean_df(read_data(year)).assign(year_id=year) for year in YEARS],
        sort=False, ignore_index=True)
    return df


def split_sexes(df):
    # getting total deaths before splitting, to check against split df at end
    df['Total'] = df['Total'].replace('-', 0)
    initial_total = df.Total.sum()

    # splitting df into male and female dfs, which will be stacked back on top of each other
    males = df[['M']]
    females = df[['F']]
    males.replace({'-': 0}, inplace=True)
    females.replace({'-': 0}, inplace=True)

    # assigning age_group headers ('drop' refers to columns that are aggregate sum columns)
    headers = ['Early Neonatal', 'Late Neonatal', 'drop', '1-2 months', '3-5 months',
               '6-8 months', '9-11 months', 'drop', 'drop', '1 year', '2 years', '3 years',
               '4 years', 'drop', '5-9', '10-14', 'drop', '15-19', '20-24', 'drop', '25-29',
               '30-34', '35-39', '40-44', 'drop', '45-49', '50-54', '55-59', '60-64',
               'drop', '65-69', '70-74', 'drop', '75-79', '80-84', '85 plus', 'drop',
               'unknown']

    males.columns = headers
    females.columns = headers
    males['sex_id'] = 1
    females['sex_id'] = 2

    males.drop('drop', axis=1, inplace=True)
    females.drop('drop', axis=1, inplace=True)

    # concatenating male and female dfs, stacking cause column on itself to account for
    # stacking of male/female dfs
    both = pd.concat([males, females])
    causes = df[['cause_name', 'value', 'year_id']]
    causes2 = causes.copy()
    stacked_causes = pd.concat([causes, causes2])
    df = pd.concat([stacked_causes, both], axis=1)

    return df, initial_total


def get_age_ids(df):
    # renaming age names to match with cod_ages name
    df['age'] = df['age'].str.replace('-', ' to ')
    # Under 5 map
    u5_map = {
        '1 to 2 months': '1-5 months',
        '3 to 5 months': '1-5 months',
        '6 to 8 months': '6-11 months',
        '9 to 11 months': '6-11 months',
        '1 year': '12 to 23 months',
        '2 years': '2 to 4',
        '3 years': '2 to 4',
        '4 years': '2 to 4'
    }
    df.age.update(df.age.map(u5_map))
    df.rename(columns={'age': 'age_group_name'}, inplace=True)

    # using get_ages() to merge on age_group_ids
    ages = get_ages()
    df = df.merge(ages[['age_group_id', 'age_group_name']], on='age_group_name', how='left')

    # manually assigning age_group_id for 'unknown'
    df.loc[df.age_group_name == 'unknown', 'age_group_id'] = 283
    assert df.age_group_id.notnull().all()

    return df


def format_rdp_frac(rdp):
    # cleaning rdp_frac dataframe
    rdp.drop(['percent2', 'percent4', 'percent5', 'percent6',
              'percent26', 'percent92', 'percent24', 'percent25', 'num'], axis=1, inplace=True)

    # keeping only frmat 1 and im_frmat 2
    rdp = rdp.loc[(rdp.frmat == 1) & (rdp.im_frmat == 2)]

    # setting super region to 6 (the one that corresponds to Sri Lanka)
    rdp = rdp.loc[rdp.super_region == 6]

    # reshaping, renaming, and standardizing column types
    rdp.drop(['super_region'], axis=1, inplace=True)
    rdp = pd.melt(rdp, id_vars=['cause', 'target', 'sex', 'frmat',
                                'im_frmat'], var_name='cod_age', value_name='pct')
    rdp['cod_age'] = rdp['cod_age'].str.replace('percent', "")

    rdp['frmat'] = rdp['frmat'].astype(int)
    rdp['im_frmat'] = rdp['im_frmat'].astype(int)
    rdp['cod_age'] = rdp['cod_age'].astype(int)

    # getting codebooks to map rdp age_group_ids to cod age_group_ids
    adult_cb = get_adult_age_codebook()
    adult_cb['frmat'] = adult_cb['frmat'].astype(int)
    adult_cb['cod_age'] = adult_cb['cod_age'].astype(int)

    infant_cb = get_infant_age_codebook()
    infant_cb['im_frmat'] = infant_cb['im_frmat'].astype(int)
    infant_cb['cod_age'] = infant_cb['cod_age'].astype(int)

    # merging on cod age_group_ids so we can merge rdp onto our vr dataframe later
    rdp = rdp.merge(adult_cb[['cod_age', 'age_group_id', 'frmat']],
                    on=['frmat', 'cod_age'], how='left')
    rdp = rdp.merge(infant_cb[['cod_age', 'age_group_id', 'im_frmat']],
                    on=['im_frmat', 'cod_age'], how='left')

    rdp.loc[rdp.age_group_id_x.isnull(), 'age_group_id_x'] = rdp['age_group_id_y']

    rdp.rename(columns={'age_group_id_x': 'age_group_id',
                        'cause': 'code', 'sex': 'sex_id'}, inplace=True)
    # cod_age 26 (age unknown) is empty for this frmat/imfrmat - use all ages
    # disaggregation proportions for unknown ages
    rdp.loc[rdp.cod_age == 1, 'age_group_id'] = 283
    rdp.drop(['age_group_id_y', 'frmat', 'im_frmat', 'cod_age'], axis=1, inplace=True)
    assert rdp.age_group_id.notnull().all()

    # standardizing rdp column types to ensure successful merge
    rdp['code'] = rdp['code'].astype(int)
    rdp['age_group_id'] = rdp['age_group_id'].astype(int)
    rdp['sex_id'] = rdp['sex_id'].astype(int)

    # There are no disaggregation proportions for the new under 5 age groups
    # Use the proportion of the parent
    age_map = pd.DataFrame(
        [[4, 388], [4, 389], [5, 238], [5, 34]],
        columns=['age_group_id', 'detailed_age_group_id'])
    rdp = rdp.merge(age_map, how='left', on='age_group_id')
    rdp.age_group_id.update(rdp.detailed_age_group_id)
    rdp = rdp.drop('detailed_age_group_id', axis='columns')
    return rdp


def disaggregate(df, rdp):
    pre_merge_deaths = df.deaths.sum()
    # column renaming and type standardizing to merge with rdp
    df['value'] = df['value'].str.replace('-', "")
    df.rename(columns={'value': 'code'}, inplace=True)
    df['code'] = df['code'].astype(int)
    df['age_group_id'] = df['age_group_id'].astype(int)
    df['sex_id'] = df['sex_id'].astype(int)

    # merging targets/percents from rdp onto dataframe
    df = df.merge(rdp, on=['code', 'sex_id', 'age_group_id'], how='left')
    assert df.loc[df.code.isin(rdp.code.unique()), 'target'].notnull().all()

    # fixing codes/death totals where merge successful
    df.loc[df.target.notnull(), 'target'] = 'acause_' + df['target']
    df.loc[df.pct.notnull(), 'deaths'] = df['deaths'] * df['pct']
    df.loc[df.target.notnull(), 'code'] = df['target']
    df.drop(['target', 'pct', ], axis=1, inplace=True)

    # asserting no null values and death totals stayed consistent
    assert df.notnull().values.all()
    assert np.allclose(df.deaths.sum(), pre_merge_deaths)

    return df


def map_code_id(df, cause_map):
    # merging code ids on using cause map
    df.rename(columns={'code': 'value'}, inplace=True)
    df['value'] = df['value'].astype(str)
    df = df.merge(cause_map[['value', 'code_id']], on='value', how='left')

    # some code_ids can't merge due to slight differences in engine room codes
    # use standard mapping to fix this 2.27.2019
    df = map_gbd2016_disagg_targets(df, 'value')

    # dropping code_id, then re merging with fixes in place
    df.drop('code_id', axis=1, inplace=True)
    df = df.merge(cause_map[['value', 'code_id']], on='value', how='left')
    assert df.code_id.notnull().all()

    return df


def format_sri_lanka():
    df = read_and_clean_data()

    # incoming data has sex data in wide format, following function splits df by
    # sex and manually sets age groups
    # function returns initial_total (a float to compare against deaths later to
    # ensure no deaths were lost in process)
    df, initial_total = split_sexes(df)

    # reshaping df age groups wide to long and assuring no deaths were lost
    df = pd.melt(
        df, id_vars=['cause_name', 'sex_id', 'value', 'year_id'],
        var_name='age', value_name='deaths'
    )
    assert np.allclose(initial_total, df.deaths.sum())

    df = get_age_ids(df)

    # importing and formatting rdp_frac dataframe to disaggregate tabulated icd10
    rdp = pd.read_stata(rdp_path)
    rdp = format_rdp_frac(rdp)

    # disaggregating tabulated icd10 codes
    df = disaggregate(df, rdp)

    # mapping code_ids using cause map from engine room
    cause_map = get_cause_map(code_system_id=9)
    df = map_code_id(df, cause_map)

    # addition of manually added columns
    # Sri Lanka location id 17
    df['location_id'] = 17
    # nid 327524
    df['nid'] = df.year_id.map({2007: 272959, 2013: 327524})
    # data_type_id 9 (VR)
    df['data_type_id'] = 9
    # code_system_id 9 (ICD10_tabulated)
    df['code_system_id'] = 9
    # site: blank, representative_id: 1
    df['site'] = ""
    df['representative_id'] = 1

    # grouping by ID_COLS and assigning system source
    df = df[FINAL_FORMATTED_COLS]
    assert df.notnull().values.all()
    df[INT_COLS] = df[INT_COLS].astype(int)
    df = df.groupby(ID_COLS, as_index=False)[VALUE_COL].sum()

    system_source = "ICD10_tabulated"

    # run finalize formatting
    finalize_formatting(df, system_source, write=WRITE)
    return df


if __name__ == "__main__":
    df = format_sri_lanka()

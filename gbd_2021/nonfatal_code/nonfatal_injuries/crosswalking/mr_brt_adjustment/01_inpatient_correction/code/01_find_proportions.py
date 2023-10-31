# team: GBD Injuries
# project: crosswalking GBD 2020
# script: get proportions for inpatient correction

# load packages
import pandas as pd
pd.set_option('display.max_columns', 500)
import numpy as np

# combine all years of NHAMCS data into one dataframe
appended_data = []
for year in [2002, 2003, 2004]:
    print(year)

    amb = pd.read_stata(
        'FILEPATH' + str(year) + 'FILEPATH')
    out = pd.read_stata(
        'FILEPATH' + str(year) + 'FILEPATH')

    amb = amb[['SEX', 'AGE', 'YEAR', 'INJURY', 'CAUSE1', 'CAUSE2', 'CAUSE3', 'ADMITHOS']]
    out = out[['SEX', 'AGE', 'YEAR', 'INJURY', 'CAUSE1', 'CAUSE2', 'CAUSE3', 'ADMITHOS']]

    amb_out = amb.append(out)
    appended_data.append(amb_out)
    print('Added data for ' + str(year))

all_data = pd.concat(appended_data)

# get only the injuries data
injury = all_data[all_data['INJURY'] == 1]

injury['CAUSE1'] = injury['CAUSE1'].str[0:3] + '.' + injury['CAUSE1'].str[3:]
injury['CAUSE2'] = injury['CAUSE2'].str[0:3] + '.' + injury['CAUSE2'].str[3:]
injury['CAUSE3'] = injury['CAUSE3'].str[0:3] + '.' + injury['CAUSE3'].str[3:]

injury['CAUSE1'] = injury['CAUSE1'].str.replace('-', '0', regex=True)
injury['CAUSE2'] = injury['CAUSE2'].str.replace('-', '0', regex=True)
injury['CAUSE3'] = injury['CAUSE3'].str.replace('-', '0', regex=True)

injury['CAUSE1'] = injury['CAUSE1'].astype('float')
injury['CAUSE2'] = injury['CAUSE2'].astype('float')
injury['CAUSE3'] = injury['CAUSE3'].astype('float')

# road injury ICD-9 codes
icd9 = [800.3,
        801.3,
        802.3,
        803.3,
        804.3,
        805.3,
        806.3,
        807.3,
        810.0, 810.1, 810.2, 810.3, 810.4, 810.5, 810.6,
        811.0, 811.1, 811.2, 811.3, 811.4, 811.5, 811.6, 811.7,
        812.0, 812.1, 812.2, 812.3, 812.4, 812.5, 812.6, 812.7,
        813.0, 813.1, 813.2, 813.3, 813.4, 813.5, 813.6, 813.7,
        814.0, 814.1, 814.2, 814.3, 814.4, 814.5, 814.6, 814.7,
        815.0, 815.1, 815.2, 815.3, 815.4, 815.5, 815.6, 815.7,
        816.0, 816.1, 816.2, 816.3, 816.4, 816.5, 816.6, 816.7,
        817.0, 817.1, 817.2, 817.3, 817.4, 817.5, 817.6, 817.7,
        818.0, 818.1, 818.2, 818.3, 818.4, 818.5, 818.6, 818.7,
        819.0, 819.1, 819.2, 819.3, 819.4, 819.5, 819.6, 819.7,
        820.0, 820.1, 820.2, 820.3, 820.4, 820.5, 820.6,
        821.0, 821.1, 821.2, 821.3, 821.4, 821.5, 821.6,
        822.0, 822.1, 822.2, 822.3, 822.4, 822.5, 822.6, 822.7,
        823.0, 823.1, 823.2, 823.3, 823.4, 823.5, 823.6, 823.7,
        824.0, 824.1, 824.2, 824.3, 824.4, 824.5, 824.6, 824.7,
        825.0, 825.1, 825.2, 825.3, 825.4, 825.5, 825.6, 825.7,
        826.0, 826.1, 826.3, 826.4,
        827.0, 827.3, 827.4,
        828.0, 828.4,
        829.0, 829.1, 829.2, 829.3, 829.4]

# subset to road injuries
road = injury[injury['CAUSE1'].isin(icd9) | injury['CAUSE2'].isin(icd9) | injury['CAUSE3'].isin(icd9)]

# data with age start, age end, and age group id
# change this to get_age_metadata
age_dat = pd.DataFrame({'age_start': [1] + range(5, 100, 5),
                        'age_end': [4] + range(9, 99, 5) + [125],
                        'age_group_id': range(5, 21, 1) + range(30, 33, 1) + [235]})


def assign_age(data):
    age_map_start = pd.Series(age_dat['age_start'].values,
                              pd.IntervalIndex.from_arrays(age_dat['age_start'], age_dat['age_end'], closed='both'))
    age_map_end = pd.Series(age_dat['age_end'].values,
                            pd.IntervalIndex.from_arrays(age_dat['age_start'], age_dat['age_end'], closed='both'))
    data['age_start'] = data['age'].map(age_map_start)
    data['age_end'] = data['age'].map(age_map_end)
    data.drop('age', axis=1, inplace=True)
    return (data)


road.rename(columns={'AGE': 'age', 'SEX': 'sex_id', 'YEAR': 'year_id'}, inplace=True)
road = assign_age(road)

# calculate the number of people admitted to the hospital for each age/sex/year combination
admit = road.groupby(['sex_id', 'age_start', 'age_end', 'year_id'])['ADMITHOS'].sum()
admit = admit.reset_index()
admit.rename(columns={'ADMITHOS': 'admitted'}, inplace=True)

# calculate the total number of people for each age/sex/year/combination
total = road.groupby(['sex_id', 'age_start', 'age_end', 'year_id'])['ADMITHOS'].count()
total = total.reset_index()
total.rename(columns={'ADMITHOS': 'total'}, inplace=True)

props = pd.merge(admit, total, on=['sex_id', 'age_start', 'age_end', 'year_id'])

# get proportion of people with road injury who were admitted to a hospital for inpatient care
props['proportion'] = props['admitted'] / props['total']

props.to_csv('FILEPATH', index=False)


"""
Created on Wed Apr 12 10:37:58 2017
Start formatting Nepal hospital data
just get it to a place where we can start running the proper formatting
script
@author: USERNAME
"""
import pandas as pd
import platform


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"



fpath = r"FILEPATH"

v_names = ['year', 'disease_code', 'disease_name', 'otp_total_cases', 'otp_female_cases',
              'otp_total_deaths', 'otp_female_deaths', 'inp_total_cases', 'inp_female_cases',
              'inp_total_deaths', 'inp_female_deaths', 'inp_under15_cases', 'inp_under5_cases',
              'inp_under15_deaths', 'inp_under5_deaths', 'code_of_area', 'sheetname']
tq_names = ['year', 'disease_code', 'disease_name', 'otp_total_cases', 'otp_female_cases',
              'otp_total_deaths', 'otp_female_deaths', 'inp_total_cases', 'inp_female_cases',
              'inp_total_deaths', 'inp_female_deaths', 'inp_under15_cases', 'inp_under5_cases',
              'inp_under15_deaths', 'inp_under5_deaths', 'sheetname']


sheets = ['MacV1', 'MacV2', 'MacV3', 'MacV4', 'MacV5', 'MacV6']
datlist = []
for sheet in sheets:
    dat = pd.read_excel(fpath,
                         sheetname=sheet)
    dat['sheetname'] = sheet
    datlist.append(dat)
mdf = pd.concat(datlist)
mdf.columns = v_names

mtq = pd.read_excel(fpath, sheetname="MacTQ")
mtq['sheetname'] = 'MacTQ'
mtq.columns = tq_names



sheets = ['ChetV1', 'ChetV2', 'ChetV3', 'ChetV4', 'ChetV5', 'ChetV6']
datlist = []
for sheet in sheets:
    dat = pd.read_excel(fpath,
                         sheetname=sheet)
    dat['sheetname'] = sheet
    datlist.append(dat)
cdf = pd.concat(datlist)
cdf.columns = v_names

ctq = pd.read_excel(fpath, sheetname="ChetTQ")
ctq['sheetname'] = 'ChetTQ'
ctq.columns = tq_names

counter = 0
same = 0
diff = 0
for disease in cdf.disease_code.unique():
    counter+=1
    if ctq[ctq.disease_code == disease].shape[0] == 0:
        continue
    if cdf[cdf.disease_code == disease].inp_total_cases.sum() != ctq[ctq.disease_code==disease].inp_total_cases.sum():
        print("This isn't a total for ", disease)
        diff += 1
    if cdf[cdf.disease_code == disease].inp_total_cases.sum() == ctq[ctq.disease_code==disease].inp_total_cases.sum():
        print("This IS a total for ", disease)
        same+=1








age_groups = ['<28 days', '29 days - 1', '1 4', '5 14', '15 19', '20 29',
              '30 39', '40 49', '50 59', '60 ']



age_sex_groups = []
for age in age_groups:
    fem = age + " female"
    ma = age + " male"
    age_sex_groups.append(fem)
    age_sex_groups.append(ma)


def name_cols(df):
    
    df = df.iloc[:, 0:22]
    
    df.columns = ['cause_code', 'icd_name'] + age_sex_groups
    return(df)

df10 = name_cols(df10)
df11 = name_cols(df11)
df13 = name_cols(df13)


df10 = df10.iloc[5:, :]
df11 = df11.iloc[3:, :]
df13 = df13.iloc[5:, :]


df10['year_start'] = 2010
df11['year_start'] = 2011
df13['year_start'] = 2013


def make_usable(df):
    
    for col in age_sex_groups:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df['0 1 male'] = df['<28 days male'] + df['29 days - 1 male']
    df['0 1 female'] = df['<28 days female'] + df['29 days - 1 female']
    df.drop(labels=['<28 days female', '<28 days male', '29 days - 1 female',
                    '29 days - 1 male'], axis=1, inplace=True)

    
    df = df.set_index(['cause_code', 'icd_name', 'year_start']).stack().\
        reset_index()
    df.rename(columns={'level_3': 'age_sex', 0: 'val'}, inplace=True)
    
    splits = df.age_sex.str.split(" ", expand=True)
    splits.columns = ['age_start', 'age_end', 'sex_id']
    
    df = pd.concat([df, splits], axis=1)
    
    df.sex_id.replace(['male', 'female'], [1, 2], inplace=True)
    df.age_end.replace([""], ["99"], inplace=True)
    
    df.drop(['age_sex', 'icd_name'], axis=1, inplace=True)
    
    df['year_end'] = df['year_start'] + 1
    return(df)

df_list = []
for df in [df10, df11, df13]:
    df_list.append(make_usable(df))

df = pd.concat(df_list)

df.to_hdf(r"FILEPATH",
          key="df")

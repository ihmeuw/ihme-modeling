# -*- coding: utf-8 -*-
import pandas as pd
import platform

# Environment:
if platform.system() == "Linux":
    root = r"FILEPATH/j"
else:
    root = "J:"

# read in each spreadsheet

fpath = r"FILEPATH/01 Hospital data by 21or50 ICD-code group of diseases.xlsx"

v_names = ['year', 'disease_code', 'disease_name', 'otp_total_cases', 'otp_female_cases',
              'otp_total_deaths', 'otp_female_deaths', 'inp_total_cases', 'inp_female_cases',
              'inp_total_deaths', 'inp_female_deaths', 'inp_under15_cases', 'inp_under5_cases',
              'inp_under15_deaths', 'inp_under5_deaths', 'code_of_area', 'sheetname']
tq_names = ['year', 'disease_code', 'disease_name', 'otp_total_cases', 'otp_female_cases',
              'otp_total_deaths', 'otp_female_deaths', 'inp_total_cases', 'inp_female_cases',
              'inp_total_deaths', 'inp_female_deaths', 'inp_under15_cases', 'inp_under5_cases',
              'inp_under15_deaths', 'inp_under5_deaths', 'sheetname']

######## read Mac sheets
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


########## read Chet sheets
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

# age groups the data was provided in
age_groups = ['<28 days', '29 days - 1', '1 4', '5 14', '15 19', '20 29',
              '30 39', '40 49', '50 59', '60 ']
# append male ond female onto age groups
# in the correct order, female is always first
# order of these items is very important
age_sex_groups = []
for age in age_groups:
    fem = age + " female"
    ma = age + " male"
    age_sex_groups.append(fem)
    age_sex_groups.append(ma)


def name_cols(df):
    # keep just the first 21 columns. These contain counts of cases
    df = df.iloc[:, 0:22]
    # name the cols using list created above
    df.columns = ['cause_code', 'icd_name'] + age_sex_groups
    return(df)
# add correct column names
df10 = name_cols(df10)
df11 = name_cols(df11)
df13 = name_cols(df13)

# skip the first n rows which contain totals and col headers
df10 = df10.iloc[5:, :]
df11 = df11.iloc[3:, :]
df13 = df13.iloc[5:, :]

# add years as strings
df10['year_start'] = 2010
df11['year_start'] = 2011
df13['year_start'] = 2013


def make_usable(df):
    # convert what will be 'val' column of counts to numeric
    for col in age_sex_groups:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    # combine counts for under age 1
    df['0 1 male'] = df['<28 days male'] + df['29 days - 1 male']
    df['0 1 female'] = df['<28 days female'] + df['29 days - 1 female']
    df.drop(labels=['<28 days female', '<28 days male', '29 days - 1 female',
                    '29 days - 1 male'], axis=1, inplace=True)

    # reshape long to create age start, age end, sex and val columns
    df = df.set_index(['cause_code', 'icd_name', 'year_start']).stack().\
        reset_index()
    df.rename(columns={'level_3': 'age_sex', 0: 'val'}, inplace=True)
    # turn column of age start, age end, sex values into separate columns
    splits = df.age_sex.str.split(" ", expand=True)
    splits.columns = ['age_start', 'age_end', 'sex_id']
    # append back onto data
    df = pd.concat([df, splits], axis=1)
    # clean values
    df.sex_id.replace(['male', 'female'], [1, 2], inplace=True)
    df.age_end.replace([""], ["99"], inplace=True)
    # drop unneeded columns
    df.drop(['age_sex', 'icd_name'], axis=1, inplace=True)
    # year end col is year start + 1
    df['year_end'] = df['year_start'] + 1
    return(df)

df_list = []
for df in [df10, df11, df13]:
    df_list.append(make_usable(df))

df = pd.concat(df_list)

df.to_hdf(r"FILEPATH/NPL_HID.H5",
          key="df")

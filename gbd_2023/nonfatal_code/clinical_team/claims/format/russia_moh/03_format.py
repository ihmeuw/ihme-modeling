# This script transforms the data from Russian into English using maps
# provided and assigns information relevant to the Epi schema
# before saving final extracted incidence & prevalence data separately.

import pandas as pd
import numpy as np
import re
from pathlib import Path

repo = 'FILEPATH'

# read in cause and location translation maps
loc_map = pd.read_excel(Path(repo).joinpath('locations_general.xlsx'))
cause_map = pd.read_excel(Path(repo).joinpath('causes.xlsx'))

# read in unformatted file
unformatted = pd.read_csv('FILEPATH/unformatted.csv')
df = unformatted.copy()

###############
# EPI COLUMNS #
###############
# val
df.rename({'0':'val'}, axis='columns', inplace=True)
df['val'] = df['val'].str.replace(',', '.')
df['val'] = df['val'].replace('(\s)*(-|=)', np.nan, regex=True) # any null lines
df['val'] = df['val'].str.lstrip('-') # any numbers with trailing -
df['val'] = df['val'].replace('', np.nan)
df['val'] = df['val'].astype(np.float)

# metric
df['metric_id'] = np.nan
df.loc[df['metric'].str.contains('абсолютные'), 'metric_id'] = 1
df.loc[df['metric'].str.contains('100 000'), 'metric_id'] = 3

# age start and end
df['age_start'] = np.nan
df['age_end'] = np.nan
df.loc[df['population'].str.contains('всего'), ['age_start', 'age_end']] = [0, 99]
df.loc[df['population'].str.contains('взрослые'), ['age_start', 'age_end']] = [18, 99]
df.loc[df['population'].str.contains('0 – 14'), ['age_start', 'age_end']] = [0, 14]
df.loc[df['population'].str.contains('15 – 17'), ['age_start', 'age_end']] = [15, 17]
df.loc[df['population'].str.contains('старше трудоспособного возраста'), ['age_start', 'age_end']] = [999, 99]

# fix location typos then translate using mapping file
df['location'] = df['location'].str.replace('гор\.', 'Город', regex=True)
df['location'] = df['location'].str.replace('Р\.', 'Республика', regex=True)
df['location'] = df['location'].str.replace('авт\.окр\.', 'авт. округ', regex=True)
df['location'] = df['location'].str.replace('авт\.округ', 'авт. округ', regex=True)
df['location'] = df['location'].str.replace('упр\.\s\s', 'упр. ', regex=True)
df['location'] = df['location'].str.replace('\+', '', regex=True)
df['location'] = df['location'].str.strip()
df = df.merge(loc_map, how='left', on=['location'])

# sex
df['sex_id'] = 3

# year start and end
df.loc[df['year'] == '2014  **)', 'year'] = 2014 # this is a typo
df['year'] = df['year'].astype(np.int)
df['year_start'] = df['year']
df['year_end'] = df['year']

# remove extra spaces and punctuation from cause names
df['cause'] = df['cause'].str.lower()
df['cause'] = df['cause'].str.replace('(\s)*(\*)?(\*)(\))(\s)*$', '', regex=True) # **) and *) but not ) 
df['cause'] = df['cause'].str.replace('из него: ', '', regex=True)
df['cause'] = df['cause'].str.replace('из них: ', '', regex=True)
df['cause'] = df['cause'].str.replace('(\s)+\s', ' ', regex=True) # more than 1 space
df['cause'] = df['cause'].str.replace('(\s)*(\[)(\s)*', ' (', regex=True) # brackets
df['cause'] = df['cause'].str.replace('(\s)*(\])(\s)*', ') ', regex=True) # brackets
df['cause'] = df['cause'].str.strip()

##################
# SPECIAL CAUSES #
##################
# salpingitis
df.loc[df['cause'].str.contains('сальпингит'), 'sex_id'] = 2
df.loc[df['cause'].str.contains('сальпингит') & (df['age_start'] == 0), 'age_start'] = 10

# menstrual
df.loc[df['cause'].str.contains('менструаций'), 'sex_id'] = 2
df.loc[df['cause'].str.contains('менструаций') & (df['age_start'] == 0), 'age_start'] = 10
df.loc[df['cause'].str.contains('менструаций') & (df['age_end'] == 99), 'age_end'] = 49

# pregnancy
df.loc[df['cause'].str.contains('беременность'), 'sex_id'] = 2
df.loc[df['cause'].str.contains('беременность') & (df['age_start'] == 0), 'age_start'] = 10
df.loc[df['cause'].str.contains('беременность') & (df['age_end'] == 99), 'age_end'] = 49

# prostate
df.loc[df['cause'].str.contains('предстательной'), 'sex_id'] = 1

# breast sysplasia
df.loc[df['cause'].str.contains('доброкачественная дисплазия'), 'sex_id'] = 2

# male infertility
df.loc[df['cause'].str.contains('мужское бесплодие'), 'sex_id'] = 1

# inflammatory pelvic organs
df.loc[df['cause'].str.contains('воспалительные болезни женских тазовых органов'), 'sex_id'] = 2

# endometriosis
df.loc[df['cause'].str.contains('эндометриоз'), 'sex_id'] = 2

# female infertility
df.loc[df['cause'].str.contains('женское бесплодие'), 'sex_id'] = 2
df.loc[df['cause'].str.contains('женское бесплодие') & (df['age_start'] == 0), 'age_start'] = 10
df.loc[df['cause'].str.contains('женское бесплодие') & (df['age_end'] == 99), 'age_end'] = 49

# ovarian dysfunction
df.loc[df['cause'].str.contains('яичников'), 'sex_id'] = 2

# testicular dysfunction
df.loc[df['cause'].str.contains('яичек'), 'sex_id'] = 1

# erosion of cervix
df.loc[df['cause'].str.contains('шейки матки'), 'sex_id'] = 2

# sex-split causes have ages
df.loc[(df['sex_id'] == 2) & (df['age_start'] == 999), 'age_start'] = 55
df.loc[(df['sex_id'] == 1) & (df['age_start'] == 999), 'age_start'] = 60

# any remaining 999's should not be included
df.loc[df['age_start'] == 999, 'age_start'] = np.nan

######################
# PREPARING TO WRITE #
######################
# use the map to translate causes
df = df.merge(cause_map, how='left', on=['cause'])

# format the columns and drop blank values
df = df[['location_name', 'location_id', 'ihme_loc_id', 'year_start', 'year_end', 
         'sex_id', 'age_start', 'age_end', 'metric_id', 'measure_id', 'cause', 'cause_english', 'val']]
df.dropna(subset=['val', 'location_id', 'age_start'], inplace=True)

# subset the data into prevalence and incidence data then write separately
inc = df.loc[df['measure_id'] == 6]
prev = df.loc[df['measure_id'] == 5]

inc.to_csv('FILEPATH/russia_incidence_counts_and_rates_2018_2019.csv', index=False, encoding='utf-8-sig')

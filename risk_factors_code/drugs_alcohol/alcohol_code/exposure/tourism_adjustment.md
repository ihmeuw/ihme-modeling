
# Extract UNWTO Tourist estimates and use them to create adjustment factors for Alcohol LPC

### 1. Set up global variables and file locations


```python
%load_ext rpy2.ipython

import pandas as pd
from pandas import DataFrame
import string
import numpy as np
import statsmodels.api as sm
import os
from multiprocessing import Pool

#Starting datasets
raw = {}

visitors_by_residence = {}
visitors_by_nationality = {}
tourists_by_residence = {}
tourists_by_nationality = {}
filtered_by_cp = {}

proportion_datasets = [['visitors_by_nationality', visitors_by_nationality], ['visitors_by_residence', visitors_by_residence], ['tourists_by_nationality', tourists_by_nationality], ['tourists_by_residence', tourists_by_residence]]
years = list(range(1980,1995,1) + [2015,2016])

##File locations
incoming = ''
outgoing = ""

template = pd.read_stata('')
template.rename(columns={'iso3':'ihme_loc_id'}, inplace=True)
template.drop_duplicates(['ihme_loc_id', 'year_id'])

alcohol_lpc = ''

#Variables used to handle draws later from alc lpc.
draws = []
for i in range(1000):
    draws.append('draw_{}'.format(i))

draws_old = []
for i in range(1000):
    draws_old.append('draw_{}_old'.format(i))

rename = dict(zip(draws, draws_old))
```

### 2. Read files from incoming folder, set name of dataframe to specific countries and store in dictionary.



```python
for files in os.listdir(incoming):
    if files.lower().endswith('.xlsx'):
        name = files.split('UNWTO_COMPENDIUM_TOURISM_STATISTICS_1995_2014_')
        name = name[1].split('_Y2016M01D12.XLSX')
        name = name[0].replace("_", " ")
        name = name.lower()
        name = string.capwords(name)
        data = pd.read_excel(incoming + '/' + files, sheetname=None, header=3, skiprows=2, na_values= ['..', '', 0], keep_dUSERt_na = False)
        raw[name] = data

#Make function to fix UNWTO location names not conforming to GBD location names
def fix_locations(data, more=False):
    location_fix = {'Antigua And Barbuda':'Antigua and Barbuda', 'Bahamas':'The Bahamas', 'Bolivia, Plurinational State Of': 'Bolivia', 'Bosnia And Herzegovina':'Bosnia and Herzegovina', 'Brunei Darussalam':'Brunei', 'Congo, Democratic Republic Of The':'Democratic Republic of the Congo', 'Cote D Ivoire':"Cote d'Ivoire", 'Gambia':'The Gambia', 'Guinea-bissau':'Guinea-Bissau', 'Hong Kong, China':'Hong Kong Special Administrative Region of China', 'Iran, Islamic Republic Of':'Iran', 'Korea, Republic Of':'South Korea', 'Macao China':'Macao Special Administrative Region of China', 'Micronesia, Federated States Of':'Federated States of Micronesia', 'Russian Federation':'Russia', 'Saint Vincent And The Grenadines':'Saint Vincent and the Grenadines', 'Sao Tome And Principe':'Sao Tome and Principe', 'State Of Palestine':'Palestine', 'Syrian Arab Republic':'Syria', 'Tanzania, United Republic Of':'Tanzania', 'Timor Leste':'Timor-Leste', 'Trinidad And Tobago':'Trinidad and Tobago', 'United States Of America':'United States', 'United States Virgin Islands':'Virgin Islands, U.S.', 'Venezuela, Bolivarian Republic Of':'Venezuela', 'Viet Nam':'Vietnam'}
    visitors_fix = {'Bahamas':'The Bahamas', 'Belgium / Luxembourg':'Belgium', 'Bolivia, Plurinational State of': 'Bolivia', 'Brunei Darussalam':'Brunei', 'China + Hong Kong, China':'China', 'Congo, Democratic Republic of the':'Democratic Republic of the Congo', "Côte d'Ivoire":"Code d'Ivoire", 'Czech Republic/Slovakia':'Czech Republic', 'Gambia':'The Gambia', 'Hong Kong, China':'Hong Kong Special Administrative Region of China', 'India, Pakistan':'India', 'Iran, Islamic Republic of':'Iran', 'Korea, Republic of':'South Korea', "Korea, Democratic People's Republic of":'South Korea', "Lao People's Democratic Republic":'Laos', 'Macao, China':'Macao Special Administrative Region of China', 'Micronesia, Federated States of':'Federated States of Micronesia', 'Russian Federation':'Russia', 'Spain,Portugal':'Spain', 'State of Palestine':'Palestine', 'Syrian Arab Republic':'Syria', 'United Kingdom/Ireland':'United Kingdom', 'Taiwan Province of China':'Taiwan', 'Tanzania, United Republic of':'Tanzania', 'United States of America':'United States', 'United States Virgin Islands':'Virgin Islands, U.S.', 'Venezuela, Bolivarian Republic of':'Venezuela', 'Viet Nam':'Vietnam'}
    for unwto, gbd in location_fix.items():
        data['location_name'][data['location_name'] == unwto] = gbd
    if more == True:
        for unwto, gbd in location_fix.items():
            data['visiting_country'][data['visiting_country'] == unwto] = gbd
        for unwto, gbd in visitors_fix.items():
            data['visiting_country'][data['visiting_country'] == unwto] = gbd
    return data
```

### 3. Read specific sheets from raw datasets and store in filtered dictionaries


```python
for country, dataset in raw.items():
    for sheet, data in dataset.items():
        if sheet == '121':
            proportion_datasets[0][1][country] = data
        if sheet == '122':
            proportion_datasets[1][1][country] = data
        if sheet == '111':
            proportion_datasets[2][1][country] = data
        if sheet == '112':
            proportion_datasets[3][1][country] = data
        if sheet == 'CP':
            filtered_by_cp[country] = data
```

### 4. Append filtered dictionaries, then pivot to create individual variables. Then transform to proportions.



```python
def merge_filtered(filtered, data_name):
    '''Extract data from filtered dictionaries.
       Only keeps data on countries and relevant variables. Renames variables to
       match template.
    '''
    copy = DataFrame()
    merger = DataFrame()
    frames = {'total':[], 'visiting':[]}

    for country, host in filtered.items():

        #Only keep countries not regions, as well as useful indicators
        host_clean = host[(host['CODE'] < 900) | (host['CODE'].isnull())]
        host_clean = host_clean.drop(['CODE', '% Change 2014-2013', 'Notes', 'NOTES'], axis=1)

        #Rename columns and convert to long
        host_clean = host_clean.rename(columns = {'Unnamed: 2':'visiting_country'})

        #Get rid of pesky marketshare category
        host_clean = host_clean.iloc[:,:-1]

        host_clean['location_name'] = '{}'.format(country)
        host_clean = pd.melt(host_clean, id_vars=['REGION', 'location_name', 'visiting_country'], var_name='year_id', value_name=data_name)

        #Keep total separate from visiting countries
        frames['total'].append(host_clean[host_clean['visiting_country'] == 'TOTAL'])
        frames['visiting'].append(host_clean[host_clean['visiting_country'] != 'TOTAL'])

    #Merge visiting countries for host, keeping total separate
    merger = pd.concat(frames['total'], ignore_index=True)
    copy = pd.concat(frames['visiting'], ignore_index=True)

    #Merge location specific with totals
    merger = merger.drop(['visiting_country', 'REGION'], axis=1)
    merger = merger.rename(columns = {data_name:'Total'})
    copy = pd.merge(copy, merger, how='left', on=['location_name', 'year_id'], sort=False)

    #Make sure missing observations are coded as NaN and that only countries are kept, not NaN
    for row in copy.index:
        if type(copy.iloc[row, -1]) == str:
            copy.iloc[row, -1] = np.nan
        if type(copy.iloc[row, -2]) == str:
            copy.iloc[row, -2] = np.nan

    #Get rid of countries with name NaN
    copy = copy[copy['visiting_country'] == copy['visiting_country']]
    return copy

proportion_datasets_clean=[]

#Merge each category group from data extracted
for dataset in range(len(proportion_datasets)):
    proportion_datasets_clean.append(merge_filtered(proportion_datasets[dataset][1], '{}'.format(proportion_datasets[dataset][0])))

    #Generate logged tourist proportions
    proportion_datasets_clean[dataset]['tourist_proportion'] = np.log(proportion_datasets_clean[dataset].iloc[:,-2]/proportion_datasets_clean[dataset].iloc[:,-1])
    proportion_datasets_clean[dataset] = proportion_datasets_clean[dataset][['location_name', 'visiting_country', 'year_id', 'tourist_proportion']]
    proportion_datasets_clean[dataset].sort_values(['location_name', 'visiting_country', 'year_id'], inplace=True)

#Merge all proportions on together    
i=1
for data in proportion_datasets_clean:
    data.rename(columns={'tourist_proportion':'tourist_proportion_{}'.format(i)}, inplace=True)
    data.set_index(['location_name', 'visiting_country', 'year_id'], inplace=True)
    i+=1

combine = pd.concat(proportion_datasets_clean, axis=1, join='outer')
combine['tourist_proportions'] = combine.mean(axis=1)
combine.reset_index(inplace=True)
combine = combine[['year_id', 'location_name', 'visiting_country', 'year_id', 'tourist_proportions']]
#combine.to_csv("FILEPATH", index=False)
```

### 5. Estimate full time series for tourist_proportions using amelia & mice regression

See R Code  
Proportion datasets were concatted together after running above cells.


```python
%%R

library(data.table)
library(plyr)

dir <- ""

#Load tourist data
df <- fread(paste0(dir, "tourist_proportions.csv"))
df[, id:=paste(df$location_name, df$visiting_country, sep=",")]

#Load covariate data
covar <- fread(paste0(dir, "covariates.csv"))
covar[, cigarettes_pc:=NULL]

#Load alcohol predictions
alc <- fread(paste0(dir, "alc.csv"))
alc <- melt(alc, id.vars=c("location_id", "location_name", "year_id", "me_name"))
alc <- alc[, mean(value, na.rm=T), by=c("location_name", "year_id")]
setnames(alc, "V1", "alc_lpc")

#Combine covariates with alcohol, merge onto tourist data.
predictors <- join(covar, alc, by=c("location_name", "year_id"), type="left")
predictors[, data:=NULL]
predictors <- predictors[!duplicated(predictors),]

final <- join(df, predictors, by=c("location_name", "year_id"), type="left")

#Make location id
locations <- data.table(location_id = seq(1, length(unique(final$location_name))), location_name = unique(final$location_name))

final <- join(final, locations, by="location_name", type="left")

#Overimpute for observed cases, parallelized on cluster
write.csv(final, paste0(dir, "final.csv"))
R_shell <- paste0(dir,"r_shell.sh")
error_path <- paste0()

for (loc in locations$location_id){
  
  #Build qsub
  name <- paste0("impute_case_",loc)
  script <- paste0(dir,"impute.R")
  arguments <- paste(dir, loc)
  
  qsub <- paste0("qsub -N ", name, 
                 " -pe multi_slot ", 2,
                 " -l mem_free=", 4,
                 error_path
                 )
  #Run qsub
  system(paste(qsub, R_shell, script, arguments))
  #print(paste(qsub, R_shell, script, arguments))
}
```


```python
%%R

#Loop through cases

library(data.table)
library(Amelia)

arg <- commandArgs()[-(1:3)]

dir <- arg[1]
loc <- arg[2]

print(loc)
print(dir)

df <- fread(paste0(dir, "final.csv"))
df[, V1:=NULL]
df <- df[location_id==loc,]

#Check that covariates exist
for (covariate in c("ln_ldi_pc", "educ_25plus", "alc_lpc")){
  if (nrow(df[is.na(get(covariate)), ])>=0){
    df[, (covariate):=NULL]
  }
}

a <- amelia(df, m=10, 
            cs="id", 
            ts="year_id", 
            p2s = 2,
            idvars=c("location_name", "visiting_country", "location_id"), 
            polytime = 1, 
            intercs = T,
            empri = 0.01*dim(df)[1],
            parallel = "multicore")

write.amelia(a, separate = FALSE, orig.data = TRUE, file.stem = paste0(dir, "imputed_", loc), impvar = "imputed", extension = ".csv", format="csv")

```


```python
%load_ext rpy2.ipython
```


```python
%%R

library(data.table)
library(plyr)

folder <- ""
setwd(folder)

#Read in only the files we want and append together in a datatable
files <- list.files(folder, pattern="^imputed.*csv$")
impute <- lapply(files, fread)
impute <- data.table(rbindlist(impute))

#Remove some columns and transform data
impute[, c("V1", "id", "location_id"):=NULL]
impute[, tourist_proportions:=exp(tourist_proportions)]

#Scale proportions to 1
impute[, tourist_proportions := tourist_proportions/sum(.SD[,tourist_proportions], na.rm=TRUE), 
           by=list(location_name, year_id, imputed)]

#Split data into raw data and estimates
data <- impute[imputed==0,]
data[, imputed:=NULL]

impute <- impute[imputed!=0,]

#Calculate estimates and uncertainty
impute[, `:=`(mean = mean(tourist_proportions),
             lower = quantile(tourist_proportions, 0.05),
             upper = quantile(tourist_proportions, 0.95)),
       by=.(location_name, visiting_country, year_id)]

#Collapse dataset and compare to original data
impute[, c("tourist_proportions", "imputed"):=NULL]
impute <- unique(impute)

data[, imputed:=NULL]
results <- join(data, impute, by=c("location_name", "visiting_country", "year_id"), type="full")

write.csv(results, "")
```


```python
#Combine all of the best datasets for each country pair. 

#Replace frames with Amelia output
tourist_proportions = pd.read_csv("")

#Format so that dataset merges correctly with later datasets
fix = fix_locations(tourist_proportions, more=True)
fix.rename(columns={'location_name':'host', 'visiting_country':'location_name'}, inplace=True)
fix = pd.merge(fix, template, how='left', on=['location_name', 'year_id'])
fix = fix[['year_id', 'host', 'location_name', 'mean', 'lower', 'upper', 'location_id']]
fix.rename(columns={'host':'location_name', 'location_name':'visiting_country', 'location_id':'location_id_visitor'}, inplace=True)

tourist_proportions = fix
```

### 6. Split cp by filtered proportions predictions to produce full time series for tourism for all countries



```python
games = []

def merge_cp(data, country):
    '''Returns cleaned dataset for total tourism'''

    #Only keep relevant variables, rename, and transform the units.
    copy = data.iloc[[4, 5, 6, 54]]
    copy = copy.drop(['Cod.', 'Notes', 'Units'], axis=1)
    copy.iloc[0,0] = 'tourist_total'
    copy.iloc[1,0] = 'overnight_visitors'
    copy.iloc[2,0] = 'same_day_visitors'
    copy.iloc[3,0] = 'length_of_stay'
    copy.iloc[:-1,1:] = copy.iloc[:-1,1:]*1000

    #Reshape by years, then make separate columns
    copy = pd.melt(copy, id_vars=['Basic data and indicators'], var_name='year_id', value_name='data')
    copy = copy.pivot(index='year_id', columns='Basic data and indicators', values='data')
    copy['location_name'] = country
    copy['year_id'] = copy.index

    #Replace almost all missing values with next best estimates
    copy = next_best(copy)
    return copy

def next_best(data):
    '''Replaces tourist total with next best estimate if tourist total is missing'''

    if np.isnan(data['tourist_total'].values).sum() >= 19:
        data['tourist_total'] = data['overnight_visitors']
    if np.isnan(data['tourist_total'].values).sum() >= 19:
        data['tourist_total'] = data['same_day_visitors']
    return data

for country, data in filtered_by_cp.items():
    games.append(merge_cp(data, country))

#Merge on stgpr template
tourist_total = pd.concat(games, ignore_index=True)
tourist_total = fix_locations(tourist_total)
tourist_total_gpr = pd.merge(tourist_total, template, how='right', on=['location_name', 'year_id'])
tourist_total_gpr['year_id'] = tourist_total_gpr['year_id'].astype(int)
```


```python
#Free up memory by deleting some variables not needed

del tourist_total_gpr
del fix
```

### 7. Prep tourist_total for ST-GPR


```python
def lowess(data, fraction):
    '''Calculates lowess and returns predictions'''

    x = data['year_id']
    y = data['data']

    prediction = sm.nonparametric.lowess(y, x, frac=fraction, it=10, missing='drop', return_sorted=False)

    return(prediction)

#Rename columns for gpr
tourist_total_gpr.rename(columns={'tourist_total':'data'}, inplace=True)

#Generate variance and SD using difference from lowess estimates
grouped = tourist_total_gpr.groupby('ihme_loc_id')
dames = []

for country, data in grouped:

    #Only run lowess for models with atleast 2 data points. For those with a small amount, use all of the data.
    check = data['data'].values
    check = np.isnan(check)

    if sum(~check) >= 7:
        lowess_hat = lowess(data, .6)
        data['lowess_hat'] = lowess_hat
        dames.append(data)
    if sum(~check) >= 4 and sum(~check) <= 6:
        lowess_hat = lowess(data, 1)
        data['lowess_hat'] = lowess_hat
        dames.append(data)

lowess_hat = pd.concat(dames, ignore_index=True)
lowess_hat = lowess_hat[['location_name', 'year_id', 'data', 'lowess_hat']]
lowess_hat.sort_values(['location_name', 'year_id'], inplace=True)

#Collect lowess predictions with gpr prep dataset
lowess_hat = lowess_hat[['location_name', 'year_id', 'lowess_hat']]
gpr = pd.merge(tourist_total_gpr, lowess_hat, on=['location_name', 'year_id'], how='left')

gpr['residual'] = gpr['lowess_hat'] - gpr['data']

#By country, use difference between lowess estimates and data to generate variance over a 5 year window
grouped = gpr.groupby('location_name')
frames=[]

for location, data in grouped:
    data['standard_deviation'] = pd.rolling_std(data['residual'], window=5, center=True, min_periods=1)
    frames.append(data)

gpr = pd.concat(frames, ignore_index=True)

#Only hold onto variance at points where we have data. (This happens due to the rolling window)
gpr['standard_deviation'][gpr['residual'] != gpr['residual']] = np.nan
gpr['variance'] = gpr['standard_deviation']**2

gpr['constant'] = 1

#Add missing China subnational
china = template[template['location_name']=='China']
china['ihme_loc_id'] = 'CHN_44533'
china['location_id'] = 44533
china['location_name'] = 'CHN_44533'
gpr = pd.merge(gpr, china, on=['location_id', 'year_id'], how='left')

#Add on last columns needed for gpr
gpr['nid'] = 239757
gpr['me_name'] = 'total_tourists'
gpr['sample_size'] = np.nan
gpr['sex_id'] = 3
gpr['age_group_id'] = 22
gpr['age_id'] = 22

gpr.to_csv(r'')
```

### 8. Bring in GPR results on LPC and use this, along with transformed tourism data, to create tourism adjustments


The following equations describe the adjustment:

$$ \text{Adjusted LPC}_h = \text{Observed LPC}_h + \text{LPC}_\textit{a} - \text{LPC}_v $$

$$ \text{LPC}_a = \frac{\sum_v (\text{Unadjusted LPC}_h * \text{Tourist pop}_\text{a,v})}{\text{Population}_h}$$

$$ \text{LPC}_v =  \frac{\sum_v (\text{Unadjusted LPC}_v * \text{Tourist pop}_\text{h,v})}{\text{Population}_h}$$

$$ \text{Tourist pop}_\text{i,v} = \text{Proportion of tourists}_{i,v} * \text{Tourist pop}_v * \frac{\text{Trip duration}_{i,v}}{365} \; \; \text{for }i={a,h}$$  

where h is a hosting country, a is citizens from a hosting country traveling abroad, and v is a visiting country.


```python
#Read in alcohol lpc gpr results and organize columns

alc_lpc = pd.read_csv("")
alc_lpc = alc_lpc[['year_id','location_id']+draws]
alc_lpc = pd.merge(alc_lpc, template, how='left', on=['location_id', 'year_id'])
alc_lpc.rename(columns={'location_name':'visiting_country', 'location_id':'location_id_visitor'}, inplace=True)
alc_lpc = pd.melt(alc_lpc, id_vars=['year_id', 'location_id_visitor', 'visiting_country'], value_vars=draws, var_name="draw", value_name="alc_lpc")

#Read in tourist proportions and merge with alc lpc
tourism_statistics = pd.merge(alc_lpc, tourist_proportions, how='left', on=['visiting_country', 'location_id_visitor', 'year_id'])
tourism_statistics = tourism_statistics[['year_id', 'location_name', 'mean', 'location_id_visitor', 'visiting_country', 'draw', 'alc_lpc']]
tourism_statistics.sort_values(['location_name', 'year_id', 'location_id_visitor'], inplace=True)
tourism_statistics['year_id'] = tourism_statistics['year_id'].astype(int)
tourism_statistics.rename(columns={'mean':'tourist_proportion'}, inplace=True)

#Read in total tourists

total_tourists = pd.read_csv(')
total_tourists = total_tourists[['year_id', 'location_id']+draws]
total_tourists = pd.melt(total_tourists, id_vars=['year_id', 'location_id'], value_vars=draws, var_name="draw", value_name="total_tourists")
total_tourists = pd.merge(total_tourists, template, on=['location_id', 'year_id'], how='left')
total_tourists = total_tourists[['location_id', 'location_name', 'year_id', 'draw', 'total_tourists']]

#Calculate trip duration/365, merge onto total_tourists

length = tourist_total[['length_of_stay', 'location_name', 'year_id']]
total_tourists = pd.merge(total_tourists, length, on=['location_name', 'year_id'], how='left')
total_tourists['length_of_stay'][total_tourists['length_of_stay'] != total_tourists['length_of_stay']] = 10
total_tourists['length_of_stay'] = total_tourists['length_of_stay']/365

#Get populations

pop = template[['location_name', 'year_id', 'pop_scaled', 'location_id']]
tourism_statistics = pd.merge(tourism_statistics, pop, on=['location_name', 'year_id'], how='left')

#Merge total tourists with tourist proportions and alcohol consumption
alc_lpc_tourists = pd.merge(tourism_statistics, total_tourists, on=['location_id', 'location_name', 'year_id', 'draw'], how='left')
del total_tourists

#Sort and rename columns to make operations below clearer:
tourism_inputs = alc_lpc_tourists[['location_name', 'location_id', 'visiting_country', 'location_id_visitor', 'year_id', 'draw', 'alc_lpc', 'total_tourists', 'tourist_proportion', 'length_of_stay']]
tourism_inputs.rename(columns={'location_name':'host_country', 'location_id':'location_id_host'}, inplace=True)

```

#Memory is getting large so delete some objects.

del tourist_proportions
del tourism_statistics
del alc_lpc_tourists
del tourist_total
del template
```


#Calculate tourist populations, numerators, and host populations
tourism_inputs['tourist_pop'] = (tourism_inputs['total_tourists']*tourism_inputs['length_of_stay']*tourism_inputs['tourist_proportion'])
tourism_inputs['numerator'] = tourism_inputs['alc_lpc']*(tourism_inputs['tourist_pop'])

#Calculate domestic citizen's consumption abroad (additive measure)
tourism_inputs['add'] = tourism_inputs.groupby(['location_id_visitor', 'visiting_country', 'year_id', 'draw'])['numerator'].transform(sum)
tourism_inputs = pd.merge(tourism_inputs, pop, how='left', left_on=['location_id_visitor', 'year_id'], right_on=['location_id', 'year_id'])

tourism_inputs['add'] = tourism_inputs['add']/tourism_inputs['pop_scaled']
tourism_inputs.drop(['location_name', 'location_id', 'pop_scaled'], axis=1, inplace=True)

#Calculate tourist consumption domestically (subtractive measure)
tourism_inputs['sub'] = tourism_inputs.groupby(['location_id_host', 'host_country', 'year_id', 'draw'])['numerator'].transform(sum)
tourism_inputs = pd.merge(tourism_inputs, pop, how='left', left_on=['location_id_host', 'year_id'], right_on=['location_id', 'year_id'])

tourism_inputs['sub'] = tourism_inputs['sub']/tourism_inputs['pop_scaled']
tourism_inputs.drop(['location_name', 'location_id', 'pop_scaled'], axis=1, inplace=True)

tourism_inputs.dropna(inplace=True)

add = tourism_inputs[['visiting_country', 'location_id_visitor', 'year_id', 'draw', 'add']].drop_duplicates()
sub = tourism_inputs[['host_country', 'location_id_host', 'year_id', 'draw', 'sub']].drop_duplicates()

del tourism_inputs

tourism_adjustments = pd.merge(add, sub, how='outer', left_on=['visiting_country', 'location_id_visitor', 'year_id', 'draw'] ,right_on=['host_country', 'location_id_host', 'year_id', 'draw'])
tourism_adjustments.drop(['visiting_country', 'location_id_visitor'], axis=1, inplace=True)

tourism_adjustments['net'] = tourism_adjustments['add'] - tourism_adjustments['sub']
tourism_adjustments = tourism_adjustments[['location_id_host', 'host_country', 'year_id', 'draw', 'net']]
```


```python
tourism_adjustments.to_csv("{}/tourism_adjustments.csv".format(outgoing), index=False)
```


```python
tourism_adjustments.loc[(tourism_adjustments.net != tourism_adjustments.net) & (tourism_adjustments.location_id_host == tourism_adjustments.location_id_host)]
```


```python
#%%Backcast average estimates of last three years to the years 1980-1995 to match the covariate, along with forecasting to 2016
#Runs in parallel 

from multiprocessing import Pool

tourism_adjustments = pd.read_csv('{}/tourism_adjustments.csv'.format(outgoing))
backcast = tourism_adjustments.groupby(['location_id_host','host_country', 'draw'])
                                        
def caster(params):
    
    keys = params[0]
    group = params[1]
    
    addon = pd.DataFrame({'year_id':years})
    addon['location_id_host'] = keys[0]
    addon['host_country'] = keys[1]
    addon['draw'] = keys[2]
    addon['net'] = group[group.year_id.isin([1995,1996,1997])]['net'].mean()
    addon.loc[(addon.year_id >= 2015), 'net'] = group[group.year_id==2014]['net'].values
    
    return(pd.concat([group, addon], ignore_index=True))

#Bind paramaters together (pool requires passing single arguments) based on keys of groupby and constituent dataframes
params = []
for keys, df in backcast:
    
    params.append((keys, df))

p = Pool(20)
final_adjustment = pd.concat(p.map(caster, params), ignore_index=True)

#Set some feasibility bounds, introduced by Euromonitor data
#final_adjustment.loc[(final_adjustment.net >= 1 ), 'net'] = 1
#final_adjustment.loc[(final_adjustment.net <= -5 ), 'net'] = -5
final_adjustment = final_adjustment.loc[final_adjustment.net == final_adjustment.net]
```




```

### 9. Combine with alc_lpc gpr results and export


#Load in alc lpc estimates, hold onto relevant columns and rows, melt to long
alc_lpc = pd.read_csv("")
alc_lpc = alc_lpc[['year_id','location_id']+draws]
alc_lpc = alc_lpc[alc_lpc['year_id'] >= 1990]
alc_lpc = pd.melt(alc_lpc, id_vars=['year_id', 'location_id'], var_name="draw", value_name="alc_lpc")

#Merge with adjustments, add in net. Only use adjustment if above 0 after applying adjustment (could not be the case due to assumptions above & poor data in alc lpc FAO)
adj_alc = pd.merge(alc_lpc, final_adjustment, how="left", left_on=["location_id", "year_id", "draw"], right_on=["location_id_host", "year_id", "draw"])
adj_alc['net'].fillna(0, inplace=True)
adj_alc['alc_lpc_new'] = adj_alc["alc_lpc"] + adj_alc["net"]
adj_alc.loc[(adj_alc.alc_lpc_new <= 0), 'alc_lpc_new'] = adj_alc.loc[(adj_alc.alc_lpc_new <= 0), 'alc_lpc']

adj_alc['alc_lpc'] = adj_alc['alc_lpc_new']

#Clean up dataframe and send off
adj_alc = adj_alc[['year_id', 'location_id', 'draw', 'alc_lpc', 'net']]
adj_alc.to_csv("{}/adj_alc_lpc_2016.csv".format(outgoing), index=False)

```


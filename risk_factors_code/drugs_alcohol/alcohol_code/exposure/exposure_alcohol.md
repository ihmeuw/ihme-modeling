
# Calculate exposure to alcohol in grams per day

This code collects the modeling outputs from the following constituent models (model id in parantheses):
   
   * Alcohol liters per capita (adjusted) (ST-GPR)
   * Alcohol g/day (3360)
   * Proportion current drinkers (3364)
   * Proportion former drinkers (3367)
   * Proportion abstainers (3365)
   * Proportion of current drinkers who binge drink (3366)
   * Proportion of drinking events that are binge events (3363)  
     
The liters per capita model sets the maximum level of consumption within a country. This is combined with current drinkers to determine the total amount consumed by drinkers. After squeezing current, former, and abstainers to sum to one, the total amount of consumption amongst drinkers is then scaled to each age/sex/subnational group using the patterns within the g/day model. This gives a final estimate of g/day amongst drinkers for every location/age/sex/year. Proportion of binge drinkers and proportion of drinking events are appended to the final dataset and exported to a central location for PAF calculation.  

Equations for each of the above steps are outlined in the relevant pieces of code below. 

Code is structured as follows:

1. Initial setup
2. Scaling & calculation of g/day
3. Appendix
    * Shell scripts
  
The first four steps need to be run, and the appendix is sourced throughout and included here for reference. Rerunning appendix cells will save over the pre-existing code.

## 1. Setup


```python
#Import packages

import sys                                                               #Change path to find custom functions
import os                                                                #Interface with Linux/Cluster
import pandas as pd                                                      #Easy dataframes
from db_queries import get_demographics_template                         #Shared functions to get demographics

#Set up directories, options, and desired final outputs

#Directories

version = 14

code_dir    = ""
input_dir   = "" 
output_dir  = ""

#Code & cluster options

debug    = False      #Runs for a single location, age, sex (as well as subnationals)
collapse = False      #This collapses final output to mean, lower, upper rather than draws. Useful for vetting results

    #Save collapsed separately from draws
if collapse == True:
    output_dir = output_dir+"/collapsed"

    #If folder for version doesn't exist, make it
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

project = "-P proj_custom_models"
shell   = "{}/python_shell.sh".format(code_dir)
errors    = ""
slots   = 13

#Needed final outputs, from shared function. Converted to comma-delimited string for qsub

wrap_string = lambda x: ",".join(map(str, x))
template  = get_demographics_template("mort")

sexes     = wrap_string(template.sex_ids.unique())
ages      = wrap_string(list(range(8,21,1)) + list(range(30,33,1)) + [235])
years     = wrap_string(list(range(1990,2017,1)))
locations = [4749] + list(range(4618,4627,1)) + list(range(44643,44793,1))

former_cap         = 0   #Cap on amount of former drinkers in a population. Rest are distributed to abstainers.
include_former     = True
include_unrecorded = True

#Submit jobs after building qsub

for location in locations:
    
    args = [debug, location, years, ages, sexes, version, input_dir, output_dir, collapse, former_cap, include_former, include_unrecorded]
    
    qsub = "qsub " + str(project) + " -l mem_free=" + str(slots*2) + " -pe multi_slot " + str(slots) \
            + " -N alc_exposure_" + str(location)  + " -o " + str(errors) + " -e " + str(errors) + " " \
            + str(shell) + " " + str(code_dir) + "/exposure_alcohol_scaling.py " + ' '.join(map(str, args))

    if debug == True:
        print(qsub)
        os.system(qsub)
        break
    else:
        os.system(qsub)
```

## 2. Scaling & calculation of g/day

Start by making sure that current drinkers, abstainers, and former drinkers sum to 1. Cap former drinkers at 5% and attribute the rest to abstainers.

Then, for each location, make sure that the subnational total amount of alcohol sums to the national total amount. After, make sure that each age/sex group is weighted appropriately using ratios from g/day model.

```python

import pandas as pd  
import sys
from transmogrifier.draw_ops import interpolate
from db_queries import get_population

#Read in arguments, first batch refer to what estimates to produce, second batch refer to where to find everything.
#Third batch are alcohol and GBD specific options

debug = eval(sys.argv[1])

if debug == False:

    location   = int(sys.argv[2])
    years      = [int(x) for x in sys.argv[3].split(",")]
    ages       = [int(x) for x in sys.argv[4].split(",")]
    sexes      = [int(x) for x in sys.argv[5].split(",")]

    version    = int(sys.argv[6])
    input_dir  = str(sys.argv[7])
    output_dir = str(sys.argv[8])
    collapse   = eval(sys.argv[9])
    
    former_cap         = float(sys.argv[10])
    include_former     = eval(sys.argv[11])
    include_unrecorded = eval(sys.argv[12])
    
    print(collapse)
    print(former_cap)
    
#Debug
else:
    input_dir = "" 
    location = 102
    ages = 10
    sexes = 1
    years = list(range(1990,2017,1))

#Load datasets and reduce to required columns. Reshape long.

lpc              = pd.read_csv("FILEPATH")
if include_unrecorded==True:
    unrecorded       = pd.read_csv("FILEPATH")
current_drinkers = interpolate("modelable_entity_id", 3364, "", age_group_ids=ages, location_ids=location, sex_ids=sexes, _n=1000, resample=False)
abstainers       = interpolate("modelable_entity_id", 3365, "", age_group_ids=ages, location_ids=location, sex_ids=sexes, _n=1000, resample=False)
former           = interpolate("modelable_entity_id", 3367, "", age_group_ids=ages, location_ids=location, sex_ids=sexes, _n=1000, resample=False)
gday             = interpolate("modelable_entity_id", 3360, "", age_group_ids=ages, location_ids=location, sex_ids=sexes, measure_ids=19, _n=1000, resample=False, version = 200915)
binge            = interpolate("modelable_entity_id", 3366, "", age_group_ids=ages, location_ids=location, sex_ids=sexes, _n=1000, resample=False)
binge_events     = interpolate("modelable_entity_id", 3363, "", age_group_ids=ages, location_ids=location, sex_ids=sexes, _n=1000, resample=False)

populations      = get_population(age_group_id = ages, location_id = location, year_id = years, sex_id = sexes)
populations      = populations[['location_id', 'year_id', 'sex_id', 'age_group_id', 'population']]

#Add dataframes to a list for easy operations later. 
alc = {'current_drinkers':current_drinkers, 
       'abstainers':abstainers, 
       'former':former, 
       'gday':gday, 
       'binge':binge, 
       'binge_events':binge_events}

def clean(df):
    return(df.drop(["measure_id", "modelable_entity_id"], axis=1, inplace=True))

def melter(df, key):
    return(pd.melt(df, id_vars = [x for x in df.columns if "draw" not in x], var_name="draw", value_name=key)) 

#For each dataset, remove some columns, melt to long, and merge together.

for key, df in alc.iteritems():
    alc[key] = clean(df)
    alc[key] = melter(df, key)
    
    #Merge dataframes onto each other, using the first as the base.
    if key != alc.keys()[0]:
        alc[alc.keys()[0]] = pd.merge(alc[alc.keys()[0]], alc[key], on=["location_id", "year_id", 
                                                                        "age_group_id", "sex_id", "draw"], how="left")      
del current_drinkers
del abstainers
del former
del gday
del binge
del binge_events

#Make final dataset from all of the merged datasets, merge on lpc and populations
alc = alc[alc.keys()[0]]

alc = pd.merge(alc, lpc, on=['year_id', 'location_id', 'draw'], how="left")
del lpc
if include_unrecorded==True:
    alc = pd.merge(alc, unrecorded, on=['location_id', 'draw'], how='left')
    
    #If no data on unrecorded, assume unrecorded is non-existent (i.e. adjustment factor = 1)
    alc.loc[pd.isnull(alc.unrecorded_rate), 'unrecorded_rate'] = 1
    
    del unrecorded
    
alc = pd.merge(alc, populations, on=['year_id', 'location_id', 'age_group_id', 'sex_id'], how='left')
del populations
        
#Scale current_drinkers, abstainers, and former drinkers to 1. Place cap on former drinkers and reapportion to abstainers

if include_former == True:
    alc["total"]            = alc.current_drinkers+alc.abstainers+alc.former
    alc["current_drinkers"] = alc.current_drinkers/alc.total
    alc["abstainers"]       = alc.abstainers/alc.total
    alc["former"]           = alc.former/alc.total

    alc.loc[(alc.former >= former_cap), 'abstainers'] = (alc.loc[(alc.former >= former_cap), 'abstainers'].values 
                                                  + alc.loc[(alc.former >= former_cap), 'former'].values 
                                                  - former_cap)
                                                        
    alc.loc[(alc.former >= former_cap), 'former']     = former_cap

else:
    alc["total"]            = alc.current_drinkers+alc.abstainers
    alc["current_drinkers"] = alc.current_drinkers/alc.total
    alc["abstainers"]       = alc.abstainers/alc.total

#Determine percentage of total liters per capita to distribute to each group, based on gday relationships and population.

#Calculate total stock in a given population, based on survey g/day
alc["drinker_population"] = alc.current_drinkers * alc.population
alc["population_consumption"] = alc.drinker_population * alc.gday

#Calculate total stock overall from summing across age_groups for each sex
alc["total_gday"] = alc.groupby(["location_id", "year_id", "draw"])["population_consumption"].transform(sum)

#Calculate percentage of total stock each invidual-level population consumes
alc["percentage_total_consumption"] = alc.population_consumption/alc.total_gday

#Now, using LPC data, calculate total stock and use above calculated percentages to split this 
#stock into lpc drinkers for each population

alc["population_consumption"] = (alc.population * alc.alc_lpc)
if include_unrecorded == True:
    alc["population_consumption"] = alc.population_consumption * alc.unrecorded_rate
alc["total_consumption"] = alc.groupby(["location_id", "year_id", "draw"])["population_consumption"].transform(sum)
alc["drinker_lpc"] = (alc.percentage_total_consumption * alc.total_consumption)/alc.drinker_population

#Convert lpc/year to g/day
alc["drink_gday"] = alc.drinker_lpc * 1000/365

#Hold onto relevant columns and export
alc = alc[['location_id', 'sex_id', 'age_group_id', 'year_id', 'draw', 'current_drinkers', 'abstainers', 'former', 
          'binge', 'binge_events', 'alc_lpc', 'drink_gday', 'population']]

#Convert to collapsed, if option chosen. Regardless, save results
if collapse == True:
    
    alc = pd.melt(alc, id_vars=['location_id', 'sex_id', 'age_group_id', 'year_id', 'draw'])
    
    collapse = pd.DataFrame()
    collapse["mean"]  = alc.groupby(["age_group_id", "location_id", "sex_id", "year_id", 'variable'])['value'].mean()
    collapse["lower"] = alc.groupby(["age_group_id", "location_id", "sex_id", "year_id", 'variable'])['value'].quantile(0.05)
    collapse["upper"] = alc.groupby(["age_group_id", "location_id", "sex_id", "year_id", 'variable'])['value'].quantile(0.95)
    collapse.reset_index(inplace=True)
        
    collapse.to_csv("FILEPATH".format(l=location), index=False)

else:
    alc.to_csv("FILEPATH".format(l=location), index=False)
    
print("Finished!")
```

## Using scaled drink gday and current drinkers, make sure subnational amounts scale to parent location. Make sure to change version argument to be consistent with above.


```python
%load_ext rpy2.ipython
```


```python
%%R

#Load packages
library(plyr)
library(data.table)
source("")

#Choose version of exposures to use
version <- 14
io      <- paste0()

#Get list of locations with parent-child relationship.
locations <- get_location_metadata(location_set_id=2)
locations <- locations[, .(location_id, parent_id, level, most_detailed, location_name)]

parents   <- locations[level>=3 & most_detailed==0]
children  <- locations[level>3 & most_detailed==1]

#Only do England
england <- c(4749,4618:4626,44643:44792)
parents <- parents[location_id %in% england,]
children <- children[location_id %in% england,]


scale_subnat <- function(loc){
    
    print(loc)
    
    #Read in lower_level files & scale to parent
    level <- rbind(parents, children)
    level <- level[(location_id == loc | parent_id == loc)]
    
    #Build list of filenames
    files <- paste0(io, "alc_exp_", level[parent_id == loc, location_id], ".csv")   
    
    #Using % current drinker and drink_gday, calculate total stock in both parent and children for specific
    #year, sex, age. Make sure children sum up to parent. If not, adjust children estimates by scaling factor.
    
    parent  <- fread("FILEPATH")
    parent[, total_stock := population*drink_gday]
    parent <- parent[, .(sex_id, age_group_id, year_id, draw, total_stock)]
    
    subnats <- rbindlist(lapply(files, fread))
    subnats[, subnat_stock := population * drink_gday]
    subnats[, subnat_stock := sum(subnat_stock), by=c("year_id", "sex_id", "age_group_id", "draw")]
    
    subnats <- join(subnats, parent, by=c("year_id", "sex_id", "age_group_id", "draw"), type="left")
    subnats[, drink_gday := drink_gday*(total_stock/subnat_stock)]
    
    #Overwrite old unscaled children
    for (location in unique(subnats$location_id)){
        
        hold <- subnats[location_id==location, c('location_id', 'sex_id', 'age_group_id', 'year_id', 'draw', 'current_drinkers', 'abstainers', 'former', 
          'binge', 'binge_events', 'alc_lpc', 'drink_gday', 'population'), with=F]
        write.csv(hold, "FILEPATH", row.names=FALSE)
        print(head(hold))
    }
}

#Start at highest level of parents and work your way down, scaling children.
#Perform function only if parent actually exists

#scale_subnat(parents)
#Issue with China thanks to GPR implementation, remove from list. 

levels <- sort(unique(parents$level), decreasing = T)

for (l in sort(levels)){
    
    submit <- parents[(level==l & location_id!=6),]
    l_ply(submit$location_id, scale_subnat)
   
}
```


## 4. Compile results

Run this cell to compile all of the datasets together and append on names for ids. Useful for plotting. Can only be run on collapsed results


```python
#Grab all of the collapsed datasets and append them together. Merge on gbd id names
os.chdir(output_dir)
df = []

for files in os.listdir(output_dir):
    if files.startswith("alc_exp"):
        df.append(pd.read_csv(files))

df = pd.concat(df)

df = pd.merge(df, get_location_metadata(location_set_id=2)[['location_id', 'location_ascii_name', 'level']], on="location_id", how="left")
df = pd.merge(df, get_ids("age_group"), on="age_group_id", how="left")
df = pd.merge(df, get_ids("sex"), on="sex_id", how="left")

```

## 5. Format and upload to epi database


```python
import os
import pandas as pd
from db_queries import get_ids, get_location_metadata

version = 14
output_dir  = "

os.chdir(output_dir)

if not os.path.exists("FILEPATH"):
    os.makedirs("FILEPATH")    

for f in os.listdir(output_dir):
    if f.startswith("alc_exp_"):
        
        df = pd.read_csv(f)
        df = df[['location_id', 'sex_id', 'age_group_id', 'year_id', 'draw', 'drink_gday']]
        df = pd.pivot_table(df, index=['location_id', 'sex_id', 'age_group_id', 'year_id'], columns='draw', values='drink_gday').reset_index()
        
        location = list(df.location_id.unique())
        sex_id   = list(df.sex_id.unique())
        year_id  = [1990, 1995, 2000, 2005, 2010, 2016]
        
        split = [df[(df.sex_id == s) & (df.year_id == y)] for s in sex_id for y in year_id]
        
        for df in split:
            
            m = 19
            l = df.location_id.unique()[0]
            s = df.sex_id.unique()[0]
            y = df.year_id.unique()[0]
            
            df.drop(['location_id', 'year_id', 'sex_id'], axis=1, inplace=True)
            df.to_csv(o"FILEPATH".format(l=l, m=m, y=y, s=s), index=False)
            
```

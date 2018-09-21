# ---HEADER-------------------------------------------------------------------------------------------------------------

# Project: OCC - OCC
# Purpose: Prep the economically active population (ILO labor force particpation rate) for modelling in ST-GPR

#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# set toggles


# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
  # necessary to set this option in order to read in a non-english character shapefile on a linux system (cluster)
  Sys.setlocale(category = "LC_ALL", locale = "C")
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
}

# load packages
pacman::p_load(data.table, gridExtra, ggplot2, lme4, magrittr, parallel, stringr)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

#set values for project
location_set_version_id <- 149
year_start <- 1970
year_end <- 2016
by_sex <- 1
by_age <- 0
relevant.ages <- c(8:18) #only ages 15-69

##in##
data.dir <- file.path(home.dir, "FILEPATH")

##out##
out.dir <- file.path(home.dir, "FILEPATH")
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
personal.function.dir <- file.path(h_root, "FILEPATH")
shared.function.dir <- file.path(j_root, 'FILEPATH')
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# this pulls the general misc helper functions
file.path(personal.function.dir, "misc.R") %>% source
# other tools created by covs team for querying db (personal version)
file.path(personal.function.dir, "db_tools.R") %>% source
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH") %>% source
# central functions
file.path(shared.function.dir, 'get_location_metadata.R') %>% source
file.path(shared.function.dir, 'get_population.R') %>% source
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

#function to check variables of a merge and see which were dropped
checkMerge <- function(var, old, new) {
  
  old[, get(var) %>% unique][old[, get(var) %>% unique] %ni%
                               new[, get(var) %>% unique]] %>% return
  
}


logit_offset <- function(x, offset) {
  
  x_len = length(x)
  
  value <- vector(mode="numeric", length=x_len)
  
  for (i in 1:x_len) {
    
    if (x[i]==1) {
      value[i] <- x[i] - offset
    } else if (x[i]==0)  {
      value[i] <- x[i] + offset
    } else value[i] <- x[i]
    
  }
  
  return(log(value/(1-value)))
}

#***********************************************************************************************************************
# ---PREP EMPLOYMENT RATIO----------------------------------------------------------------------------------------------
#ratio of workers to total population
#read in and prep data
ilo.data <- file.path(data.dir, "ILO_ILOSTAT_EMPLOYMENT_POPULATION_RATIO_SEX_AGE.csv") %>% fread

#do some flagging of nonref points before cleanup
ilo.data[, cv_subgeo := 0]
ilo.data[obs_status.label %like% 'Provisional', cv_subgeo := 1] #these are all classified as non national
ilo.data[notes_source.label %like% 'Geographical', cv_subgeo := 1] #these are all classified as non national
ilo.data[notes_source.label %like% 'Population', cv_subgeo := 1] #these are all classified as non national
ilo.data[notes_source.label %like% 'Establishment', cv_subgeo := 1] #these are all classified as non national

#cleanup
setnames(ilo.data,
         c('ref_area', 'ref_area.label', 'classif1', 'time'),
         c('ihme_loc_id', 'ilo_location_name', 'age', 'year_id'))
dt <- ilo.data[, c('ihme_loc_id', 'ilo_location_name', 'sex', 'age', 'year_id', 'obs_value', 'source', 'cv_subgeo'),
               with=F]
dt <- dt[age %like% "5YR"] #keep only the estimates in 5 year bands
dt[sex=="SEX_M", sex_id := 1] #use GBD notation
dt[sex=="SEX_F", sex_id := 2] #use GBD notation
dt[sex=="SEX_T", sex_id := 3] #use GBD notation
#cleanup the ages title
dt[, age_short := str_replace_all(age, "AGE_5YRBANDS_Y", "")]
dt[age_short=="GE65", age_short := "65-69"]
dt[age_short=="AGE_5YRBANDS_TOTAL", age_short := "TOTAL"]
dt[, data := obs_value / 100] #convert to true %
dt <- dt[!is.na(data)] #remove missing points

## first get levels of location
locs <- get_location_hierarchy(location_set_version_id)
levels <- locs[,grep("level|location_id|location_name|ihme_loc_id", names(locs)), with=F]

#merge on levels
#first fix CHN iso3s
dt[ihme_loc_id=="CHN", 'ihme_loc_id' := "CHN"]
dt[ihme_loc_id=="HKG", 'ihme_loc_id' := "CHN_354"]
dt[ihme_loc_id=="MAC", 'ihme_loc_id' := "CHN_361"]
dt <- merge(dt, levels, by="ihme_loc_id")

## now get levels of age
ages <- get_ages()
ages <- ages[age_group_id %in% relevant.ages, list(age_group_id, age_group_name, age_group_name_short)]
ages[, age_short := paste0(age_group_name_short, "-", as.integer(age_group_name_short) + 4)]

## then get pop
#read in population using central fx
pops <- get_population(location_set_version_id=location_set_version_id,
                       location_id = paste(unique(dt$location_id), collapse=" "),
                       year_id = paste(unique(dt$year_id), collapse=" "),
                       sex_id = c(1,2,3), #pull all sexes
                       age_group_id = relevant.ages) #pull only 15-69
pops <- merge(pops, ages, by='age_group_id')

#now add the pop group total
total.pop <- pops[age_group_id %in% relevant.ages]
total.pop[, population := sum(population), by=list(location_id, year_id, sex_id)]
totals <- unique(total.pop, by=c('location_id', 'year_id', 'sex_id'))
totals[, c('age_short', 'age_group_id') := list("TOTAL", 201)] #age gorup id for 15-69 aggregate

#now combine
all.pops <- rbindlist(list(pops, totals))
all.pops <- all.pops[, list(location_id, year_id, sex_id, population, age_short, age_group_id)]

#merge pops to your data
dt <- merge(dt, all.pops, by=c('location_id', 'year_id', 'sex_id', 'age_short'))

#outliers

##age/sex splitting
#create reference dataset
#create country year indicator to display which you need to split
dt[, id := paste(ihme_loc_id, year_id, sep="_")]
ref <- dt[!(sex %like% "SEX_T")& !(age %like% "TOTAL"),]

#create reference age/sex pattern at the region, super-region, global level
ref[, ratio := (data*population)/sum(data*population), by=list(location_id, year_id)]
ref[, super_ratio := mean(ratio), by=list(age_group_id, sex_id, level_1)]
ref[, reg_ratio := mean(ratio), by=list(age_group_id, sex_id, level_2)]
ref[, sex_ratio := (data*population)/sum(data*population), by=list(age_group_id, location_id, year_id)]
ref[, super_sex_ratio := mean(sex_ratio), by=list(sex_id, level_1)]
ref[, reg_sex_ratio := mean(sex_ratio), by=list(sex_id, level_2)]
ref[, age_ratio := (data*population)/sum(data*population), by=list(sex_id, location_id, year_id)]
ref[, super_age_ratio := mean(age_ratio), by=list(age_group_id, level_1)]
ref[, reg_age_ratio := mean(age_ratio), by=list(age_group_id, level_2)]

super <- ref[, list(level_1, sex_id, age_group_id, super_ratio)] %>% unique
reg <- ref[, list(level_2, sex_id, age_group_id, reg_ratio)] %>% unique
super.sex <- ref[, list(level_1, sex_id, age_group_id, super_sex_ratio)] %>% unique
reg.sex <- ref[, list(level_2, sex_id, age_group_id, reg_sex_ratio)] %>% unique
super.age <- ref[, list(level_1, sex_id, age_group_id, super_age_ratio)] %>% unique
reg.age <- ref[, list(level_2, sex_id, age_group_id, reg_age_ratio)] %>% unique

#first find out which countries need splitting
splits.list <- dt[, id %>% unique][dt[, id %>% unique] %ni% ref[, id %>% unique]]

#now se which countries need full age/sex splitting
full.splits.list <- dt[, id %>% unique][dt[, id %>% unique] %ni% dt[!(sex %like% "SEX_T" & age %like% 'TOTAL'),
                                                                    id %>% unique]] %>% sort
#then find the countries that need only age splitting
age.splits.list <- dt[, id %>% unique][dt[, id %>% unique] %ni% dt[!(age %like% "TOTAL"),
                                                                   id %>% unique]]
age.splits.list <- age.splits.list[age.splits.list %ni% full.splits.list] %>% sort
#then find the countries that need only sex splitting
sex.splits.list <- splits.list[splits.list %ni% c(age.splits.list, full.splits.list)] %>% sort 

#create splitting datasets
message("need to do splitting for:", paste(splits.list, collapse=","))
message("need to split both sex and age for:", paste(full.splits.list, collapse=","))
full.splits <- dt[id %in% c(full.splits.list)]
message("need to split just age for:", paste(age.splits.list, collapse=","))
age.splits <- dt[id %in% c(age.splits.list)]
message("need to split just sex for:", paste(sex.splits.list, collapse=","))
sex.splits <- dt[id %in% c(sex.splits.list)]

# split the age/sex split data
if(mean(full.splits[, level_2 %>% unique] %in% reg[, level_2 %>% unique])!=1) stop("need to merge on SR for full.split")
setnames(full.splits, c('data', 'population', 'age_group_id', 'sex_id'),
         paste0(c('data', 'population', 'age_group_id', 'sex_id'), "_og"))
full.splits <- merge(full.splits, reg, by='level_2', allow.cartesian=TRUE)
full.splits <- merge(full.splits, pops, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'))
full.splits[, data := ((data_og*population_og)/population)*reg_ratio]
full.splits[, data_diff := data - data_og]


# split the age split data
setnames(age.splits, c('data', 'population', 'age_group_id'),
         paste0(c('data', 'population', 'age_group_id'), "_og"))

age.splits <- merge(age.splits, reg.age, by=c('level_2', 'sex_id'), allow.cartesian=TRUE)
age.splits <- merge(age.splits, pops, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'))
age.splits[, data := data_og*population_og/population*reg_age_ratio]
age.splits[, data_diff := data - data_og]

# split the sex split data
# first drop the totals
sex.splits <- sex.splits[!(age %like% "TOTAL")]
setnames(sex.splits, c('data', 'population', 'sex_id'),
         paste0(c('data', 'population', 'sex_id'), "_og"))

sex.splits <- merge(sex.splits, reg.sex, by=c('level_2', 'age_group_id'), allow.cartesian=TRUE)
sex.splits <- merge(sex.splits, pops, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'))
sex.splits[, data := data_og*population_og/population*reg_sex_ratio]
sex.splits[, data_diff := data - data_og]

#combine and clean up
splits <- list(full.splits, age.splits, sex.splits) %>% rbindlist(fill=TRUE)
#some points that start high get pulled up to 1. drop them as they are too unstable
clean.splits <- splits[data < 1 & abs(data_diff) < .5,
                       -c('sex', 'age', 'age_short.x', 'age_short.y', 'process_version_map_id',
                          'age_short', 'age_group_name', 'age_group_name_short', 'data_diff',
                          names(splits)[names(splits) %like% 'ratio' | names(splits) %like% 'og']),
                       with=FALSE]
clean.splits[, cv_subgeo := 1] #give less weight to split points
clean.ref <- ref[, -c('sex', 'age','age_short.x', 'age_short.y', 'process_version_map_id',
                      'age_short', 'age_group_name_short', 'data_diff',
                      names(ref)[names(ref) %like% 'ratio' | names(ref) %like% 'og']),
                 with=FALSE]

dt <- list(clean.ref, clean.splits) %>% rbindlist(use.names=T)

browser()

#use lowess to predict a smoothed version
#note if there are less than 3 points we cannot predict
#will use region/sr cv to predict here
predLow <- function(data) {
  
  if (nrow(data) > 3) {
    
    mod <- loess(data~year_id, data=data, span=nrow(data))
    pred <- predict(mod)
    
  } else pred <- NA
  
  return(pred)
  
}

dt[, lowess := predLow(.SD) %>% as.numeric, .SDcols=c("data", 'year_id'), by=list(ihme_loc_id, sex_id, age_group_id)]

#calculate residuals, then take the SD for each lowess model and use it to compute the variance
#this means that more spikey models will have higher variance
dt[, residual := (lowess-data)]
dt[, sd := sd(residual), by=list(ihme_loc_id, sex_id, age_group_id)]

#use cv to impute the variance where we didnt have enough points to use the lowess
dt[, cv := sd / data]
dt[, reg_cv := mean(cv, na.rm=T), by=list(level_2)]
dt[, sr_cv := mean(cv, na.rm=T), by=list(level_1)]
dt[is.na(sd), sd := data * reg_cv]
dt[is.na(sd), sd := data * sr_cv]

#calculate variance
dt[, variance := sd^2]

#cleanup
dt <- dt[, -c('lowess', 'residual', 'cv', 'sd', 'reg_cv', 'sr_cv')]

#final gpr prep/cleanup
dt[, nid := 144369] 
dt[, sample_size := NA]
dt[, me_name := "occ_employment_ratio"]

#outliers
dt <- dt[!(ihme_loc_id == "BWA" & year_id == 2006)] 
dt <- dt[!(ihme_loc_id == "BHS" & year_id == 2009)] 


dt <- dt[ihme_loc_id != "UZB"] 
dt <- dt[ihme_loc_id != "TKM"]

#output
fwrite(dt, file=file.path(out.dir, "occ_employment_ratio.csv"))
#***********************************************************************************************************************

# ---TEST REG-----------------------------------------------------------------------------------------------------------
# get the covariates to test
cov.list <- c('sdi', 'education_yrs_pc', 'prop_urban')
covs <- get_covariates(cov.list)
dt <- merge(dt, covs, by=c('location_id', 'year_id', 'age_group_id', 'sex_id'))

#transform
dt[, lt_data := logit_offset(data, .001)]

# do some test regressions to provide coeffs
mod <- lmer(data=dt, lt_data~sdi+education_yrs_pc+prop_urban+(1|level_1) + (1|level_2) + (1|level_3))

#***********************************************************************************************************************
####################################################################################################################################################################
# 
# Author: AUTHOR
# Purpose: Age split IPV data, to be run after data is prepped and st-gpr model run
#
####################################################################################################################################################################

rm(list=ls())

library(data.table)
library(dplyr)
library(openxlsx)
library(stringr)
library(ggplot2)
library(tidyr)
library(gridExtra)
library(broom)
library(magrittr)
library(parallel)

# source central functions
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_population.R")
library(crosswalk, lib.loc = "FILEPATH")
library(logitnorm, lib.loc='FILEPATH')

#set params 
run_id <- 163418 #agesplit run id
bv <- 34838
model_folder <- 'stgpr'

locs <- get_location_metadata(22)
ages <- get_age_metadata(19)
ages[, age_group_years_end:=age_group_years_end-1]
ages[age_group_id==235, age_group_years_end:=99]
setnames(ages, c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))

# functions -------------------------------------------------------------------------------------------------------------------------------------------------------

## Function to fill out mean/cases/sample sizes
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## Function to get cases if they are missing
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure %in% c("prevalence","proportion"), sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

## Function to calculate standard error
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure %in% c("prevalence","proportion"), standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

#function to get lowest multiple of base
mround <- function(x,base){
  base*floor(x/base)
}

get_stgpr_draws <- function(run_id, location_ids=0){
  #' @description Get draws function 
  #' @param run_id int. The STGPR run_id that you want to get draws from
  #' @param location_ids numeric. A numeric vector
  #' @return a data.table of all N number of draws from the STGPR output
  
  path <- paste0("FILEPATH",run_id,"/draws_temp_0/")
  if(location_ids==0){files <- list.files(path)}else{files <- paste0(location_ids, ".csv")}
  if(F){
    # for testing
    files <- paste0("FILEPATH",run_id,"/FILEPATH")
  }
  
  vars <- paste0("draw_",seq(0,99,1))
  read_draw <- function(file){
    message(paste("Reading", file))
    data <- fread(file)
    message(paste("Done Reading", file))
    data
  }
  
  #' Internal parallelization to read draws from each csv (csvs divided up by location)
  mclapply(paste0(path, files), read_draw, mc.cores = 40) %>% rbindlist(use.names = T)
}

# read in data and divide into train and test sets  --------------------------------------------------------------------------------------------------------
data <- as.data.table(read.xlsx(paste0('FILEPATH', model_folder, '/', 'ipv_bv', bv, '_2020xwalkadj.xlsx')))

# divide data into test and train to be used for ST-GPR 
train <- data[age_group_id!=999]
test <- fsetdiff(data,train)
test[, c('age_start', 'age_end'):=NULL]
setnames(test, c('age_start_orig', 'age_end_orig'), c('age_start', 'age_end'))

# expand ages in test data set --------------------------------------------------------------------------------------------------------------------------------------------------------
raw_test <- copy(test) #save unexpanded copy of test data
setnames(test, c("age_start", "age_end"), c("agg_age_start", "agg_age_end"))
  
# find cases in which we need to combine non-standard age bins in order to avoid duplicate points
test[, raw_age_band:=paste0(agg_age_start, '-', agg_age_end)]
by_nid <- test[, unique(raw_age_band), by='nid']
  
#subset to age groups that do not start with a multiple of 5
combine <- test[!(agg_age_start %% 5)==0 | !str_sub(agg_age_end,-1,-1) %in% c('4', '9')]
  
#subset to nids that have multiple age bands
combine <- combine[, num_age_bands:=length(unique(raw_age_band)), by='nid']
combine <- combine[num_age_bands>1]
combine_nids <- unique(combine$nid)
combine <- test[nid %in% combine_nids]
  
#aggregate these age cases/ss 
combine <- calculate_cases_fromse(combine)
combine <- get_cases_sample_size(combine)
combine[, `:=` (comb_cases=sum(cases), comb_sample_size=sum(sample_size)), by='nid']
combine[, comb_mean:=comb_cases/comb_sample_size, by='nid'] #calculate combined mean
z <- qnorm(0.975)
combine[, comb_se:=sqrt(comb_mean*(1-comb_mean)/comb_sample_size + z^2/(4*comb_sample_size^2))] #calculate combined se
combine[, `:=` (comb_age_start=min(agg_age_start), comb_age_end=max(agg_age_end)), by='nid'] #create correct age_start and age_end of the combined band
combine <- combine[!duplicated(combine[,c('nid', 'comb_age_start', 'comb_age_end', 'comb_mean')]),] #remove what are now duplicates, should have the same number of rows as combine_nids
  
#format
combine[, c('mean', 'standard_error', 'cases', 'sample_size', 'agg_age_start', 'agg_age_end'):=NULL] #drop old fields
setnames(combine, c('comb_mean', 'comb_se', 'comb_cases', 'comb_sample_size', 'comb_age_start', 'comb_age_end'), 
           c('mean', 'standard_error', 'cases', 'sample_size', 'agg_age_start', 'agg_age_end')) #rename created fields to standard
  
#drop these nids from test, and rbind combine in their place
test <- test[!nid %in% combine_nids]
test <- rbind(test, combine)

#create, rounded age start, which is the lowest multiple of 5, this allows non-standard age starts (e.g. 17) to still get replicating rows in GBD age-bins
test[(agg_age_start %% 5)==4, agg_age_start:=agg_age_start+1]
test[(agg_age_start %% 5)==3, agg_age_start:=agg_age_start+2]
test[, round_agg_age_start:=mround(agg_age_start,5)] 
  
#modify groups that end less than or equal to 2 yrs away from GBD start ages, 
#which would result in replicating into rows that the original data don't really represent
test[(agg_age_end %% 5)==0, agg_age_end:=agg_age_end-1] 
test[(agg_age_end %% 5)==1, agg_age_end:=agg_age_end-2]
test[(agg_age_end %% 5)==2, agg_age_end:=agg_age_end-3]
  
#remove groups that do not make sense (e.g. if they were single yr groups and now age_start<age_end)
bad_groups <- test[agg_age_end<=agg_age_start]
test <- test[agg_age_end>agg_age_start]
  
#now get n.age, number of replicated rows 
test[, `:=`(split.id = 1:.N,
              n.age = ceiling((agg_age_end + 1 - round_agg_age_start)/5))] 

## Expand for age
expanded <- rep(test[,split.id], test[,n.age]) %>% data.table("split.id" = .) #replicates split id by number of 5-year age groups that a non-standard age bin spans
test <- merge(expanded, test, by="split.id", all=T) #merges back on to test 
test[, age.rep := (1:.N) - 1, by=split.id] #creates age.rep, which tells number of rows replicated 
test[, age_start := round_agg_age_start + age.rep * 5] # adds number of rows replicated*5 to the age_start 
test[, age_end := age_start + 4] #gets age end by adding 4 to the newly created age_start
test[, created_age_band:=paste0(age_start, '-', age_end)]

# get populations to merge on to expanded test set ------------------------------------------------------------------------------------------------------------------------------------------------------------

test[, year_id:=floor((year_start+year_end)/2)]

#merge age group ids onto test
test[, age_group_id:=NULL]
test <- merge(test, ages[,c('age_start', 'age_end', 'age_group_id')], by=c('age_start', 'age_end'))

years <- unique(test$year_id)
pops <- get_population(age_group_id='all', gbd_round_id=7, location_id=locs$location_id, year_id=years, sex_id=2, decomp_step='iterative')
pops <- merge(pops, ages[,c('age_start', 'age_end', 'age_group_id')], by='age_group_id')
test[, sex_id:=2]
expanded <- merge(pops, test, by = c('age_group_id', 'sex_id', 'location_id', 'year_id'))[, run_id:=NULL] 

#get draws from st.gpr run ------------------------------------------------------------------------------------------------------------------------------------------------------------------

# get draws for all of the countries because we will need to aggregate to the region level
st.draw <- get_stgpr_draws(run_id, locs[level >= 3, location_id])[,measure_id := NULL]

# aggregating ST-GPR draws to create regional age patterns
st.draw <- merge(st.draw,locs[,.(location_id,region_id)],by="location_id")
st.draw <- merge(st.draw,pops,by=c("location_id","sex_id","age_group_id","year_id"))

drawvars <- paste0("draw_",0:999)
new_drawvars <- paste0("draw_ipvcases_",0:999)

#make sure all draws are numeric
st.draw[, (drawvars) := lapply(.SD, function(x) as.numeric(x)), .SDcols=drawvars]

# get draws of the number of cases in each age, location, year, and sex
st.draw[,(new_drawvars) := lapply(.SD, function(x) x*population), .SDcols=drawvars, by=c("location_id", "year_id", "age_group_id", "sex_id")]

# aggregate these draws by region
# now we have number of cases in each region, year, location, age
st.draw[,(new_drawvars) := lapply(.SD, function(x) sum(x)), .SDcols=new_drawvars, by=c("region_id", 'year_id', "age_group_id", "sex_id")] 

# then divide to get new prevalence draws for each age, sex, year, and region:
st.draw[,(drawvars) := lapply(.SD, function(x) x/sum(population)), .SDcols=new_drawvars, by=c("region_id", 'year_id', "age_group_id", "sex_id")]  

# only use the most stable age-trend
st.draw <- st.draw[location_id==163]

#Then clean up draws
st.draw[,(new_drawvars):=NULL]
null_out <- c("region_id","population","run_id","age_start","age_end","age_group")
st.draw[,(null_out) := NULL]

# Expand ID for tracking the draws. 100 draws for each expand ID
expanded[,expand.id := 1:.N]

# Split ID is related to each orignial aggregated test data point. So add up the population of each individual
#group within each split ID. That is the total population of the aggregated age/sex group
expanded[, pop.sum := sum(population), by = split.id] # does this need to be changed for regional/year age-sex splitting?

# Using best age pattern for all data; only merge by age/sex/year and allow cartesian merge 
st.draw[, location_id:=NULL]
draws <- merge(expanded, st.draw, by = c("age_group_id", "sex_id","year_id"), allow.cartesian=T)

# Take all the columns labeled "draw" and melt into one column, row from expanded now has 1000 rows with same data with a unique draw. Draw ID for the equation
draws <- melt.data.table(draws, id.vars = names(draws)[!grepl("draw", names(draws))], measure.vars = patterns("draw"),
                         variable.name = "draw.id", value.name = "pi.value")  

# Generate draws from data, using logit transform to bound between 0 and 1 --------------------------------------------------------------------------------------

#get logit calcs using the delta transform package
logit_means <- as.data.table(delta_transform(mean=draws$mean, sd=draws$standard_error, transformation='linear_to_logit'))
setnames(logit_means, c('mean_logit', 'sd_logit'), c('logit_mean', 'logit_se'))
draws <- cbind(draws, logit_means)

p <- draws[,logit_mean]
sd <- draws[, (logit_se)]
set.seed(123)
sample.draws <- rnorm(1:nrow(draws), p, sd)

#' Now each row of draws has a random draw from the distribution N(mean, SE)
draws[,sample.draws := invlogit(sample.draws)]

# now do the final calculations ----------------------------------------------------------------------------------------------------------------------------------------------------------------

#' This is the numerator of the equation, the total number of cases for a specific age-sex-loc-yr
draws[, numerator := pi.value * population] 

#' This is the denominator, the sum of all the numerators by both draw and split ID. The number of smokers in the aggregated age/sex group
#' The number of terms to be summed should be equal to the number of age/sex groups present in the original aggregated data point
draws[, denominator := sum(numerator), by = .(split.id, draw.id)]

#calculate estimate
draws[, estimate := (pi.value/(denominator/pop.sum)) * sample.draws]

# Collapsing the draws by the expansion ID, by taking the mean, SD, and quantiles of the 100 draws of each calculated split point
#' Each expand ID has 100 draws associated with it, so just take summary statistics by expand ID
final <- draws[, .(mean.est = mean(estimate),
                   var.est = var(estimate),
                   upr.est = quantile(estimate, .975),
                   lwr.est = quantile(estimate, .025),
                   sample_size_new = unique(sample_size_new)), by = expand.id] %>% merge(expanded, by = "expand.id")

#' Set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
final[mean==0, mean.est := 0]
setnames(final, c('mean', 'standard_error', 'variance'), c('agg.mean', 'agg.standard_error', 'agg.variance'), skip_absent=T)
setnames(final, c("mean.est", "var.est"), c("mean", "variance"))
setnames(final, c('upper', 'lower'), c('agg.upper', 'agg.lower'))
setnames(final, c('upr.est', 'lwr.est'), c('upper', 'lower'))
final[, uncertainty_type_value:=95]
final[, c('age_start.y', 'age_end.y'):=NULL]
setnames(final, c('age_start.x', 'age_end.x'), c('age_start', 'age_end'))

# clean up/format final dt for further data processing ----------------------------------------------------------------------------------------------------------------------------------
#drop unnecessary cols
final[, c('created_age_band', 'raw_age_band', 'pop.sum', 'population', 'round_agg_age_start', 'n.age', 'age.rep', 'agg.mean', 'agg.standard_error',
          'agg.upper', 'agg.lower', 'agg.variance', 'split.id', 'expand.id', 'agg_age_start', 'agg_age_end'):=NULL]

#some points already have crosswalk_parent_seqs, but if they don't, set it and clear seq
final[is.na(crosswalk_parent_seq), crosswalk_parent_seq:=seq]
final[!is.na(crosswalk_parent_seq), seq:='']

# clear out old uncertainty info
final[, standard_error:='']
final[, cases:='']
final[, sample_size:='']

#mark that this data was agesplit
final[, note_modeler:=paste0(note_modeler, ' | agesplit using ST-GPR model on train data set ', Sys.Date())]

#fill in SE
final[, standard_error:=sqrt(variance)]

#bind back to train data for the full data set
all_data <- rbind(train, final, fill=T)

#save
write.xlsx(all_data, paste0('FILEPATH', model_folder, '/', 'ipv_bv', bv, '_2020agesplit_region159.xlsx'), sheetName='extraction')


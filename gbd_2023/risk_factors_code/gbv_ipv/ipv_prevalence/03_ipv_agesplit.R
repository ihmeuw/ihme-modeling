####################################################################################################################################################################
# 
# Purpose: Age split IPV data, to be run after data is prepped and st-gpr model run
#
####################################################################################################################################################################

rm(list=ls())

#set parameters -----------------------------
root <- "FILEPATH"

gbd_relid <- 16     #gbd release id
run_id <- 'RUN ID'  #age split run id
bv <- 'BV ID'       #bundle version
date <- 'DATE'      #crosswalk model version



### 0. SET UP ####################################################################################################################################################################

#libraries + central functions
pacman::p_load(data.table, dplyr, openxlsx, stringr, ggplot2, tidyr, gridExtra, broom, magrittr, parallel)
invisible(sapply(list.files("FILEPATH", full.names = T), source))

#crosswalk package
library(reticulate)
reticulate::use_python("FILEPATH")
cw <- import("crosswalk")
library(crosswalk002)
library(logitnorm, lib.loc = paste0('FILEPATH', Sys.info()['user']))

#location + age metadata
locs <- get_location_metadata(22, release_id = gbd_relid)
ages <- get_age_metadata(24, release_id = gbd_relid)[, age_group_years_end := age_group_years_end - 1] %>% setnames(c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
age21 <- data.table(age_group_id = 21, age_start = 80, age_end = 99)
ages <- rbind(ages, age21, fill = T)
ages[age_group_id == 235, age_end := 99]

#custom function to calculate mean/cases/sample sizes
get_cases_sample_size <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

#custom function to calculate cases
calculate_cases_fromse <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure %in% c("prevalence","proportion"), sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

#custom function to calculate standard error
get_se <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure %in% c("prevalence","proportion"), standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

#custom function to get lowest multiple of base
mround <- function(x,base) {
  base*floor(x/base)
}

#custom function to read in stgpr draws
get_stgpr_draws <- function(run_id, location_ids=0){
  #' @description Get draws function 
  #' @param run_id int. The STGPR run_id that you want to get draws from
  #' @param location_ids numeric. A numeric vector
  #' @return a data.table of all N number of draws from the STGPR output
  
  path <- paste0("FILEPATH", run_id, "/draws_temp_0/")
  files <- paste0(location_ids, ".csv")
  
  # vars <- paste0("draw_",seq(0, 99, 1))
  read_draw <- function(file){
    message(paste("Reading", file))
    data <- fread(file)
    message(paste("Done Reading", file))
    data
  }
  
  #' Internal parallelization to read draws from each csv (csvs divided up by location)
  mclapply(paste0(path, files), read_draw, mc.cores = 1) %>% rbindlist(use.names = T)
}

#z score for uncertainty
z <- qnorm(0.975)



### 1. GET + DIVIDE DATA ####################################################################################################################################################################

#read in xwalked data
data <- as.data.table(read.xlsx(paste0(root, 'FILEPATH', date, '/ipv_bv', bv, '_2023xwalkadj_update.xlsx')))

#only keep data used for modeling (alternatively, dropped data only used for crosswalking)
data <- data[is.na(group_review) | group_review==1]

#divide data into test and train data for ST-GPR 
train <- data[age_group_id != 999]
test <- fsetdiff(data, train)

#rename ages for test data
test[, c('age_start', 'age_end') := NULL]
setnames(test, c('age_start_orig', 'age_end_orig'), c('age_start', 'age_end'))



### 2. COMBINE 80+ AGE GROUPS IN TRAIN ####################################################################################################################################################################

#subset to ages 80+
train80plus <- train[age_group_id>20]

#subset to nids that have multiple age bands
train80plus <- train80plus[, num_age_bins := length(unique(age_group_id)), by='nid']

#calculate cases and sample sizes
train80plus <- calculate_cases_fromse(train80plus)
train80plus <- get_cases_sample_size(train80plus)

#aggregate cases/sample sizes by nid; recalculate combined uncertainty, se, etc.
train80plus[, `:=` (comb_cases=sum(cases), comb_sample_size=sum(sample_size)), by='nid']
train80plus[, comb_mean:=comb_cases/comb_sample_size, by='nid'] #calculate combined mean
train80plus[, comb_se:=sqrt(comb_mean*(1-comb_mean)/comb_sample_size + z^2/(4*comb_sample_size^2))] #calculate combined se
train80plus[, `:=` (comb_age_start=min(age_start), comb_age_end=max(age_end)), by='nid'] #create correct age_start and age_end of the combined band
train80plus <- train80plus[!duplicated(train80plus[,c('nid', 'comb_age_start', 'comb_age_end', 'comb_mean')]),] #remove what are now duplicates, should have the same number of rows as combine_nids
train80plus[, variance:=standard_error^2]

#format names
train80plus[, c('mean', 'standard_error', 'variance' ,'cases', 'sample_size', 'age_start', 'age_end') := NULL] #drop old fields
setnames(train80plus, c('comb_mean', 'comb_se', 'comb_cases', 'comb_sample_size', 'comb_age_start', 'comb_age_end'),
                      c('mean', 'standard_error', 'cases', 'sample_size', 'age_start', 'age_end')) #rename created fields to standard gbd naming

#duplicate 80+ rows into gbd estimation bins
train80plus[, `:=`(split.id = 1:.N, n.age = ceiling((age_end - age_start)/5))]

#expand rows by n.age
expanded80plus <- rep(train80plus[,split.id], train80plus[,n.age]) %>% data.table("split.id" = .) #replicates split id by number of 5-year age groups that a 80plus point spans
train80plus <- merge(expanded80plus, train80plus, by="split.id", all=T) #merges back on to test
train80plus[, age.rep := (1:.N) - 1, by=split.id] #creates age.rep, which tells number of rows replicated
train80plus[, age_start := age_start + age.rep * 5] # adds number of rows replicated*5 to the age_start
train80plus[, age_end := age_start + 4]#gets age end by adding 4 to the newly created age_start
train80plus[, age_group_id:=NULL]
train80plus <- merge(train80plus, ages[, c('age_start', 'age_end', 'age_group_id')], by=c('age_start', 'age_end')) #get granular age group ids on
train80plus[, c('n.age', 'age.rep'):=NULL]

#drop these nids from train, and rbind the combine dt in their place
train <- train[!(nid %in% unique(train80plus$nid) & age_group_id>20)]
train <- rbind(train, train80plus, fill=T)



### 3. EXPAND AGES IN TEST DATA ####################################################################################################################################################################

#save unexpanded copy of test data
raw_test <- copy(test) 

#rename
setnames(test, c("age_start", "age_end"), c("agg_age_start", "agg_age_end"))
  
#find cases in which we need to combine non-standard age bins in order to avoid duplicate points
test[, raw_age_band := paste0(agg_age_start, '-', agg_age_end)]
by_nid <- test[, unique(raw_age_band), by='nid']
  
#subset to age groups that do not start with a multiple of 5
combine <- test[!(agg_age_start %% 5)==0 | !str_sub(agg_age_end,-1,-1) %in% c('4', '9')]
  
#subset to nids that have multiple age bands
combine <- combine[, num_age_bands := length(unique(raw_age_band)), by = 'nid']
combine <- combine[num_age_bands>1]
combine_nids <- unique(combine$nid)
combine <- test[nid %in% combine_nids]
  
#aggregate cases/sample sizes by nid; recalculate combined uncertainty, se, etc. 
combine <- calculate_cases_fromse(combine)
combine <- get_cases_sample_size(combine)
combine[, `:=` (comb_cases=sum(cases), comb_sample_size=sum(sample_size)), by='nid']
combine[, comb_mean:=comb_cases/comb_sample_size, by='nid'] #calculate combined mean
combine[, comb_se:=sqrt(comb_mean*(1-comb_mean)/comb_sample_size + z^2/(4*comb_sample_size^2))] #calculate combined se
combine[, `:=` (comb_age_start=min(agg_age_start), comb_age_end=max(agg_age_end)), by='nid'] #create correct age_start and age_end of the combined band
combine <- combine[!duplicated(combine[,c('nid', 'comb_age_start', 'comb_age_end', 'comb_mean')]),] #remove what are now duplicates, should have the same number of rows as combine_nids
  
#format
combine[, c('mean', 'standard_error', 'cases', 'sample_size', 'agg_age_start', 'agg_age_end'):=NULL] #drop old fields
setnames(combine, c('comb_mean', 'comb_se', 'comb_cases', 'comb_sample_size', 'comb_age_start', 'comb_age_end'), 
           c('mean', 'standard_error', 'cases', 'sample_size', 'agg_age_start', 'agg_age_end')) #rename created fields to standard
  
#drop these nids from test, and rbind combine in their place
test <- test[!nid %in% combine_nids]
test <- rbind(test, combine, fill = T)

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
bad_groups <- test[agg_age_end <= agg_age_start]
test <- test[agg_age_end > agg_age_start]
  
#now get n.age, number of replicated rows 
test[, `:=`(split.id = 1:.N,
              n.age = ceiling((agg_age_end + 1 - round_agg_age_start)/5))] 

#expand for age
expanded <- rep(test[,split.id], test[,n.age]) %>% data.table("split.id" = .) #replicates split id by number of 5-year age groups that a non-standard age bin spans
test <- merge(expanded, test, by="split.id", all=T) #merges back on to test 
test[, age.rep := (1:.N) - 1, by=split.id] #creates age.rep, which tells number of rows replicated 
test[, age_start := round_agg_age_start + age.rep * 5] # adds number of rows replicated*5 to the age_start 
test[, age_end := age_start + 4] #gets age end by adding 4 to the newly created age_start
test[, created_age_band:=paste0(age_start, '-', age_end)]

#get populations to merge on to expanded test set
test[, year_id:=floor((year_start+year_end)/2)]

#merge age group ids onto test
test[, age_group_id:=NULL]
test <- merge(test, ages[,c('age_start', 'age_end', 'age_group_id')], by=c('age_start', 'age_end'))



### 4. COMBINE 80+ AGE GROUPS IN TEST ####################################################################################################################################################################

#aggregate 80+ age bins to match stgpr draws
test80plus <- test[age_group_id>20]

#subset to nids that have multiple age bands
test80plus <- test80plus[, num_age_bins:=length(unique(age_group_id)), by='nid']

#aggregate cases/sample sizes by nid; recalculate combined uncertainty, se, etc. 
test80plus <- calculate_cases_fromse(test80plus)
test80plus <- get_cases_sample_size(test80plus)
test80plus[, `:=` (comb_cases=sum(cases), comb_sample_size=sum(sample_size)), by='nid']
test80plus[, comb_mean:=comb_cases/comb_sample_size, by='nid'] #calculate combined mean
test80plus[, comb_se:=sqrt(comb_mean*(1-comb_mean)/comb_sample_size + z^2/(4*comb_sample_size^2))] #calculate combined se
test80plus[, `:=` (comb_age_start=min(age_start), comb_age_end=max(age_end)), by='nid'] #create correct age_start and age_end of the combined band
test80plus <- test80plus[!duplicated(test80plus[,c('nid', 'comb_age_start', 'comb_age_end', 'comb_mean')]),] #remove what are now duplicates, should have the same number of rows as combine_nids
test80plus[, variance:=standard_error^2]

#format
test80plus[, c('mean', 'standard_error', 'variance' ,'cases', 'sample_size', 'age_start', 'age_end'):=NULL] #drop old fields
setnames(test80plus, c('comb_mean', 'comb_se', 'comb_cases', 'comb_sample_size', 'comb_age_start', 'comb_age_end'), 
         c('mean', 'standard_error', 'cases', 'sample_size', 'age_start', 'age_end')) #rename created fields to standard
test80plus[, age_group_id:=21] #set age group id to 21

#drop these nids from train, and rbind the combine dt in their place
test <- test[!(nid %in% unique(test80plus$nid) & age_group_id>20)]
test <- rbind(test, test80plus, fill=T)

#merge test data onto populations
years <- unique(test$year_id)
pops <- get_population(age_group_id = 'all', release_id = gbd_relid, location_id = locs$location_id, year_id = years, sex_id = 2)
pops <- merge(pops, ages[,c('age_start', 'age_end', 'age_group_id')], by='age_group_id')
test[, sex_id:=2]
expanded <- merge(pops, test, by = c('age_group_id', 'sex_id', 'location_id', 'year_id'))[, run_id:=NULL] 



### 5. CLEAN ST-GPR DRAWS FROM AGE SPLIT RUN ####################################################################################################################################################################

#get draws for all of the countries because we will need to aggregate to the region level
st.draw <- get_stgpr_draws(run_id, locs[level >= 3, location_id])

#aggregating ST-GPR draws to create regional age patterns
st.draw <- merge(st.draw, locs[,.(location_id,region_id)], by="location_id")
st.draw <- merge(st.draw, pops, by=c("location_id","sex_id","age_group_id","year_id"))

#initialize draws
drawvars <- paste0("draw_",0:999)
new_drawvars <- paste0("draw_ipvcases_",0:999)

#make sure all draws are numeric
st.draw[, (drawvars) := lapply(.SD, function(x) as.numeric(x)), .SDcols=drawvars]

#get draws of the number of cases in each age, location, year, and sex
st.draw[,(new_drawvars) := lapply(.SD, function(x) x*population), .SDcols=drawvars, by=c("location_id", "year_id", "age_group_id", "sex_id")]

#aggregate these draws by region - now we have number of cases in each region, year, location, age
st.draw[,(new_drawvars) := lapply(.SD, function(x) sum(x)), .SDcols=new_drawvars, by=c("region_id", 'year_id', "age_group_id", "sex_id")]

#then divide to get new prevalence draws for each age, sex, year, and region:
st.draw[,(drawvars) := lapply(.SD, function(x) x/sum(population)), .SDcols=new_drawvars, by=c("region_id", 'year_id', "age_group_id", "sex_id")]  

#only use the most stable age-trend from region-id 159 (south asia) and location_id=163 (India) - draws have been averaged by region, so we only need location_id
st.draw <- st.draw[location_id == 163]

#clean up draws by dropping extra columns
st.draw[, (new_drawvars) := NULL]
null_out <- c("region_id","population","run_id","age_start","age_end","age_group")
null_out <- c("region_id","population","run_id","age_start","age_end")

st.draw[, (null_out) := NULL]

#expand ID for tracking the draws. 100 draws for each expand ID
expanded[, expand.id := 1:.N]

#split ID is related to each orignial aggregated test data point. So add up the population of each individual
#group within each split ID. That is the total population of the aggregated age/sex group
expanded[, pop.sum := sum(population), by = split.id]

#use best age pattern for all data - only merge by age/sex/year and allow cartesian merge 
st.draw[, location_id:=NULL]
draws <- merge(expanded, st.draw, by = c("age_group_id", "sex_id","year_id"), allow.cartesian=T)

#take all the columns labeled "draw" and melt into one column, row from expanded now has 1000 rows with same data with a unique draw. Draw ID for the equation
draws <- melt.data.table(draws, id.vars = names(draws)[!grepl("draw", names(draws))], measure.vars = patterns("draw"),
                         variable.name = "draw.id", value.name = "pi.value")  


#Generate draws from data, using logit transform to bound between 0 and 1 --------------------------------------------------------------------------------------

#get logit calcs using the delta transform package
logit_means <- as.data.table(delta_transform(mean=draws$mean, sd=draws$standard_error, transformation='linear_to_logit'))
setnames(logit_means, c('mean_logit', 'sd_logit'), c('logit_mean', 'logit_se'))
draws <- cbind(draws, logit_means)

p <- draws[, logit_mean]
sd <- draws[, (logit_se)]
set.seed(123)
sample.draws <- rnorm(1:nrow(draws), p, sd)

#Now each row of draws has a random draw from the distribution N(mean, SE)
draws[,sample.draws := invlogit(sample.draws)]



### 6. SOME CALCULATIONS ####################################################################################################################################################################

#this is the numerator of the equation: the total number of cases for a specific age-sex-loc-yr
draws[, numerator := pi.value * population] 

#this is the denominator: the sum of all the numerators by both draw and split ID - the number of people in the aggregated age/sex group
#the number of terms to be summed should be equal to the number of age/sex groups present in the original aggregated data point
draws[, denominator := sum(numerator), by = .(split.id, draw.id)]

#calculate estimate and sample size
draws[, estimate := (pi.value/(denominator/pop.sum)) * sample.draws]
draws[, sample_size_new := sample_size * population / pop.sum] 

#collapsing the draws by the expansion ID, by taking the mean, SD, and quantiles of the 100 draws of each calculated split point
#each expand ID has 100 draws associated with it, so just take summary statistics by expand ID
final <- draws[, .(mean.est = mean(estimate),
                   var.est = var(estimate),
                   upr.est = quantile(estimate, .975),
                   lwr.est = quantile(estimate, .025),
                   sample_size_new = unique(sample_size_new)), by = expand.id] %>% merge(expanded, by = "expand.id")

#set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
final[mean==0, mean.est := 0]
setnames(final, c('mean', 'standard_error', 'variance'), c('agg.mean', 'agg.standard_error', 'agg.variance'), skip_absent=T)
setnames(final, c("mean.est", "var.est"), c("mean", "variance"))
setnames(final, c('upper', 'lower'), c('agg.upper', 'agg.lower'))
setnames(final, c('upr.est', 'lwr.est'), c('upper', 'lower'))
final[, uncertainty_type_value:=95]
final[, c('age_start.y', 'age_end.y'):=NULL]
setnames(final, c('age_start.x', 'age_end.x'), c('age_start', 'age_end'))



### 7. FIX 80+ AGES ####################################################################################################################################################################

#purpose: re-split and duplicate 80+ age group so that we get more granular 5-year age bins

#subset to 80+ and tag with a split id
final80plus <- final[age_group_id==21]
final80plus[, `:=`(split.id = 1:.N,
                   n.age = ceiling((age_end - age_start)/5))] 

#expand rows by n.age - basically just copying the estimates for each bin that exists above 80
expanded_final80plus <- rep(final80plus[,split.id], final80plus[,n.age]) %>% data.table("split.id" = .) #replicates split id by number of 5-year age groups that a 80plus point spans
final80plus <- merge(expanded_final80plus, final80plus, by="split.id", all=T) #merges back on to test 
final80plus[, age.rep := (1:.N) - 1, by=split.id] #creates age.rep, which tells number of rows replicated 
final80plus[, age_start := age_start + age.rep * 5] # adds number of rows replicated*5 to the age_start 
final80plus[, age_end := age_start + 4]#gets age end by adding 4 to the newly created age_start
final80plus[, age_group_id:=NULL]
final80plus <- merge(final80plus, ages[, c('age_start', 'age_end', 'age_group_id')], by=c('age_start', 'age_end')) #get granular age group ids on
final80plus[, c('n.age', 'age.rep'):=NULL]

#drop original 80+ estimate
final <- final[!age_group_id==21]

#add the more granular ages back
final <- rbind(final, final80plus, fill=T)

#check age group ids
table(final$age_group_id)



### 8. FINAL FORMATS FOR FUTURE DATA PROCESSING ####################################################################################################################################################################

#drop unnecessary cols that were created above and/or are left over from extractions
final[, c('created_age_band', 'raw_age_band', 'pop.sum', 'population', 'round_agg_age_start', 'n.age', 'age.rep', 'agg.mean', 'agg.standard_error',
          'agg.upper', 'agg.lower', 'agg.variance', 'split.id', 'expand.id', 'agg_age_start', 'agg_age_end',
          'ca', 'ipv', 'perp_mapping', 'phys_violence_type', 'perpetrator_identity','force_mapping','ind_mapping','og_var','phys_ind_mapping','sex_ind_mapping','act_tag') := NULL]

#some points already have crosswalk_parent_seqs, but if they don't, set it and clear seq
final[is.na(crosswalk_parent_seq), crosswalk_parent_seq:=seq]
final[!is.na(crosswalk_parent_seq), seq:='']

#clear out old uncertainty info
final[, standard_error:='']

#mark that this data was agesplit
final[!is.na(note_modeler), note_modeler:=paste0(note_modeler, ' | agesplit using ST-GPR model on train data set')]
final[is.na(note_modeler), note_modeler:=paste0('agesplit using ST-GPR model on train data set')]

#fill in se
final[, standard_error:=sqrt(variance)]

#bind back to train data for the full data set (remember - train data was already in 5 year age bins!)
all_data <- rbind(train, final, fill=T)





### 9. SAVE FINAL RESULTS ##########################################################################################################################################################################################

dir.create(paste0(root, 'FILEPATH', date), recursive = T)
write.xlsx(all_data, paste0(root, 'FILEPATH', date, '/ipv_bv', bv, '_2023_agesplit_', run_id, '_region159_update.xlsx'), sheetName='extraction')


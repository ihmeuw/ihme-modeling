####################################################################################################################################################################
# 
# Author: AUTHOR
# Purpose: Apply ever-partnered proportion adjustment to IPV data
#
####################################################################################################################################################################

rm(list=ls())

# Map drives -------------------------------------------------------------------------------------------------------------------------------------------------------

if (Sys.info()['sysname'] == 'Linux') {
  j_drive <- 'FILEPATH' 
  h_drive <- 'FILEPATH'
} else { 
  j_drive <- 'FILEPATH'
  h_drive <- 'FILEPATH'
}

fn_dir <- 'FILEPATH'

library(logitnorm, lib.loc='FILEPATH')
library(openxlsx)
library(ggplot2)
library(stringr)

source(paste0(fn_dir,"get_draws.R"))
source(paste0(fn_dir,"get_age_metadata.R"))
source(paste0(fn_dir,"get_population.R"))
source(paste0(fn_dir,"get_crosswalk_version.R"))
source(paste0(fn_dir,"save_crosswalk_version.R"))
source(paste0(fn_dir,"get_location_metadata.R"))
library(crosswalk, lib.loc = "FILEPATH")

# Short functions used & other initializing -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
mround <- function(x,base){
  base*round(x/base)
}

se <- function(x){
  sd(x)/sqrt(length(x))
}

## Function to get cases if they are missing
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure %in% c("prevalence","proportion"), sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

draws <- paste0("draw_",0:999)

bv <- 34838
model_folder <- 'stgpr'

# Read in IPV data --------------------------------------------------------------------------------------------------------------------------------------------------------------
ipv <- as.data.table(read.xlsx(paste0('FILEPATH', model_folder, '/', 'ipv_bv', bv, '_2020agesplit_region159.xlsx')))
ipv[, sex_id:=2]

#assume untagged data does not need this adjustment
ipv[is.na(cv_pstatall),cv_pstatall:=0]
ipv[is.na(cv_pstatcurr),cv_pstatcurr:=0]

# map old cv naming to 2020 cvs
ipv[is.na(cv_epart_pop), cv_epart_pop:=eval(parse(text='cv_pstatall'))]
ipv[is.na(cv_currpart_pop), cv_currpart_pop:=eval(parse(text='cv_pstatcurr'))]

#create merge id to get xwalked values back onto metadata later
ipv[, merge_id:=1:.N]

#make sure all data has an age group id
# pull age data
age_dt <- get_age_metadata(19, gbd_round_id=7)
age_dt[,age_group_weight_value:=NULL]
age_dt <- as.data.table(sapply(age_dt, as.integer))
age_dt[,age_group_years_end:=age_group_years_end - 1] # switch to 0-4 age bins to match IPV data
ages <- copy(age_dt)
setnames(ages, c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))

# subset to rows that need this adjustment
toxwalk <- copy(ipv[(cv_epart_pop==1 | cv_currpart_pop==1),])
noxwalk <- copy(ipv[cv_epart_pop==0 & cv_currpart_pop==0,])

# get locs that need to be xwalked
ipv_locs <- unique(toxwalk$location_id)

## ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# get year_ids
toxwalk[,year_id:=floor((year_start+year_end)/2)]
toxwalk[!year_id==2019, year_id:=mround(year_id,5)] 

## PULL PROPORTION OF EVER PARTNERED WOMEN --------------------------------------------------------------------------------------------------------------------------------------

# load dismod model of women who have ever had an intimate partner; me_id 9380
epart_model <- get_draws(gbd_id_type = "modelable_entity_id", 
                         gbd_id = 9380,
                         source = "epi", 
                         measure_id = 6, 
                         sex_id=2,
                         location_id = ipv_locs,
                         version_id = 505475,
                         gbd_round_id = 7,
                         decomp_step = 'iterative')

epart_model <- epart_model[age_group_id>=7,] # subset to only ages greater than age group 10-14

# calc draw means
epart_model$mean <- rowMeans(epart_model[,draws,with=F])

# subset to vars of interest
epart_model <- epart_model[,c("location_id",
                              "year_id",
                              "age_group_id",
                              "sex_id",
                              "mean",
                              draws), with = F]

# pull population envelope
year_ids <- c(seq(1990,2020,5),2019)
age_group_ids <- unique(epart_model$age_group_id)
pop <- get_population(age_group_id = age_group_ids, sex_id = 2, year_id = year_ids,
                      location_id = ipv_locs, gbd_round_id=7, decomp_step = 'iterative')[,run_id:=NULL]

# merge epart draws on pop counts
epart_model <- merge(epart_model, pop, by = c("age_group_id","location_id","year_id","sex_id"), all.x = T)

## Generate draws of ipv data to adjust --------------------------------------------------------------------------------------------------------------------------------------------------------

#calculate standard error if needed
toxwalk[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
z <- qnorm(0.975)
toxwalk[is.na(standard_error), standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]

## estimate sample_size where missing (assuming binomial distribution)
toxwalk[is.na(sample_size),sample_size := mean*(1-mean)/(standard_error^2)]

# initialize for ipv draws
ipv_draws <- data.table(matrix(NA, nrow = 1000, ncol = nrow(toxwalk)+1))
names(ipv_draws)[1] <- "draw"
ipv_draws$draw <- draws

#get logit calcs using the delta transform package
logit_means <- as.data.table(delta_transform(mean=toxwalk$mean, sd=toxwalk$standard_error, transformation='linear_to_logit'))
setnames(logit_means, c('mean_logit', 'sd_logit'), c('logit_mean', 'logit_se'))
toxwalk <- cbind(toxwalk, logit_means)

for(i in 1:nrow(toxwalk)){
  logit.mean <- toxwalk[i,]$logit_mean
  logit.se <- toxwalk[i,]$logit_se
  merge_id <- toxwalk[i,]$merge_id
  var <- paste0("V",i+1)
  dr <- rnorm(n = 1000, mean=logit.mean, sd=(logit.se))
  ipv_draws[, (var) := dr]
  names(ipv_draws)[i+1] <- merge_id
}

ipv_long <- melt(ipv_draws, id.vars = "draw", variable.name = "merge_id", value.name = "ipv_mean")

#now back-transform logit means to linear space
ipv_long[, ipv_mean:=invlogit(ipv_mean)]

# Merge draws onto xwalk dt
ipv_long$merge_id <- as.integer(as.character(ipv_long$merge_id))
toxwalk$merge_id <- as.integer(as.character(toxwalk$merge_id))
ipv_long <- merge(ipv_long, toxwalk[,.(seq,crosswalk_parent_seq,year_id,age_start,age_end,age_group_id,location_id,merge_id)], by = "merge_id", all.x = T)

# Create epart proportions --------------------------------------------------------------------------------------------------------------------------------------------------------------

# cast epart dt to long
long_epart <- melt(data = epart_model, id.vars = c("age_group_id","year_id","location_id"), measure.vars = draws,
                   variable.name = "draw", value.name = "ever_partnered")

# merge xwalk data table and epart model data table
epart_ipv <- merge(ipv_long, long_epart, by = c("draw","year_id","age_group_id","location_id"),
                   all.x = T)

# get adjusted means
epart_ipv[,adj_ipv_mean:=ipv_mean*ever_partnered]

final <- epart_ipv[, .(mean.est = mean(adj_ipv_mean),
                   var.est = var(adj_ipv_mean),
                   upr.est = quantile(adj_ipv_mean, .975),
                   lwr.est = quantile(adj_ipv_mean, .025)), by = merge_id] %>% merge(ipv[merge_id %in% unique(epart_ipv$merge_id)], by = "merge_id")

#' Set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
final[mean==0, mean.est := 0]
# setnames(final, c("variance"), c("agg.mean", "agg.variance"))
setnames(final, c('mean', 'standard_error', 'variance'), c('nonepart.mean', 'nonepart.standard_error', 'nonepart.variance'), skip_absent=T)
setnames(final, c("mean.est", "var.est"), c("mean", "variance"))
setnames(final, c('upper', 'lower'), c('nonepart.upper', 'nonepart.lower'))
setnames(final, c('upr.est', 'lwr.est'), c('upper', 'lower'))

#clear out unadjusted se information
final[, standard_error:='']
final[, cases:='']
final[, sample_size:='']

#apply same calcs to get year id in 'final'
final[,year_id:=floor((year_start+year_end)/2)]
final[!(year_id %in% c(2019)),year_id := mround(year_id,5)]

if(nrow(final)!=length(unique(epart_ipv$merge_id))){
  warning("something went wrong with final merge!")
}

# Fill in some fields and note that adjustment was made with date
final[!is.na(upper) & !is.na(lower),uncertainty_type := "Confidence interval"]
final[is.na(upper) & is.na(lower),uncertainty_type_value := NA]
final[,note_modeler := paste0(note_modeler," | applied ever_partnered xwalk ", Sys.Date())]

# If crosswalk_parent_seq not already filled out by previous data processing steps, fill it in now and set seq to null
final[is.na(crosswalk_parent_seq), crosswalk_parent_seq:=seq]
final[!is.na(crosswalk_parent_seq), seq:=''] 

#Check for bad values
if(nrow(final[is.na(mean),])>0){
  warning("uh oh, mean missing")
}

if(nrow(final[is.na(upper) & is.na(lower) & is.na(effective_sample_size) & is.na(standard_error)])>0){
  warning("uh oh, variance missing")
}

if(nrow(final[mean < 0 | mean > 1])>0){
  warning("mean out of range")
}

#recreate year_id
final[,year_id:=floor((year_start+year_end)/2)]
final[, year_start:=year_id]
final[, year_end:=year_id]

# Bind back on to data not adjusted in this step, drop merge id
to_upload <- rbind(final, noxwalk, fill=T)
to_upload[,merge_id:=NULL]

#save csv
write.csv(to_upload, paste0('FILEPATH', model_folder, '/','ipv_bv', bv, '_data_2020epartadj_raw_region159.csv'))


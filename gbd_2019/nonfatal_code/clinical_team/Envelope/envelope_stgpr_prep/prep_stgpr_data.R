#############################################################
##Date: 4/2/2019
##Purpose: Vet ST-GPR input data in response to plots from run 45750
##Notes: 
##Updates: Check out issues with variance and outliering
# Do I drop as outlier or just drop the data point? possibly drop data point because I've heard weird stuff with outliers...
###########################################################
library(data.table)
library(readr)
source(get_outputs.R)
source('get_envelope.R')
source('get_draws.R')
source('get_location_metadata.R')
source('get_cause_metadata.R')
source('get_demographics.R')
source('utility.r')
locs <- get_location_metadata(35)

source('get_covariate_estimates.R')
test <- get_covariate_estimates(1099, gbd_round_id = 7, decomp_step = 'iterative')


date <- Sys.Date()
write_csv(dt, paste0(FILEPATH))

# Previous model
#write_csv(dt, FILEPATH)

## Add all-cause mortality as custom covariate. Use get_outputs square to see if you can do it that way because the get_draws didn't really work
#dt <- fread(FILEPATH)


ages <- c(2,3,28, 5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235,238,388,389)
ages <- sort(ages)
locs <- get_location_metadata(22, gbd_round_id = 6)
#locs <- locs[level >= 3]

library(mortdb)


mort <- get_envelope(year_id=c(1990:2019), sex_id=c(1,2), gbd_round_id=7, with_hiv=1, rates=1,
                     decomp_step = 'iterative', location_id = unique(locs$location_id),
                     age_group_id= ages)
setnames(mort, 'mean', 'cv_mort')
write_csv(mort, FILEPATH)

dt <- merge(dt, mort[, c('location_id', 'age_group_id', 'sex_id', 'year_id', 'cv_mort')], 
            by= c('location_id', 'age_group_id', 'sex_id', 'year_id'), all.x = TRUE)




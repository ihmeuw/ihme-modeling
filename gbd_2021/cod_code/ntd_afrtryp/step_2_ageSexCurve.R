#########################################################################
# Description:                                                          #
# Fit an age/sex curve to be applied in Step 4. Use all available age-  #
# specific case data (no sex-specific data available) to fit a Poisson  #
# glm regressing number of cases on age (fit as a restricted cubic      #
# spline), with effective sample size fit as an offset.                 #
#########################################################################

### ========================= BOILERPLATE ========================= ###

rm(list=ls())
data_root <- "FILEPATH"
cause <- "ntd_afrtryp"

## Define paths 
params_dir  <- "FILEPATH"
draws_dir   <- "FILEPATH"
interms_dir <- "FILEPATH"
logs_dir    <- "FILEPATH"

##	Source relevant libraries
library(rms)
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_bundle_data.R")

## Define Constants
decomp_step = ADDRESS
gbd_round_id <- ADDRESS
age_group_set_id <- 19

# Switch for refitting the age-sex curve
refit_agesex = 0
# If set to 0, read in the ageSexCurve file from gbd2019 and add new ages.
# If set to 1, the age-sex curve will be re-fit

### ========================= MAIN EXECUTION ========================= ###

if (refit_agesex==0){
  
  # Read in age-sex curve:
  ageSexCurve <- readRDS(paste0(params_dir, "/FILEPATH"))
  
  # Convert age group 4 into 388 and 389
  ageSexCurve$age_group_id[ageSexCurve$age_group_id == 4] <- 388
  ageSexCurve <- rbind(setDT(ageSexCurve), ageSexCurve[age_group_id==388,][,age_group_id := 389])
  
  # Convert age group 5 into 34 and 238
  ageSexCurve$age_group_id[ageSexCurve$age_group_id == 5] <- 34
  ageSexCurve <- rbind(setDT(ageSexCurve), ageSexCurve[age_group_id==34,][,age_group_id := 238])
  
  # Sort
  ageSexCurve <- ageSexCurve[order(sex_id, age_group_id)]
  
  # Save output
  saveRDS(ageSexCurve, file=paste0(interms_dir,"/FILEPATH"))
}


if (refit_agesex==1){

## (1) Pull start and end years for all modelled age groups 
ages <- get_age_metadata(age_group_set_id=age_group_set_id,
                         gbd_round_id=gbd_round_id)[#age_group_id %in% age_groups,
                         ,.(age_group_id, age_start = age_group_years_start, age_end = age_group_years_end)]


## (2) Retrieve age-specific data
age_spec_dt <- get_bundle_data(bundle_id = ADDRESS , gbd_round_id=gbd_round_id, decomp_step = decomp_step)


## (3) Prep for fitting the model
age_spec_dt <- rbind(age_spec_dt, ages, fill=TRUE)

age_spec_dt[, `:=` (age_mid = (age_start + age_end)/2,
                    cases = mean * effective_sample_size,
                    effective_sample_size = ifelse(is.na(effective_sample_size),1, effective_sample_size))]

# Store values of the spline vars
ageSpline <- as.data.table(rcspline.eval(x=age_spec_dt$age_mid, knots=c(1,5,10,60), inclx=TRUE))
setnames(ageSpline, c('ageS1', 'ageS2', 'ageS3'))
age_spec_dt <- cbind(age_spec_dt, ageSpline)


## (4) Model and predict age/sex pattern
## *fit a glm regressing number of cases on age
## *fit age as a restricted cubic spline
## *include effective sample size as an offset
## *log(cases/effective sample size) = b0 + b1AGE1 + b2AGE2 + b3AGE3
## *log(cases) = b0 + b1AGE1 + b2AGE2 + b3AGE3 + log(effective sample size)

m1 <- glm(cases ~ rcs(x=age_mid, parms=c(1,5,10,60), name='ageS') + offset(log(effective_sample_size)),
          family="poisson", data=age_spec_dt)

# Beta hats
b0_hat <- summary(m1)$coefficients[1,1]
b1_hat <- summary(m1)$coefficients[2,1]
b2_hat <- summary(m1)$coefficients[3,1]
b3_hat <- summary(m1)$coefficients[4,1]

# Calculate fitted values (without offset)
age_spec_dt[, ageSexCurve := b0_hat + ageS1*b1_hat + ageS2*b2_hat + ageS3*b3_hat]
# Calc the standard error of the predicted expected value
age_spec_dt$ageSexCurveSe <- predict.glm(m1, newdata=age_spec_dt, type="link", se.fit=TRUE)$se.fit


## (5) Clean file and save

# Subset the dataset to modeled age groups
ageSexCurve <- age_spec_dt[is.na(seq), .(age_group_id, ageSexCurve, ageSexCurveSe)]

# Create rows for each sex
row_list <- rep(seq_len(nrow(ageSexCurve)), each = 2)
ageSexCurve <- ageSexCurve[row_list,][, sex_id := seq_len(.N), by=age_group_id][order(sex_id, age_group_id)]

# Save output
saveRDS(ageSexCurve, file = paste0(interms_dir,"/FILEPATH"))

}

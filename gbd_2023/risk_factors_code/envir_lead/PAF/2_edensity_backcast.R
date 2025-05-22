# Project: RF: Lead Exposure
#
# Purpose: This code implements the ensemble density approach to estimating 1) direct bone lead to CVD PAFs, 2) the proportion
# of people with blood lead levels above specified thresholds (not a formal GBD output, but something we're producing
# in response to regular collaborator and agency requests), and 3) IQ shifts resulting from lead exposure.

### SETUP THE ENVIRONMENT ###

# Start with a clean slate
rm(list=ls())

# Load packages
library(arrow)
library(data.table)
library(parallel)
library(dplyr)
library(feather)
library(readr)

# Establish code directories
CODE_DIR <- 'FILEPATH'
SHARED_FUN_DIR <- 'FILEPATH'

# Load shared functions
source(file.path(CODE_DIR, 'get_edensity.R'))
source(file.path(SHARED_FUN_DIR, 'get_age_metadata.R'))
source(file.path(SHARED_FUN_DIR, 'get_demographics.R'))
source(file.path(SHARED_FUN_DIR, 'get_ids.R'))
source(file.path(SHARED_FUN_DIR, 'get_location_metadata.R'))



# Determine if this is an interactive session or a batch job
print(commandArgs())
INTERACTIVE = commandArgs()[2]=='--interactive'

# If running as a batch job, read in the command line arguments
if (INTERACTIVE == FALSE) {  
  args <- commandArgs(trailingOnly = T)
  
  release  <- as.integer(args[1])
  run_id   <- as.integer(args[2])
  version  <- as.integer(args[3])
  threads  <- as.integer(args[4])
  n_draws  <- as.integer(args[5])
  loc      <- as.integer(args[6])
  run_iq   <- as.logical(args[7])
  type_dir <- as.character(args[8])
  
  print(args)
  
} else { # running interactively
  release <- 16
  run_id  <- 221323
  version <- 26
  threads <- 16
  n_draws <- 250
  loc <- 198
}



# Set hard-coded constants
LOC_SET <- 22
IQ_AGE <- 2L
MAX_AGE <- 99L
BLOOD_TMREL <- 0.016 


warning('Processing job for release ', release, ', run ', run_id, ', version ', version, ', location ', loc, '...')
print(paste0('Processing job for release ', release, ', run ', run_id, ', version ', version, ', location ', loc, '...'))

# Get metadata and demographic dimensions for the current GBD release
warning('Reading metadata and demographic dimensions for release ', release, '...')
print(paste0('Reading metadata and demographic dimensions for release ', release, '...'))
loc_meta <- get_location_metadata(location_set_id = LOC_SET, release_id = release)
loc_meta[, super_region_id := as.factor(super_region_id)] # used as categorical variable in a regression, so need to make a factor

demog <- get_demographics('epi', release_id = release)
first_year <- demog$year_id[1] 
last_year  <- demog$year_id[length(demog$year_id)]

# Calculate derived constants
draws <- paste0('draw_', 0:(n_draws-1))
max_draw <- as.integer(n_draws - 1)
oldtime_start <- first_year - MAX_AGE


# Paste together file paths
warning('Reading ST-GPR inputs...')
print('Reading ST-GPR inputs...')
gpr_input_dir <- file.path('FILEPATH', run_id, 'draws_temp_0')
output_root <- file.path('FILEPATH', paste0('release_', release))
version_dir <- file.path(output_root, paste0('version_', version))




# LOAD INPUTS ######################################
warning('Reading mean -> SD model...')
print('Reading mean -> SD model...')
# Load mean -> SD model (used to estimate missing SD values based on mean and super-region)
mod <- readRDS(file.path(version_dir, 'mean_sd_mod.RDS'))


# Read in the ensemble weight for envir_lead_blood
warning('Reading ensemble weights...')
print('Reading ensemble weights...')
weights <- feather::read_feather(file.path(version_dir, "blood_lead_weights.feather"))
setDF(weights)  # get_edensity function needs weights to be a data.frame -- converting here


warning('Reading IQ shift coefficients...')
print('Reading IQ shift coefficients...')
# pull in coefficients for IQ shift relative risks
iq_rr <- feather::read_feather(file.path(version_dir, 'blood_lead_coefs.feather')) 
setDT(iq_rr)


# Load blood to bone conversion coefficients
# create 1000 draws of the blood to bone conversion factor -- 0.05 conversion factor from Howard Hu's paper
# pulling draws in log-space to avoid negative draws
bone_coeff_mean  <- log(0.05)
bone_coeff_lower <- log(0.046)
bone_coeff_upper <- log(0.055)
bone_coeff_sd <- (bone_coeff_upper - bone_coeff_lower) / (2 * qnorm(0.975)) 

set.seed(89132)
bone_coefs <- exp(rnorm(n_draws, mean = bone_coeff_mean, sd = bone_coeff_sd)) 


# Load bone:CVD RR draws
warning('Reading bone:CVD RR draws...')
print('Reading bone:CVD RR draws...')
cvd_rr <- feather::read_feather(file.path(version_dir, 'mrbrt_bone_cvd_rr_draws.feather'))
setDT(cvd_rr)
cvd_rr[bone_lead < BLOOD_TMREL, bone_lead := BLOOD_TMREL] # set bone lead to the TMREL if it's below the TMREL as offset for later log transform


# calc duration of each age group #######################################################
# We need to know the duration spent in each age group to determine cumulative exposure
age_meta <- get_age_metadata(age_group_set_id = 24, release_id = release)[order(age_group_days_end), ]
age_meta[which(age_group_years_end == max(age_group_years_end)), `:=` (age_group_years_end = MAX_AGE+1, age_group_days_end = ((MAX_AGE+1)*365)-1)] # constrain oldest age group to end at the max age
age_meta[, duration := as.integer(1 + age_group_days_end - age_group_days_start) / 365] 
age_meta[, rep := as.integer(ceiling(duration))] # we want to repeat rows for the number of years in an age group

age_expanded = data.table(age_group_id = as.integer(rep(age_meta$age_group_id, age_meta$rep)),
                          duration = rep(age_meta$duration, age_meta$rep),
                          age = rep(age_meta$age_group_years_start, age_meta$rep))
age_expanded[duration > 1, duration := 1] 
age_expanded[, age := age + (1:.N) - 1, by = age_group_id][, age := as.integer(age)]


# Read in the demographic_id link file (we save blood edens draws with this demog_id, rather than seperate year/location/age/sex variables to save disk space)
demog_link <- feather::read_feather(file.path(version_dir, 'demog_link.feather'))


# Read in the draw files that are being fed into this by the lead_backcast_launch.R file
gpr_draws <- fread(file.path(gpr_input_dir, paste0(loc, '.csv')))
gpr_draws <- melt.data.table(gpr_draws, id.vars = c('location_id', 'year_id', 'age_group_id', 'sex_id'),
                             variable.name = 'draw', value.name = 'blood_lead', variable.factor = FALSE)

gpr_draws[, draw := as.integer(gsub('draw_', '', draw))]




# CREATE FULL COHORT TIME-SERIES OF BLOOD LEAD EXPOSURE ########################################

# Create data table with same demographics as draw file, but for the burn in time period
oldtime <- unique(copy(gpr_draws)[, .(location_id, sex_id, age_group_id, draw)])[, merge := 1]
oldtime <- merge(oldtime, data.table(year_id = oldtime_start:1920, merge = 1), by = 'merge', allow.cartesian = T, all = T)[, merge := NULL]
oldtime[, blood_lead := BLOOD_TMREL] 

# Create data table with data linearly increasing from tmrel of 1920 to earliest modeled year, for years 1921-earliest year
#first determine what the oldest year is in the stgpr draws
stgpr_start_yr <- min(gpr_draws$year_id)

#now calculate
linear <- copy(gpr_draws)[year_id == stgpr_start_yr, ][, .(location_id, sex_id, age_group_id, draw, blood_lead)][, merge := 1]
linear <- merge(linear, data.table(year_id = 1921:(stgpr_start_yr - 1), merge = 1), by = 'merge', allow.cartesian = T, all = T)[, merge := NULL]
linear[, blood_lead := ((1:.N) * (blood_lead - BLOOD_TMREL) / (stgpr_start_yr - 1920)) + BLOOD_TMREL, by = c('location_id', 'sex_id', 'age_group_id', 'draw')]

# Combine all time periods
alltime <- rbind(oldtime, linear, gpr_draws)

# use 1-year age to determine year of birth (yob)
alltime <- merge(alltime, age_expanded, by = 'age_group_id', allow.cartesian = T, all.y = T)
alltime[, yob := year_id - as.integer(age)]
alltime <- alltime[yob >= oldtime_start, ] # we don't need any values for anyone born before oldtime_start -- drop for efficiency
alltime[, super_region_id := loc_meta[location_id == loc, super_region_id]] # need SR for sd model prediction below
alltime <- alltime[order(draw, location_id, sex_id, yob, age), ]

setnames(alltime, 'blood_lead', 'mean')
alltime[, 'exp_sd' := exp(predict(mod, alltime))]
setnames(alltime, 'mean', 'blood_lead')



# bone lead processing #######################################
## PROCESS EDENSITY & ESTIMATE BONE LEAD & PROPOPROTIONS ABOVE THRESHOLDS ##############################################

## functions ##################
# Create a function to compile the ensemble density for a given demographic group
compile_dens <- function(mean, sd, groups) {
  dens <- mapply(get_edensity, mean = mean, sd = sd, MoreArgs = list(weights = weights)) 
  dens <- rbindlist(lapply(groups, function(group) {data.table(do.call(cbind, dens[, group][1:2]), group_id = group)}))
  return(dens)
}

# Create a function to estimate bone lead edensity for a given draw and YOB
get_bone_dens <- function(d, y, n_samples = 1000) {
  # To avoid extreme draws from the edensity, we are going to Windsorize, 
  # and remove the lowest and highest 5% of edraws -- buffer is number of 
  # extra draws needed to get correct number after Windsorization
  sample_buffer <- as.integer(round(n_samples * 0.05))
  n_samples_buffered <- n_samples + (2 * sample_buffer)
  
  sub <- alltime[draw == d & yob == y, ][, .(draw, location_id, sex_id, age, age_group_id, year_id, blood_lead, exp_sd, duration)]
  sub[, group_id := 1:.N]
  
  dens <- try(compile_dens(mean = sub[, blood_lead], sd = sub[, exp_sd], groups = 1:nrow(sub)), silent = T)
  dens[, prob := fx / sum(fx), by = 'group_id']
  
  dens <- rbindlist(lapply(1:nrow(sub), function(group) 
    data.table(blood_lead_edens = sample(dens[group_id==group, x], size = n_samples_buffered, replace = TRUE, prob = dens[group_id==group, prob]),
               group_id = group)))
  
  dens <- merge(dens, sub, by = 'group_id', all = T)
  dens <- dens[order(group_id, blood_lead_edens), ]
  dens[, edraw := (1:n_samples_buffered) - (sample_buffer + 1), by = 'group_id']
  
  # Windsorize (i.e. drop smallest and largest 5% of draws)
  dens <- dens[edraw >= 0 & edraw < n_samples, ]
  
  # Rescale edraws such that mean is equal to the original ST-GPR estimate
  dens[, blood_lead_edens := blood_lead_edens * blood_lead / mean(blood_lead_edens), by = 'group_id']
  
  dens <- dens[order(edraw, location_id, sex_id, age, age_group_id), ]
  dens[, bone_lead := cumsum(blood_lead_edens * duration * bone_coefs[d + 1]), by = c('edraw', 'location_id', 'sex_id')]
  
  dens <- dens[year_id %in% first_year:last_year, .(draw, edraw, location_id, sex_id, age_group_id, year_id, bone_lead, blood_lead_edens)]
  return(dens)
}


# Define a function to return the relative risk for a given demographic group, draw, and vector of bone lead values
calc_pafs <- function(x, a, d) {
  bl <- x[age_group_id==a & draw==d, ]
  rr <- cvd_rr[age_group_id==a & draw==d, ]
  
  # MR-BRT returns RR estimates at intervals rather than a continuous function, so we need to interpolate to the bone lead values
  bl_rr <- cbind(bl, rr = approx(x = log(rr$bone_lead), y = rr$rr, xout = log(bl$bone_lead), rule = 2)$y)
  bl_rr[, paf := (rr - 1)/rr]
  
  # Take the mean of the PAFs for each draw (collapsing the edensity draws)
  bl_rr <- bl_rr[, .(paf = mean(paf)), by = c('draw', 'location_id', 'sex_id', 'age_group_id', 'year_id')]
  return(bl_rr)
}



# Define the function to determine the proportion of people with blood lead above a given threshold
get_pr_above <- function(x, threshold) {
  x <- x[, .(pr_above = mean(blood_lead_edens > threshold)), by = c('draw', 'location_id', 'sex_id', 'age_group_id', 'year_id')]
  x[, threshold := threshold]
  return(x)
}


# Define the function to call get_bone_dens, calc_pafs, and get_pr_above for a given draw
# This function sets off the chain of events defined in all the above functions
process_draw <- function(d, pr_above_thresholds = c(5, 10)) {
  n_edraws <- 1000
  
  tmp <- rbindlist(lapply(oldtime_start:last_year, function(y) get_bone_dens(d, y, n_edraws)))
  
  pafs <- rbindlist(lapply(unique(cvd_rr$age_group_id), function(a) calc_pafs(tmp, a, d)))
  
  pr_above <- rbindlist(lapply(pr_above_thresholds, function(t) get_pr_above(tmp, threshold = t)))
  
  # We need to return a single draw of bone lead from the ensemble, so we randomly pick a draw to return here
  random_edraw <- sample((1:n_edraws) - 1, 1)
  bone_lead <- tmp[edraw == random_edraw, ]
  # We've estimated for each year of age, so we need to take the mean across all years of age within each age group
  bone_lead <- bone_lead[, .(bone_lead = mean(bone_lead)), by = c('draw', 'location_id', 'sex_id', 'age_group_id', 'year_id')]
  
  return(list(pafs = pafs, pr_above = pr_above, bone_lead = bone_lead))
}

## run all functions #######################################
# Run the edensity, calcuting PAFs and proportion of people above a threshold for all draws
# Note that mclapply can fail on some draws if the thread times out or exceeds memory available
# on the specific thread, so we need to check for this and re-run those draws
start <- Sys.time()

missing_draws <- 0:max_draw
pr_above <- data.table()
bone_cvd_pafs <- data.table()
bone_lead <- data.table()
count <- 1

warning("while loop")
print("while loop")

tryCatch({
  while (TRUE) {
    out_list <- mclapply(missing_draws, process_draw, mc.cores = threads, mc.preschedule = FALSE)
    
    # Compile pr above draws
    pr_above <- rbind(pr_above, 
                      rbindlist(lapply(out_list, function(x) x$pr_above)), 
                      fill = T)   
    
    # Compile PAF draws
    bone_cvd_pafs <- rbind(bone_cvd_pafs, 
                           rbindlist(lapply(out_list, function(x) x$pafs)),
                           fill = T) 
    
    # Compile bone lead draws
    bone_lead <- rbind(bone_lead, 
                       rbindlist(lapply(out_list, function(x) x$bone_lead)),
                       fill = T)
    
    # Check to see if we have the correct number of draws (i.e. no failures of mclapply)
    complete_draws <- unique(bone_cvd_pafs$draw)
    n_complete_draws <- length(complete_draws)
    
    if (n_complete_draws == n_draws) {
      message('all draws complete on attempt ', count)
      break
    } else {
      missing_draws <- setdiff(0:max_draw, complete_draws)
      message('need to complete missing draws:', missing_draws)
      count <- count + 1
    }
  }
}, error = function(e){
  #do nothing
})

difftime(Sys.time(), start)

# edited PAF and bone lead exp draws #########################################################
warning("Reshaping PAF draws")
print("Reshaping PAF draws")
# Reshape draws to wide and add the measure id as required by the uploader
bone_cvd_pafs <- dcast.data.table(bone_cvd_pafs, location_id + age_group_id + sex_id + year_id ~ paste0('draw_', draw), value.var = 'paf')
bone_cvd_pafs[, measure_id := 3]
bone_cvd_pafs[, cause_id := 493]
message('PAF reshape complete')

warning("Reshaping bone lead exposure draws")
print("Reshaping bone lead exposure draws")
# Reshape draws to wide and add the measure id as required by the uploader
bone_lead <- dcast.data.table(bone_lead, location_id + age_group_id + sex_id + year_id ~ paste0('draw_', draw), value.var = 'bone_lead')
bone_lead[, measure_id := 19]
bone_lead[, metric_id := 3]
bone_lead[, cause_id := 243]
message('Exposure reshape complete')

# save ###########################################################
warning("save draws")
print("save draws")
# Save the draw files
write.csv(bone_cvd_pafs, file.path(type_dir, 'bone_pafs', paste0(loc, '_bone_lead_edens.csv')), row.names = F)
message('Bone PAFs exported to ', file.path(type_dir, 'bone_pafs', paste0(loc, '_bone_lead_edens.csv')))

write.csv(bone_lead, file.path(type_dir, 'bone_backcast', paste0(loc, '_bone_lead.csv')), row.names = F)
message('Bone lead exposure exported to ', file.path(type_dir, 'bone_backcast', paste0(loc, '_bone_lead.csv')))

write_parquet(pr_above, file.path(type_dir, 'pr_above', paste0(loc, '_pr_above.parquet')))
message('Proportion above threshold exported to ', file.path(type_dir, 'pr_above', paste0(loc, '_pr_above.parquet')))


# IQ shift calc ###########################################################
### IQ SHIFT ESTIMATION: CREATE IQ-SHIFT EXPOSURE, RUN E-DENSITY, AND CALCULATE IQ-SHIFTS ###
if(run_iq==T){
  warning("IQ shift")
  print("IQ shift")
  # Create IQ blood lead exposure variable
  iq_exp <- copy(alltime)
  iq_exp[yob >= oldtime_start - MAX_AGE + IQ_AGE, # for the oldest YOBs there are no rows @ IQ age, so leave those empty
         iq_exp := ifelse(age <= IQ_AGE, blood_lead, max(blood_lead * (age == IQ_AGE))), # exposure is data below IQ age, but data @ IQ age for older groups
         by = c('draw', 'location_id', 'sex_id', 'yob')]
  
  # Collapse single-year ages to GBD age_groups
  iq_exp <- iq_exp[year_id >= first_year, ]
  iq_exp <- iq_exp[, .(mean = mean(iq_exp)), by = c('age_group_id', 'location_id', 'super_region_id', 'sex_id', 'year_id', 'draw')]
  
  warning("IQ predict")
  print("IQ predict")
  # Predict the SD based on the mean 
  iq_exp[, 'iq_exp_sd' := exp(predict(mod, iq_exp))]
  setnames(iq_exp, 'mean', 'iq_exp_mean')
  
  # Create demographic group variable that we'll use to keep track of things
  iq_exp[, group_id := .GRP, by = c('location_id', 'year_id', 'age_group_id', 'sex_id')]
  iq_exp <- iq_exp[order(draw, group_id), ]
  
  #if IQ exp mean is equal to 0, change this to the TMREL. This is because in the get_edensity function we will be dividing by this value, and it throws and error
  #if it trys to divide by 0
  iq_exp[iq_exp_mean==0,iq_exp_mean:=BLOOD_TMREL]
  
  iq_groups <- unique(iq_exp$group_id)
  
  # Run the e-density
  get_iq_shifts <- function(d) {
    dens <- compile_dens(mean = iq_exp[draw == d, iq_exp_mean], sd = iq_exp[draw == d, iq_exp_sd], groups = iq_groups)
    dens <- data.table(dens, iq_rr[draw == d, ])
    
    # translate ensemble distribution x values into their corresponding shift in IQ (rr*log(exp+1)), 
    # setting shift to 0 at exposure of TMREL (currently 0.016)
    dens[, iq_shift := ifelse(x < BLOOD_TMREL, 0, log(x + 1) * iq_rr)]
    
    # using frequencies at different exposure values, determine average IQ shift for each distribution by summing
    # IQ shift by the corresponding frequency. But have to rescale it because the frequencies do not add to 1
    iq_shifts <- dens[,  .(iq_shift = sum(iq_shift * fx/sum(fx))), by = c('draw', 'group_id')]
    
    return(iq_shifts)
  }
  start <- Sys.time()
  warning("run edensity for iq")
  print("run edensity for iq")
  iq_shifts <- rbindlist(mclapply(0:max_draw, get_iq_shifts, mc.cores = threads))
  difftime(Sys.time(), start)
  
  iq_shifts[, draw := paste0('draw_', draw)]
  iq_shifts <- dcast.data.table(iq_shifts, group_id ~ draw, value.var = 'iq_shift')
  iq_shifts <- merge(iq_exp[draw == 0, .(location_id, year_id, age_group_id, sex_id, group_id)], iq_shifts, by = 'group_id', all = T)
  iq_shifts[, group_id := NULL]
  iq_shifts[, measure_id := 19]
  
  ### ADD SOME VALIDATION CHECKS AND EXPORT IF GOOD
  warning("save IQ")
  print("save IQ")
  write_excel_csv(iq_shifts, file.path(type_dir, 'iq_shifts', paste0(loc, '_iq_shift_edens.csv')))
}






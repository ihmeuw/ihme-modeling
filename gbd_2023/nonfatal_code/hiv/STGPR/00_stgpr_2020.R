### HIV ST-GPR custom model
### Expected runtime: about 2 hours

###############################################################################################################
## Set Up
Sys.umask("0002")
rm(list=ls())
library(foreign)
library(ggplot2) ; library(reshape2); library(RMySQL)
library(mortdb, lib.loc = "FILEPATH")
invisible(sapply(list.files("/share/cc_resources/libraries/current/r/", full.names = T), source))
user <- Sys.getenv("USER")
s.shell <- 'S_SHELL'
p.shell <- 'P_SHELL'
cluster.project <- 'PROJECT'

# sourcing the new shared function
source(paste0("FILEPATH", "/rt-shared-functions/cluster_functions.R"))

#### Arguments and run name
date <- 'RUNNAME'

stgpr.path <- 'FILEPATH'
dir.create(stgpr.path)
code_dir <- 'FILEPATH'
release_id <- 'RELEASE ID'
step_1 = T
step_2 = T
step_3 = T
step_4 = T

gbd_year <- 2023

#### get_locations and get_locations lowest
loc.table <- data.table(get_locations(hiv_metadata = T, gbd_year = gbd_year))
write.csv(loc.table, 'FILEPATH', row.names = F) 
loc.table.lowest <- data.table(get_locations(level = 'lowest', hiv_metadata = T, gbd_year = gbd_year))
write.csv(loc.table.lowest, 'FILEPATH', row.names = F) 

####################################################
#Edit parameter file
####################################################
file.copy(from = paste0(code_dir,'/params.csv'), to = paste0(FILEPATH, date, '/params.csv'))

###############################################################################################################
# ## Prep COD data
if(!file.exists(paste0(stgpr.path, '/cod_data_2020', '.csv'))) {
  HIV <- get_cod_data(cause_id = 298,
                      cause_set_id = 2,
                      release_id = release_id) 
  
  HIV <- HIV[!age_group_id %in% c(22, 27), ]
  HIV <- HIV[rate != 'NA']
  
  HIV <- HIV[data_type_name %in% c('VR', 'VR-S', 'MITS diagnosed') |
               (location_id %in% c(129, 18, 20) &
                  data_type_name %in% c('VA', 'Surveillance')), ]
  write.csv(HIV, paste0(stgpr.path, '/cod_data_2020', '.csv'))
}

file.copy(from = 'FILEPATH', to = 'FILEPATH')

if(!file.exists(paste0(stgpr.path, 'cod_chn.csv'))){
  submit_job(script = paste0(code_dir, "/prepping_china.R"), queue = "all.q", memory = "5G", threads = "4",
             time = "01:00:00", name = paste0("prep_china"), archive = T,
             args = c(date))
}



###############################################################################################################
## Run ST-GPR
## Step 1
## Get data, change ST and GPR parameters, and run the preliminary linear model

# Set seed 
set.seed = 123

# Change run name in 01_linear_model_ver2.do first 
# extend years by changing line 289 (expand 55) to expand 56+ (1969 + x)
if(step_1){
  submit_job(script = paste0(code_dir, "/01_linear_model_ver2.do"), queue = "long.q", memory = "10G", threads = "2",
             time = "00:30:00", name = "st_01", archive = T, r_shell = s.shell, language = "stata")
  file.exists(paste0(stgpr.path, '/linear_predictions.csv'))
}


## Step 2
## Generate draws
if(step_2){
  linear_predictions <- fread(paste0(stgpr.path,"/linear_predictions.csv"))
  params <- fread(paste0(stgpr.path, "/params.csv"))
  lparams = params
  locs <- unique(linear_predictions$location_id)
  
  ##append locs that are missing params
  append <- data.table(
    location_id = setdiff(locs, params$location_id),
    ihme_loc_id = loc.table[location_id %in% setdiff(locs, params$location_id), ihme_loc_id],
    lambda = 0.2,
    omega = 1,
    zeta = 0.95,
    scale = 5,
    amp = 'regional'
  )
  params <- rbind(params, append[!is.na(location_id)], fill=T)
  
  write.csv(params, paste0(stgpr.path, "/params.csv"), row.names = F)
  params <- fread(paste0(code_dir, "/params.csv"))
  
  for(l in locs){
    lambdaa = lparams[location_id == l, lambda]
    omega = lparams[location_id == l, omega]
    zeta = lparams[location_id == l, zeta]
    lambdaa = 1
    omega = 1
    zeta = 0.95
    submit_job(script = paste0(code_dir, "/run_st.py"), queue = "long.q", memory = "50G", threads = "5",
               time = "00:30:00", name = paste0("step2_", l), archive = T, r_shell = p.shell, 
               args = c(l, lambdaa, omega, zeta, date))
  }

  assertable::check_files(paste0(stgpr.path, '/st/', locs, '.csv'), continual = T)
}


## Step 3
## Prepare location-specific data for GPR
if(step_3){

  submit_job(script = paste0(code_dir, "/03_prep_gpr.py"), queue = "long.q", memory = "5G", threads = "4",
             time = "01:00:00", name = paste0("step3"), archive = T, r_shell = p.shell, 
             args = c(date))
  file.exists(paste0(stgpr.path, '/forgpr.csv'))
  
}


### Step 4

# Check that step 3 was successful 
file.exists(paste0(stgpr.path, '/forgpr.csv'))

# Run python script, step 4

# Check that step 4 was successful 
file.exists(paste0(stgpr.path, '/gpr_results.csv'))



# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
  } else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
  }

project <- "PROJECT "
sge.output.dir <- " -o FILEPATH -e FILEPATH "
#sge.output.dir <- "" # toggle to run with no output files

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","openxlsx","pbapply")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

decomp <- "iterative"
n_draws <- 1000
cores.provided <- 4

hap_bundle_id <- 4736
clean_bundle_id <- 6164
coal_bundle_id <- 6167
crop_bundle_id <- 6173
dung_bundle_id <- 6176
wood_bundle_id <- 6170

fuel_type <- c("solid", "coal", "crop", "dung", "wood")
# fuel_type <- "wood"

desc <- "DESCRIPTION"

# Functions and directories------------------------------------------------

# functions
source(file.path(central_lib,"FILEPATH/get_bundle_data.R"))
source(file.path(central_lib,"FILEPATH/upload_bundle_data.R"))
source(file.path(central_lib,"FILEPATH/validate_input_sheet.R"))
source(file.path(central_lib,"FILEPATH/validate_crosswalk_input_sheet.R"))
source(file.path(central_lib,"FILEPATH/save_bundle_version.R"))
source(file.path(central_lib,"FILEPATH/save_crosswalk_version.R"))
source(file.path(central_lib,"FILEPATH/get_crosswalk_version.R"))
source(file.path(central_lib,"FILEPATH/get_bundle_version.R"))

home_dir <- "FILEPATH"

crosswalk_version <- fread(file.path(home_dir,"FILEPATH/xwalk_version_metadata.csv"))
# only take the latest crosswalk version for each fuel type
crosswalk_version[order(date),N:=.N:1,by=c("fuel_type")]
crosswalk_version <- crosswalk_version[N==1]


# Launch st-gpr -----------------------------------------------------------

central_root <- 'FILEPATH'
setwd(central_root)

source('FILEPATH/register.R')
source('FILEPATH/sendoff.R')
source("FILEPATH/plot_gpr.R")
source("FILEPATH/utility.r")

# make this empty vector to track run_ids for plotting later
run_ids <- c()

for (n in fuel_type) {
  
  disaggregation_config <- fread(file.path("FILEPATH","disaggregation_config.csv"))
  
  # set description
  if (n=="clean"){
    description <- paste0(desc, ", clean")
  } else if (n=="coal") {
    description <- paste0(desc, ", coal")
  } else if (n=="crop") {
    description <- paste0(desc, ", crop")
  } else if (n=="dung") {
    description <- paste0(desc, ", dung")
  } else if (n=="wood"){
    description <- paste0(desc, ", wood")
  } else {
    description <- paste0(desc, ", solid")
  }
  
  # Arguments
  new_config <- copy(disaggregation_config[nrow(disaggregation_config)])
  new_config[,gbd_round_id:=7]
  new_config[,notes:=description]
  new_config[,decomp_step:=decomp]
  new_config[,crosswalk_version_id:=crosswalk_version[fuel_type==n,crosswalk_version_id]]
  new_config[,holdouts:=0] # Keep at 0 unless you want to run cross-validation
  new_config[,draws:=n_draws]
  new_config[,location_set_id:=22] # using covariate computation loc set, because it has loc_ids that are deprecated in loc set 35
  new_config[,gpr_draws:=n_draws]
  new_config[,year_end:=2020]
  new_config[,rake_logit:=1]
  new_config[,prediction_units:="Proportion of Individuals"]
  new_config[,gpr_amp_cutoff:=NA]
  new_config[,gpr_amp_factor:=1]

  
  new_config[,stage_1_model_formula:="data ~ maternal_educ_yrs_pc + prop_urban + (1|level_1) + (1|level_2)"]
  new_config[,gbd_covariates:="maternal_educ_yrs_pc, prop_urban"]
  
  
  if (n%in%c("clean","solid")){
    new_config[,transform_offset:=NA]
    new_config[,density_cutoffs:="4"]
    new_config[,st_lambda:="0.07,0.07"]
    new_config[,st_omega:="0,0"]
    new_config[,st_zeta:="0.002,0.03"]
    new_config[,gpr_scale:="18,18"]
    new_config[,gpr_amp_method:="global_above_cutoff"]
  } else {
    new_config[,transform_offset:=0.00001]
    new_config[,density_cutoffs:="0,2,4"]
    new_config[,st_lambda:="0.05,0.05,0.05"]
    new_config[,st_omega:="0,0,0"]
    new_config[,st_zeta:="0.001,0.002,0.03"]
    new_config[,gpr_scale:="18,18,18"]
    new_config[,gpr_amp_factor:=1]
    new_config[,gpr_amp_method:="global_above_cutoff"]
  }
  
  # set bundle_id
  if (n=="clean"){
    new_config[,bundle_id:=clean_bundle_id]
  } else if (n=="coal") {
    new_config[,bundle_id:=coal_bundle_id]
  } else if (n=="crop") {
    new_config[,bundle_id:=crop_bundle_id]
  } else if (n=="dung") {
    new_config[,bundle_id:=dung_bundle_id]
  } else if (n=="wood"){
    new_config[,bundle_id:=wood_bundle_id]
  } else {
    new_config[,bundle_id:=hap_bundle_id]
  }
  
  # set me_id
  if (n=="clean"){
    new_config[,modelable_entity_id:=23829]
  } else if (n=="coal") {
    new_config[,modelable_entity_id:=23830]
  } else if (n=="crop") {
    new_config[,modelable_entity_id:=23832]
  } else if (n=="dung") {
    new_config[,modelable_entity_id:=23833]
  } else if (n=="wood"){
    new_config[,modelable_entity_id:=23831]
  } else { # solid
    new_config[,modelable_entity_id:=2511]
  }
  
  disaggregation_config <- rbind(disaggregation_config,new_config,fill=T,use.names=T)
  
  disaggregation_config[,model_index_id:=1:.N]
  
  write.csv(disaggregation_config,file.path(home_dir,"disaggregation_config.csv"),row.names=F)
  
  # Registration
  run_id <- register_stgpr_model(path_to_config=file.path(home_dir,"disaggregation_config.csv"),model_index_id = nrow(disaggregation_config))
  
  # make a vector to track run_ids for plotting later
  run_ids <- c(run_ids,run_id)
  
  # Sendoff
  stgpr_sendoff(run_id, project)
  
  # make run directories
  if (n=="clean"){
    run_dir <- file.path("FILEPATH",run_id)
  } else if (n=="coal") {
    run_dir <- file.path("FILEPATH",run_id)
  } else if (n=="crop") {
    run_dir <- file.path("FILEPATH",run_id)
  } else if (n=="dung") {
    run_dir <- file.path("FILEPATH",run_id)
  } else if (n=="wood") {
    run_dir <- file.path("FILEPATH",run_id)
  } else {
    run_dir <- file.path("FILEPATH",run_id)
  }
  
  dir.create(run_dir,recursive=T)
  
}

# save the run_id tracker just in case I need it later
tracker <- cbind(fuel_type,run_ids) %>% as.data.table
tracker[,date:=as.character(Sys.Date())]
tracker[,run_ids:=as.numeric(run_ids)]

temp <- read.csv(paste0(home_dir,"FILEPATH/run_id_tracker.csv")) %>% as.data.table
temp <- rbind(temp,tracker,use.names=T,fill=T)
temp[,run_ids:=as.numeric(run_ids)]
temp <- unique(temp)

write.csv(temp,paste0(home_dir,"FILEPATH/run_id_tracker.csv"),row.names=F)


## Diagnostics

# look at amplitudes
amps <- model_load(run_id, "amp_nsv")

source("FILEPATH/plot_gpr.R")
tracker <- fread("FILEPATH/run_id_tracker.csv")
tracker <- tracker[date=="DATE"]


# helper functions for once the run finishes
for (n in 1:nrow(tracker)){
  plot_gpr(run.id = tracker[n,run_ids], output.path = paste0(home_dir,"/run/",tracker[n,fuel_type],"/",tracker[n,run_ids],"/plot_air_hap_disagg.pdf"))
}


#----HEADER----------------------------------------------------------------------------------------------------------------------
# Author: 
# Owner: 
# Purpose: Save results for air_pm

#------------------------Setup -------------------------------------------------------
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

project <- "-P proj_erf "
sge.output.dir <- "FILEPATH"
#sge.output.dir <- "" # toggle to run with no output files

# load packages, install if missing
pacman::p_load(data.table, magrittr, ini, parallel,openxlsx,pbapply,fst)

#Versions for upload:
exp.version <- 'v5_2'
cores.provided <- 20

# Bundle IDs
ambient_bundle <- 1160
stgpr_bundle <- 8222
bundle.dir <- file.path(j_root, "FILEPATH")
xw.dir <- file.path(j_root, "FILEPATH")
stgpr_xw <- 20720

#draw directories
# exp.dir <-file.path("/share/epi/risk/air_pm/exp", exp.version, "final_draws")
# full.exp.dir <- file.path("/share/epi/risk/air_pm/exp",exp.version,"interpolated_draws")
# covariate.dir <- file.path(j_root,"WORK/05_risk/risks/air_pm/products/covariate",exp.version)
# exp.out.dir <- file.path(j_root,"WORK/05_risk/risks/air_pm/products/exp",exp.version)
# dir.create(full.exp.dir)
# dir.create(covariate.dir, recursive=T)
grid.dir <- paste0("FILEPATH/air_pm/exp/gridded/",exp.version,"/draws/")
# exp.dir <- paste0(j_root, "/WORK/05_risk/risks/air_pm/products/exp/45/")
exp.dir <- paste0("FILEPATH/air_pm/exp/results/",exp.version,"/")
dir.create(exp.dir, recursive = T,showWarnings = F)

#parameters
years <- c(1990, 1995, 1998:2022)
# years <- c(1998:2022)
# NOTE: for GBD2020, we're going to extrapolate to 2022 simply by copying over 2020 results for 2021/2022
# As this is an annual risk factor, we can just come back and remodel next year:)
location_set_id <- 35

ndraws <- 500
draw_cols <- paste0("draw_",0:(ndraws-1))

#----------------------------Functions---------------------------------------------------

source(file.path(central_lib, "current/r/save_results_epi.R"))
source(file.path(central_lib, "current/r/save_results_sdg.R"))
source(file.path(central_lib, "current/r/save_results_covariate.R"))
source(file.path(central_lib, "current/r/save_results_risk.R"))
source(file.path(central_lib, "current/r/get_demographics.R"))
source(file.path(central_lib, "current/r/get_location_metadata.R"))
locations <- get_location_metadata(location_set_id, release_id = 16)
source(file.path(central_lib, "current/r/get_draws.R"))
source(file.path(central_lib, "current/r/get_population.R"))
source(file.path(central_lib, "current/r/interpolate.R"))
source(file.path(central_lib, "current/r/get_bundle_data.R"))
source(file.path(central_lib, "current/r/upload_bundle_data.R"))
source(file.path(central_lib, "current/r/save_bundle_version.R"))
source(file.path(central_lib, "current/r/save_crosswalk_version.R"))

"%ni%" <- Negate("%in%") # create a reverse %in% operator


#--------------------Bundle work------------------------------------------------------------
# Note: for GBD2020, we are not allowed to save_results_epi without a XW version
# We're also not allowed to save XW versions for custom bundles
# This means that I'm saving this data to a new ST-GPR bundle, just so we can use save_results_epi
# (minor arghhh)

# # Save bundle and xw-version to new ST-GPR shape bundle
# bundle <- get_bundle_data(ambient_bundle,decomp="step4",gbd_round_id=6) %>% as.data.table
# 
# # add in required stgpr columns & save
# bundle[,age_start:=NULL]
# bundle[,age_end:=NULL]
# bundle[,age_start:=0]
# bundle[,age_end:=125]
# bundle[,mean:=NULL]
# bundle[,year_id:=(year_start+year_end)/2]
# bundle[,orig_year_start:=year_start]
# bundle[,orig_year_end:=year_end]
# bundle[,year_start:=year_id]
# bundle[,year_end:=year_id]
# bundle[,age_group_id:=22]
# bundle[,val:=1] # just putting this dummy value here because we need it for this stgpr bundle
# bundle[,variance:=1] # just putting this dummy value here because we need it for this stgpr bundle
# bundle[,sample_size:=100] # just putting this dummy value here because we need it for this stgpr bundle
# bundle[,seq:=1:.N]
# bundle[,measure:=NULL]
# bundle[,measure:="continuous"]
# bundle[,location_id:=6] # just putting this dummy location_id here (China) because global location_ids aren't allowed for this stgpr bundle
# bundle[,is_outlier:=NULL]
# bundle[,is_outlier:=0]
# 
# write.xlsx(bundle,bundle.dir,sheetName="extraction")
# 
# # upload bundle & save bundle_version
# upload <- upload_bundle_data(bundle_id=stgpr_bundle,decomp_step="iterative",filepath=bundle.dir,gbd_round_id=7)
# bundle_version <- save_bundle_version(stgpr_bundle,decomp="iterative",gbd_round_id=7)
# 
# # add in required xw columns, save, & upload
# bundle[,crosswalk_parent_seq:=1:.N]
# bundle[,unit_value_as_published:=1] # just putting this dummy value here because we need it for this stgpr bundle
# write.xlsx(bundle,xw.dir,sheetName="extraction")
# xw <- save_crosswalk_version(bundle_version$bundle_version_id,xw.dir)


#--------------------Save results!----------------------------------------------------------
# save CSVs for draws for most-detailed locations, sexes, & age-groups
sex_id <- c(1:2)
age_group_id <- c(2:3,6:20,30,31,32,34,235,238,388,389)
# pred_cols <- paste0("pred_", 1:1000)
pred_cols <- paste0("draw_", 1:500)
save_draws <- function(file){
  data <- read.fst(file) %>% as.data.table
  data[data<0] <- 0 # make sure there are no negative values; if so, just set to 0 (left by previous modeler, using it again for GBD 2023 --nhashmeh)
  data[is.na(POP),POP:=0] # some NA in POP columns, likely due to coast/border issues. Set to 0 for now?
  exp <- sapply(1:ndraws,
                function(draw.number){weighted.mean(data[[pred_cols[draw.number]]],data[['POP']], na.rm = TRUE)})
  # exp <- sapply(1:1000,
  #               function(draw.number){weighted.mean(data[[pred_cols[draw.number]]],data[,'POP'], na.rm = TRUE)})
  names(exp) <- draw_cols
  # names(exp) <- paste0("draw_",0:999)
  exp <- as.data.table(t(exp))
  exp$location_id <- data[1,"location_id"]
  exp$year_id <- data[1,"year_id"]
  exp[,measure_id:=19] # continous?? what does this mean? -nhashmeh
  out_exp <- cbind(age_group_id,exp)
  temp <- out_exp
  out_exp <- rbindlist(list(out_exp,temp),use.names=T)
  out_exp <- cbind(rep(sex_id,each=25),out_exp)
  setnames(out_exp,"V1","sex_id")
  # write.csv(out_exp,paste0(exp.dir, data[1,"location_id"], "_", data[1,"year_id"], ".csv"),row.names=F)
  fwrite(out_exp,paste0(exp.dir, data[1,"location_id"], "_", data[1,"year_id"], ".csv"))
  
}

files <- list.files(grid.dir,full.names=T)
save <- pblapply(files, save_draws, cl=cores.provided)

# Save exposure

save_results_epi(input_dir=exp.dir,
                 input_file_pattern="{location_id}_{year_id}.csv",
                 modelable_entity_id=11231,
                 # description="GBD2020 update w/ new SAT data: version 46",
                 description="GBD2023 update w/ v5 data",
                 measure_id=19,
                 mark_best=TRUE, # ???
                 year_id=years,
                 # gbd_round_id=7,
                 gbd_round_id=9,
                 release_id=16,
                 n_draws=500, # change for final??
                 # decomp_step="iterative", # ???
                 bundle_id=stgpr_bundle, # ???
                 crosswalk_version_id=stgpr_xw) # ???



#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Save results for ozone (exp & PAFs)
#------------------------Setup -------------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "/home/j/"
  h_root <- "~/"
  central_lib <- "/ihme/cc_resources/libraries/"
} else {
  j_root <- "J:/"
  h_root <- "H:/"
  central_lib <- "K:/libraries/"
}

# load packages, install if missing
pacman::p_load(data.table, magrittr, ini, openxlsx, pbapply)

# version for upload
paf.version <- 22

#parameters
years <- c(1990:2022)
location_set_version_id <- 35
decomp <- "iterative"
ages <- c(2:20,30,31,32,235)
sexes <- c(1,2)
desc <- "GBD2020 results with Fisher's Information boost for RR estimates"
date <- "030321"

save_exposure <- F

# needed because we're not allowed to save to epi with a custom bundle (argh)
ozone_bundle <- 3890
stgpr_bundle <- 8219
bundle.dir <- file.path(j_root, "FILEPATH/air_ozone_exp_stgpr_bundle_upload.xlsx")
xw.dir <- file.path(j_root, "FILEPATH/air_ozone_exp_stgpr_xw_upload.xlsx")

#draw directories
paf.dir <- file.path("/FILEPATH/paf", paf.version,"draws/")
exp.dir <- file.path(j_root, "FILEPATH/exp", paf.version, "draws/")
upload.dir <- file.path(j_root, "FILEPATH", paf.version, "upload/")


#----------------------------Functions---------------------------------------------------

source(file.path(central_lib,"current/r/save_results_epi.R"))
source(file.path(central_lib,"current/r/save_results_risk.R"))
source(file.path(central_lib,"current/r/get_population.R"))
source(file.path(central_lib,"current/r/get_location_metadata.R"))
source(file.path(central_lib,"current/r/get_bundle_data.R"))
source(file.path(central_lib,"current/r/upload_bundle_data.R"))
source(file.path(central_lib,"current/r/save_bundle_version.R"))
source(file.path(central_lib,"current/r/save_crosswalk_version.R"))

locs <- get_location_metadata(35)

#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

stgpr_xw <- 20564

#--------------------Save results!----------------------------------------------------------
# save CSVs for draws for most-detailed locations, sexes, & age-groups
sex_id <- c(1:2)
age_group_id <- c(2:3,6:20,30,31,32,34,235,238,388,389)
draw_cols <- paste0("draw_",0:999)

save_draws <- function(file){
  data <- read.csv(file) %>% as.data.table
  data[data<0] <- 0 # make sure there are no negative values; if so, just set to 0
  data[,measure_id:=19]
  out_exp <- cbind(age_group_id,data)
  temp <- out_exp
  out_exp <- rbindlist(list(out_exp,temp),use.names=T)
  out_exp <- cbind(rep(sex_id,each=25),out_exp)
  setnames(out_exp,"V1","sex_id")
  write.csv(out_exp,paste0(upload.dir, "exp_", data[1,location_id], "_", data[1,year_id], ".csv"),row.names=F)
}

files <- list.files(exp.dir,full.names=T)
save <- pblapply(files, save_draws, cl=10)

# Save exposure
if(save_exposure){
  save_results_epi(input_dir=paste0(upload.dir),
                   input_file_pattern=paste0("exp_{location_id}_{year_id}.csv"),
                   year_id=years,
                   modelable_entity_id=8881,
                   description=desc,
                   measure_id=19,
                   mark_best=TRUE,
                   gbd_round_id=7,
                   decomp_step=decomp,
                   bundle_id=stgpr_bundle,
                   crosswalk_version_id = stgpr_xw)
}



# save PAFs
save_results_risk(input_dir = paf.dir,
                  input_file_pattern = "paf_{measure}_{location_id}_{year_id}.csv",
                  year_id = years,
                  modelable_entity_id = 8748,
                  description = desc,
                  decomp_step = decomp,
                  risk_type = "paf",
                  measure_id = "4",
                  mark_best=TRUE)

# PAF scatters
source("/ihme/code/risk/diagnostics/paf_scatter.R")

paf_scatter(rei_id=88,measure_id=4,decomp_step="iterative",
            file_path=paste0(j_root,"FILEPATH/paf_scatter_yll_",paf.version,".pdf"),add_isos=T)

paf_scatter(rei_id=88,measure_id=4,decomp_step="iterative", year_id=1990,
            file_path=paste0(j_root,"FILEPATH/paf_scatter_yll_1990_",paf.version,".pdf"),add_isos=T)


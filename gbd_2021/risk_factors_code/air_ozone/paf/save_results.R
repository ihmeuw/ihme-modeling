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

# load packages, install if missing
pacman::p_load(data.table, magrittr, ini, openxlsx, pbapply)

# version for upload
paf.version <- 22

#parameters
years <- c(1990:2020)
location_set_version_id <- 35
decomp <- "iterative"
ages <- c(2:20,30,31,32,235)
sexes <- c(1,2)
desc <- "DESCRIPTION"
date <- "DATE"

save_exposure <- F

ozone_bundle <- 3890
bundle.dir <- file.path(j_root, "FILEPATH.xlsx")
xw.dir <- file.path(j_root, "FILEPATH.xlsx")

#draw directories
paf.dir <- file.path("FILEPATH", paf.version,"draws/")
exp.dir <- file.path(j_root, "FILEPATH", paf.version, "draws/")
upload.dir <- file.path(j_root, "FILEPATH", paf.version, "upload/")


#----------------------------Functions---------------------------------------------------

source(file.path(central_lib,"FILEPATH/save_results_epi.R"))
source(file.path(central_lib,"FILEPATH/save_results_risk.R"))

locs <- get_location_metadata(35)

#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

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
source("FILEPATH/paf_scatter.R")

paf_scatter(rei_id=88,measure_id=4,decomp_step="iterative",
            file_path=paste0(j_root,"FILEPATH",paf.version,".pdf"),add_isos=T)

paf_scatter(rei_id=88,measure_id=4,decomp_step="iterative", year_id=1990,
            file_path=paste0(j_root,"FILEPATH",paf.version,".pdf"),add_isos=T)



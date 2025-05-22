
#-------------------Header------------------------------------------------
# Purpose: Uploading/editing bundle data for radon exposure
#------------------SET-UP--------------------------------------------------

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

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","openxlsx")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

decomp <- "iterative"
bundle <- 7274

# Directories & Functions -----------------------------------------------

# radon directory
home_dir <- "FILEPATH"
out_path <- file.path(j_root,"FILEPATH",bundle,"FILEPATH/upload_GBD20.xlsx")


# bundle functions
source(file.path(central_lib,"current/r/get_bundle_data.R"))
source(file.path(central_lib,"current/r/validate_input_sheet.R"))
source(file.path(central_lib,"current/r/upload_bundle_data.R"))

# Format & upload bundle data--------------------------------------------

# get old bundle step (final GBD 2019)
dt <- get_bundle_data(bundle,decomp_step="step4",gbd_round_id=6) %>% as.data.table

# replace old Norway subnats with new loc_ids
nrow(dt[location_id%in%c(4911:4919, 4921:4922, 4927:4928)])

dt[location_id%in%4918:4919, location_id:=60133]
dt[location_id%in%4912:4913, location_id:=60135]
dt[location_id%in%4927:4928, location_id:=60137]
dt[location_id%in%4916:4917, location_id:=60134]
dt[location_id%in%4921:4922, location_id:=60132]
dt[location_id%in%c(4911,4915,4914), location_id:=60136]

dt[,seq:=1:.N]


write.xlsx(dt,out_path,sheetName="extraction")

upload_bundle_data(bundle,decomp,out_path,gbd_round_id=7)

stgpr_outpath <- "FILEPATH/stgpr_upload_GBD20.xlsx"
stgpr_bundle <- 8186

# add new ST-GPR bundle requirements
stgpr <- copy(dt)
stgpr[,age_start:=NULL]
stgpr[,age_end:=NULL]
stgpr[,age_start:=0]
stgpr[,age_end:=125]

stgpr[,mean:=NULL]

stgpr[,orig_year_start:=year_start]
stgpr[,orig_year_end:=year_end]
stgpr[,year_id:=year_end] # for ST-GPR shape, year_id, year_start, and year_end must all match
stgpr[,year_start:=year_end]

stgpr[,val:=mean]
stgpr[,variance:=1]

write.xlsx(stgpr,stgpr_outpath,sheetName="extraction")

upload_bundle_data(stgpr_bundle,decomp,stgpr_outpath,gbd_round_id=7)

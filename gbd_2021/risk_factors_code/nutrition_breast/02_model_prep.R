########################################################################################################################
## Project: RF: Suboptimal Breastfeeding
## Purpose: Generate crosswalk versions and crosswalk ABF6-11/ABF12-23 data
########################################################################################################################

##################
##### SET-UP #####
##################
rm(list=ls())
pacman::p_load(ggplot2, data.table, magrittr, dplyr, msm, openxlsx)

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
} else {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH/", user)
  k <- "FILEPATH"
}

## Source Central Functions
setwd(paste0(k,"FILEPATH"))
source("save_bundle_version.R")
source("get_bundle_version.R")
source("save_crosswalk_version.R")
source("get_location_metadata.R")
source("get_elmo_ids.R")
source("upload_bundle_data.R")

## Source General/Custom Functions
source("FILEPATH/merge_on_location_metadata.R")
source("FILEPATH/xwalk_breastfeeding.R") 

## Define Functions
upload_xwalk_version <- function(df, acause, bundle_id, bundle_version){
  # write out to xlsx
  FOLDER <- acause
  BUNDLE <- bundle_id
  path <- paste0("FILEPATH.xlsx")
  write.xlsx(df,path,sheetName="extraction")
  # upload
  save_crosswalk_version(bundle_version,path,
                         description=paste0("Best data decomp step iterative ",format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%Y-%m-%d")))
}


###############
##### RUN #####
###############

## For Non-Exclusive Breastfeeding + ABF611, just need to save bundle versions and upload crosswalk version
for (bid in c(4835, 4838, 4841, 4844, 4847)){
  bvid <- save_bundle_version(bid,gbd_round_id=7,decomp_step='iterative')
  df <- get_bundle_version(bvid$bundle_version_id, fetch="all")
  df[, crosswalk_parent_seq := seq]
  df[, seq:=""]
  df[, unit_value_as_published := 1]
  df$unit_value_as_published <- as.numeric(df$unit_value_as_published)
  df[val==0, val:=.00001] # epi uploader not accepting 0s for some reason, these points are outliered anyway
  if(bid %in% c(4835,4838,4841,4844)) upload_xwalk_version(df, "nutrition_breast_nonexc", bid, bvid$bundle_version_id)
  if(bid == 4847) upload_xwalk_version(df,"nutrition_breast_disc",bid,bvid$bundle_version_id)
}

# For Discontinued Breastfeeding, need to impute missing data where we have ABF 6-11 but not ABF 12-23

# Save bundle version and pull data
abf611_bvid <- save_bundle_version(4847, gbd_round_id=7, decomp_step='iterative')
abf611 <- get_bundle_version(abf611_bvid$bundle_version_id, fetch="all")
abf611[,group:="abf611"]
abf1223_bvid <- save_bundle_version(4850, gbd_round_id=7, decomp_step='iterative')
abf1223 <- get_bundle_version(abf1223_bvid$bundle_version_id, fetch="all")
abf1223[,group:="abf1223"]

# Append data for crosswalk modeling
df <- rbind(abf611,abf1223,fill=TRUE)

# Generate data to be crosswalked - this is rows in ABF611 that do not have corresponding observations in ABF1223
# Identify location/years needing imputation
bf611 <- abf611[,.(location_id,year_id,val)]
setnames(bf611,"val","abf611")
bf1223 <- abf1223[,.(location_id,year_id,val)]
setnames(bf1223,"val","abf1223")
data <- merge(bf611,bf1223,by=c("location_id","year_id"),all=TRUE)
data[,cv_impute := 0]
data[!is.na(abf611)&is.na(abf1223),cv_impute:=1]

# Merge on cv_impute
xwalk_df <- merge(abf611,data[,.(location_id,year_id,abf611,cv_impute)],
                  by.x=c("location_id","year_id","val"),by.y=c("location_id","year_id","abf611"),
                  all.x=T)
xwalk_df <- xwalk_df[cv_impute==1] 

# Run MR-BRT model
data <- copy(df)
data <- prep_data(data)
data <- match_data(data)
save_matches(data)
data <- prep_mrbrt(data)
out <- fit_mrbrt(data,me="abf_imputation",folder="abf_5yr",xwalk_df=xwalk_df)

# Append data to abf1223 data set, upload to xwalk version
out[,seq:=abf1223[1,seq]]
abf1223 <- rbind(abf1223,out,fill=TRUE)
abf1223[, crosswalk_parent_seq := seq]
abf1223[, seq:=""]
abf1223[, unit_value_as_published := 1]
upload_xwalk_version(abf1223,"nutrition_breast_disc",4850,abf1223_bvid$bundle_version_id)

## Upload data to age-specific model and save xwalk version
abf05 <- get_bundle_data(4835,gbd_round_id=7,decomp_step="iterative")
abf05[,age_group_id:=388]
abf611 <- get_bundle_data(4847,gbd_round_id=7,decomp_step="iterative")
abf611[,age_group_id:=389]
abf1223[, `:=` (age_group_id=238, crosswalk_parent_seq=NULL)]

abf <- rbind(abf05,abf611,abf1223,fill=TRUE)
abf[,c("age_start","age_end"):=NULL]
abf[,seq:=""]
abf[,standard_error:=sqrt(variance)]
abf[,`:=` (lower=val-1.96*standard_error, upper=val+1.96*standard_error)]

# clear existing bundle 
clear <- get_bundle_data(8138,"iterative",7)
clear <- clear[,.(seq)]
path <- paste0("FILEPATH/abf_clear.xlsx")
write.xlsx(clear,path,sheetName="extraction")
upload_bundle_data(8138,"iterative",path,gbd_round_id=7)

# Upload new data to bundle
path <- paste0("FILEPATH.xlsx")
write.xlsx(abf,path,sheetName="extraction")
upload_bundle_data(8138,"iterative",path,gbd_round_id=7)

# now save bundle version and xwalk version
bvid <- save_bundle_version(8138,gbd_round_id=7,decomp_step='iterative')
df <- get_bundle_version(bvid$bundle_version_id, fetch="all")
df[, crosswalk_parent_seq := seq]
df[, seq:=""]
df[, unit_value_as_published := 1]
df$unit_value_as_published <- as.numeric(df$unit_value_as_published)
upload_xwalk_version(df,"nutrition_breast",8138,bvid$bundle_version_id)

message("All data prepped, imputed, and uploaded to crosswalk versions!")

## END
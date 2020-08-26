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
  h <- paste0("FILEPATH", user)
  k <- "FILEPATH"
}

## Source Central Functions
setwd(paste0(k,"FILEPATH"))
source("save_bundle_version.R")
source("get_bundle_version.R")
source("save_crosswalk_version.R")
source("get_location_metadata.R")

## Source General/Custom Functions
source("FILEPATH/merge_on_location_metadata.R")
source("FILEPATH/xwalk_breastfeeding.R")

## Define Functions
upload_xwalk_version <- function(df, acause, bundle_id, bundle_version){
  # write out to xlsx
  FOLDER <- acause
  BUNDLE <- bundle_id
  path <- paste0("FILEPATH")
  write.xlsx(df,path,sheetName="extraction")
  # upload
  save_crosswalk_version(bundle_version,path,
                         description=paste0("Best data decomp step 4 ",format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%Y-%m-%d")))
}


###############
##### RUN #####
###############

## For Non-Exclusive Breastfeeding + ABF611, just need to save bundle versions and upload crosswalk version as-is
for (bid in c(4835, 4838, 4841, 4844, 4847)){
  bvid <- save_bundle_version(bid,gbd_round_id=6,decomp_step='step4')
  df <- get_bundle_version(bvid$bundle_version_id, fetch="new")
  df[, crosswalk_parent_seq := seq]
  df[, seq:=""]
  df[, unit_value_as_published := 1]
  df$unit_value_as_published <- as.numeric(df$unit_value_as_published)
  if(bid %in% c(4835,4838,4841,4844)) upload_xwalk_version(df, "nutrition_breast_nonexc", bid, bvid$bundle_version_id)
  if(bid == 4847) upload_xwalk_version(df,"nutrition_breast_disc",bid,bvid$bundle_version_id)
}

# For Discontinued Breastfeeding, need to impute missing data where we have ABF 6-11 but not ABF 12-23

# Save bundle version and pull data
abf611_bvid <- save_bundle_version(4847, gbd_round_id=6, decomp_step='step4')
abf611 <- get_bundle_version(abf611_bvid$bundle_version_id, fetch="all")
abf611[,group:="abf611"]
abf1223_bvid <- save_bundle_version(4850, gbd_round_id=6, decomp_step='step4')
abf1223 <- get_bundle_version(abf1223_bvid$bundle_version_id, fetch="all")
abf1223[,group:="abf1223"]

# Append data for crosswalk modeling
df <- rbind(abf611,abf1223,fill=TRUE)

# Run MR-BRT model
data <- copy(df)
data <- prep_data(data)
data <- match_data(data)
save_matches(data)
data <- prep_mrbrt(data)
fit <- fit_mrbrt(data,me="abf_imputation",folder="abf_5yr")

# Identify location/years needing imputation
bf611 <- abf611[,.(location_id,year_id,val)]
setnames(bf611,"val","abf611")
bf1223 <- abf1223[,.(location_id,year_id,val)]
setnames(bf1223,"val","abf1223")
data <- merge(bf611,bf1223,by=c("location_id","year_id"),all=TRUE)
data[,cv_impute := 0]
data[!is.na(abf611)&is.na(abf1223),cv_impute:=1]

# Merge on cv_impute
abf611$cv_impute <- NULL
abf611 <- merge(abf611,data[,.(location_id,year_id,abf611,cv_impute)],
                by.x=c("location_id","year_id","val"),by.y=c("location_id","year_id","abf611"),
                all.x=T)

# Append data to impute ABF1223
abf1223 <- rbind(abf1223,abf611[cv_impute==1],fill=TRUE)

# Read in betas
preds <- fread("FILEPATH/model_summaries.csv")
beta0 <- preds$Y_mean
beta0_se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 3.92

# Apply crosswalk for ABF1223
abf1223[cv_impute==1, orig.val:=val]
abf1223[cv_impute==1, val := inv.logit(logit(val) - beta0)]
abf1223[cv_impute==1, variance := logit(variance) + logit(beta0_se)^2]
abf1223[cv_impute==1]$standard_error <- sapply(1:nrow(abf1223[cv_impute==1]), function(i){
  mean_i <- abf1223[cv_impute==1][i,logit(val)]
  se_i <- abf1223[cv_impute==1][i, variance]
  deltamethod(~exp(x1)/(1+exp(x1)), mean_i, se_i)
})
abf1223[cv_impute==1, variance := standard_error^2]
abf1223 <- abf1223[!(cv_impute==1 & val==1)] 
abf1223[,crosswalk_parent_seq := seq]
abf1223[,seq := ""]

# Upload Crosswalk Version ABF1223 (only new data and the imputed data)
df <- get_bundle_version(abf1223_bvid$bundle_version_id, fetch="new")
abf1223 <- abf1223[cv_impute==1]
abf1223 <- rbind(df,abf1223,fill=TRUE)
abf1223[, crosswalk_parent_seq := seq]
abf1223[, seq:=""]
abf1223[, unit_value_as_published := 1]
upload_xwalk_version(abf1223,"nutrition_breast_disc",4850,abf1223_bvid$bundle_version_id)

message("All data prepped, imputed, and uploaded to crosswalk versions!")


## END
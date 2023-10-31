### Sepsis MR-BRT Outliering

## Set-up
rm(list=ls())
os <- .Platform$OS.type
if (os=="Windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
}

## Source Functions
pacman::p_load(ggplot2, data.table, magrittr, dplyr, splitstackshape, msm, openxlsx, tidyr, matrixStats)
setwd("FILEPATH")
source("get_crosswalk_version.R")
source("get_model_results.R")
source("get_covariate_estimates.R")
source("get_location_metadata.R")
library(crosswalk, lib.loc = "FILEPATH")
locs <- get_location_metadata(22,gbd_round_id=7)

## Read in Data

# Sepsis Incidence Data
df <- get_crosswalk_version(34730)[,.(seq,nid,location_id,year_id,age_group_id,sex,val,variance)]
df[,standard_error := sqrt(variance)] 
df[,sex_id := ifelse(sex=="Male",1,2)]

# HAQI
haqi <- get_covariate_estimates(1099,gbd_round_id=7,decomp_step="iterative")[,.(location_id,year_id,mean_value)]
setnames(haqi,"mean_value","haqi")

## Format

# Merge data
df <- merge(df,haqi,by=c("location_id","year_id"))
df[val==0,val:=.00001]
df[,altvar:="alternate"]
df[,refvar:="reference"]
df[,enn:=ifelse(age_group_id==2,1,0)]
df <- df[!nid==104241] 

# Prep for MR-BRT
transform <- delta_transform(df$val, df$standard_error,"linear_to_log")
df <- data.table(df,transform)
setnames(df,c("mean_log","sd_log"),c("val_log","sd_val_log"))
df[,study_id:=paste0(nid,"_",location_id)]

## MR-BRT Models

# Set-up
cw_data <- CWData(
  df = df,          # dataset for metaregression
  obs = "val_log",       # column name for the observation mean
  obs_se = "sd_val_log", # column name for the observation standard error
  alt_dorms = "altvar",     # column name of the variable indicating the alternative method
  ref_dorms = "refvar",     # column name of the variable indicating the reference method
  covs = list("haqi","enn"),     # names of columns to be used as covariates later
  study_id = "study_id",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE, # adds a column called "intercept" that may be used in CWModel()
  data_id = "seq"
)

# Incidence w/ HAQI cubic + 1 internal knot + 30% trim
fit_cub30 <- CWModel(
  cwdata = cw_data,            
  obs_type = "diff_log", 
  cov_models = list(       
    CovModel(cov_name = "intercept"),
    CovModel(cov_name = "enn"),
    CovModel(cov_name = "haqi",
             spline=XSpline(knots=c(20,55,100),
                            degree=3L,
                            l_linear=TRUE),
             spline_monotonicity="decreasing")),
  inlier_pct=0.7,
  gold_dorm="reference"
)

## Merge outliering onto data set
df_pred <- df[,.(location_id,year_id,age_group_id,sex_id,seq,nid,sex,val,standard_error,haqi)]
df_pred[,w_cub30 := fit_cub30$w]
df_pred <- merge(df_pred,locs[,.(ihme_loc_id,location_name,location_id,super_region_name)],by="location_id",all.x=TRUE)
df_pred[,age_group_name:=ifelse(age_group_id==2,"ENN","LNN")]
df_pred[,sex:=ifelse(sex_id==1,"Males","Females")]

# Set outliers
df_pred[,is_outlier := ifelse(w_cub30>=0.5,0,1)]
df_pred[,shape:=ifelse(is_outlier==0,"In","Out")] # for plotting


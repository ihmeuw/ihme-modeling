
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 9/24/19
# Purpose: Crosswalk HAP data before STGPR
#          
# source("FILEPATH.R", echo=T)
#***************************************************************************

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  } else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  }


# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

#------------------FUNCTIONS AND DIRECTORIES-------------------------------

#functions
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))

source(file.path(central_lib,"FILEPATH.R"))
locs <- get_location_metadata(35)

decomp <- "step4"
bundle_id <- 4736 
description <- ""

home_dir <- "FILEPATH"
xwalk_dir <- file.path(home_dir,"FILEPATH")


bundle_version <- save_bundle_version(bundle_id,decomp)

out <- get_bundle_version(bundle_version$bundle_version_id)

xw_log <- fread(file.path(xwalk_dir,"FILEPATH.csv"))

# MR-BRT crosswalk --------------------------------------------------------

# only use data that is not outliered, is at individual level, also has HH level data and doesn't have a mean of zero or 1
data_metareg <- out[is_outlier==0 & cv_hh==0 & !is.na(mean_household) & val !=0 & val !=1]

# logit transformations
out[,mean_logit := log(val/(1-val))]
out[,se_logit := deltamethod(~log(x1/(1-x1)),val,standard_error^2), by=1:nrow(out)]

data_metareg[,mean_logit := log(val/(1-val))]
data_metareg[,se_logit := deltamethod(~log(x1/(1-x1)),val,standard_error^2), by=1:nrow(data_metareg)]

data_metareg[,mean_logit_hh := log(mean_household/(1-mean_household))]
data_metareg[,se_logit_hh := deltamethod(~log(x1/(1-x1)),mean_household,standard_error_household^2), by=1:nrow(data_metareg)]

data_metareg[,diff_logit := mean_logit_hh - mean_logit ]
data_metareg[,se_diff_logit := sqrt(se_logit_hh^2 + se_logit^2)]


# fit the MR-BRT model
source("FILEPATH.R")

fit1 <- run_mr_brt(
  output_dir = xwalk_dir, 
  model_label = "hh_size_xwalk",
  data = data_metareg,
  mean_var = "diff_logit",
  se_var = "se_diff_logit",
  trim_pct = 0.1,
  method = "trim_maxl",
  overwrite_previous = TRUE,
  max_iter = 1000
)

plot_mr_brt(fit1)

preds <- predict_mr_brt(fit1,newdata = out)$model_summaries

out[,adj_logit:=preds$Y_mean]
out[,se_adj_logit:=(preds$Y_mean_hi - preds$Y_mean_lo) / 3.92]

out[cv_hh==1,mean_logit_household_adjusted:=mean_logit-adj_logit]
out[cv_hh==1,se_logit_household_adjusted:= sqrt(se_logit^2 + se_adj_logit^2)]

out[,mean_household_adjusted:=exp(mean_logit_household_adjusted)/(1+exp(mean_logit_household_adjusted))]
out[,se_household_adjusted:=deltamethod(~exp(x1)/(1+exp(x1)),mean_logit_household_adjusted,se_logit_household_adjusted^2), by=1:nrow(out)]

# for zeros and ones, copy over
out[cv_hh==1 & val %in% c(1,0),c("mean_household_adjusted","se_household_adjusted"):=.(val,standard_error)]

ggplot(out[cv_hh==1],aes(x=val,y=mean_household_adjusted))+geom_point()+geom_abline(slope=1,intercept=0)
ggplot(out[cv_hh==1],aes(x=standard_error,y=se_household_adjusted,color=val))+geom_point()

# Replace hh data with crosswalked values

out[cv_hh==1,c("val","variance","standard_error"):=.(mean_household_adjusted,NA,se_household_adjusted)]
out[,variance:=standard_error^2]

out <- out[,setdiff(names(out),c("mean_logit","se_logit","adj_logit","se_adj_logit","mean_logit_household_adjusted","se_logit_household_adjusted","mean_household_adjusted","se_househlold_adjusted","unit_value_as_published")),with=F]
out[,crosswalk_parent_seq:=seq]
out[,unit_value_as_published:=1]


write.xlsx(out,paste0(xwalk_dir,"/upload_bv_",bundle_version$bundle_version_id,".xlsx"),sheetName="extraction",row.names=F)

xw <- save_crosswalk_version(bundle_version_id = bundle_version$bundle_version_id, description=description, data_filepath = paste0(xwalk_dir,"/upload_bv_",bundle_version$bundle_version_id,".xlsx"))

xw[,bundle_version_id:=bundle_version$bundle_version_id]

xw_log <- rbind(xw_log,xw,use.names=T,fill=T)

write.csv(xw_log,file.path(xwalk_dir,"xwalk_version_metadata.csv"),row.names=F)
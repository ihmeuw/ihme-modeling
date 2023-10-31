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

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr", "msm", "ggplot2","openxlsx")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

library(crosswalk, lib.loc = "FILEPATH")

#functions
source(file.path(central_lib,"FILEPATH/get_bundle_data.R"))
source(file.path(central_lib,"FILEPATH/save_bundle_version.R"))
source(file.path(central_lib,"FILEPATH/save_crosswalk_version.R"))
source(file.path(central_lib,"FILEPATH/get_crosswalk_version.R"))
source(file.path(central_lib,"FILEPATH/get_bundle_version.R"))

source(file.path(central_lib,"FILEPATH/get_location_metadata.R"))
locs <- get_location_metadata(35)

"%ni%" <- Negate("%in%")

decomp <- "iterative"
hap_bundle_id <- 4736
clean_bundle_id <- 6164
coal_bundle_id <- 6167
crop_bundle_id <- 6173
dung_bundle_id <- 6176
wood_bundle_id <- 6170

home_dir <- "FILEPATH"
xwalk_dir <- file.path(home_dir,"FILEPATH")

description <- "DESCRIPTION"


#------------------Save bundle versions------------------------------------

# Use this section for when you only want to save a new bundle/crosswalk version for the overall solid category
bundle_version_1 <- save_bundle_version(hap_bundle_id,decomp,gbd_round_id=7,include_clinical="None")

fuel_type <- "solid"
bundle_out <- bundle_version_1
bundle_out <- cbind(bundle_out, fuel_type)
bundle_out[,date:=as.character(Sys.Date())]
bundle_versions <- bundle_out[,bundle_version_id]

# Only need to do this once for fuel types (since we do not crosswalk this data)
# Comment out after you do this the first time for updated data
# bundle_version_1 <- save_bundle_version(hap_bundle_id,decomp,gbd_round_id=7,include_clinical="None")
# bundle_version_2 <- save_bundle_version(coal_bundle_id,decomp,gbd_round_id=7,include_clinical="None")
# bundle_version_3 <- save_bundle_version(crop_bundle_id,decomp,gbd_round_id=7,include_clinical="None")
# bundle_version_4 <- save_bundle_version(dung_bundle_id,decomp,gbd_round_id=7,include_clinical="None")
# bundle_version_5 <- save_bundle_version(wood_bundle_id,decomp,gbd_round_id=7,include_clinical="None")
# 
# fuel_type <- c("solid","coal","crop","dung","wood")
# bundle_out <- rbind(bundle_version_1,bundle_version_2,bundle_version_3,
#                     bundle_version_4,bundle_version_5)
# bundle_out <- cbind(bundle_out, fuel_type)
# bundle_out[,date:=as.character(Sys.Date())]
# bundle_versions <- bundle_out[,bundle_version_id]

temp <- read.csv(paste0(xwalk_dir,"/bundle_version_metadata.csv")) %>% as.data.table
temp[,date:=as.character(date)]
bundle_out <- rbind(temp,bundle_out,use.names=T,fill=T)
write.csv(bundle_out,paste0(xwalk_dir,"/bundle_version_metadata.csv"),row.names=F)

out <- data.table()
for (n in 1:length(bundle_versions)){
  temp <- get_bundle_version(bundle_versions[n], fetch="all") %>% as.data.table
  temp <- temp[,fuel_type:=fuel_type[n]]
  out <- rbind(out,temp,use.names=T,fill=T)
}

out[,"Unnamed: 0":=NULL]

xw_log <- fread(file.path(xwalk_dir,"xwalk_version_metadata.csv")) %>% as.data.table
xw_log[,date:=as.character(date)]

# MR-BRT crosswalk --------------------------------------------------------
# format data for metaregression
out[,altvar:="household"]
out[,refvar:="individual"]

# add in survey method column
out[,survey_method:=ifelse(cv_hh==1,"household","individual")]

# only use data that is not outliered, is at individual level, also has HH level data and doesn't have a mean of zero or 1
data_metareg <- out[is_outlier==0 & cv_hh==0 & !is.na(mean_household) & val !=0 & val !=1]

# transform means and SDs to logit space
ref_logit <- delta_transform(mean = data_metareg$val, sd = data_metareg$standard_error, transformation = "linear_to_logit") %>% as.data.table
alt_logit <- delta_transform(mean = data_metareg$mean_household, sd = data_metareg$standard_error_household, transformation = "linear_to_logit") %>% as.data.table

data_metareg[,mean_ref_logit:=ref_logit$mean_logit]
data_metareg[,se_ref_logit:=ref_logit$sd_logit]
data_metareg[,mean_alt_logit:=alt_logit$mean_logit]
data_metareg[,se_alt_logit:=alt_logit$sd_logit]

# calculate diff in logit space
data_metareg[,c("logit_diff","logit_diff_se"):=calculate_diff(data_metareg,
                                                              alt_mean="mean_alt_logit",
                                                              alt_sd="se_alt_logit",
                                                              ref_mean="mean_ref_logit",
                                                              ref_sd="se_ref_logit")]


# fit fuel-specific MR-BRT model using MR-BRT functions
source("FILEPATH/mr_brt_functions.R")

fit1 <- run_mr_brt(
  output_dir = xwalk_dir,
  model_label = "hap_xwalk_solid_DATE",
  data = data_metareg[fuel_type=="solid"],
  mean_var = "logit_diff",
  se_var = "logit_diff_se",
  trim_pct = 0.1,
  method = "trim_maxl",
  overwrite_previous = TRUE,
  max_iter = 1000
)

plot_mr_brt(fit1)

# predict MR-BRT parameters
preds1 <- predict_mr_brt(fit1,newdata = out[fuel_type=="solid"])$model_summaries

# adjust household values
out[,mean_logit := log(val/(1-val))]
out[,se_logit := deltamethod(~log(x1/(1-x1)),val,standard_error^2), by=1:nrow(out)]

out[fuel_type=="solid",adj_logit:=preds1$Y_mean]
out[fuel_type=="solid",se_adj_logit:=(preds1$Y_mean_hi - preds1$Y_mean_lo) / 3.92]

out[fuel_type=="solid" & cv_hh==1,mean_logit_household_adjusted:=mean_logit-adj_logit]
out[fuel_type=="solid" & cv_hh==1,se_logit_household_adjusted:= sqrt(se_logit^2 + se_adj_logit^2)]

out[,mean_household_adjusted:=exp(mean_logit_household_adjusted)/(1+exp(mean_logit_household_adjusted))]
out[,se_household_adjusted:=deltamethod(~exp(x1)/(1+exp(x1)),mean_logit_household_adjusted,se_logit_household_adjusted^2), by=1:nrow(out)]

# for zeros and ones, copy over
out[fuel_type=="solid" & cv_hh==1 & val %in% c(1,0),c("mean_household_adjusted","se_household_adjusted"):=.(val,standard_error)]

# make plots to vet
pdf(paste0(xwalk_dir, "FILEPATH", Sys.Date(), ".pdf"), width=11, height=8.5)
ggplot(out[fuel_type=="solid" & cv_hh==1],aes(x=val,y=mean_household_adjusted,color=fuel_type))+geom_point()+geom_abline(slope=1,intercept=0)
ggplot(out[fuel_type=="solid" & cv_hh==1],aes(x=standard_error,y=se_household_adjusted,color=val))+geom_point()
dev.off()

# Replace hh data with crosswalked values

out[fuel_type=="solid" & cv_hh==1,c("val","variance","standard_error"):=.(mean_household_adjusted,NA,se_household_adjusted)]
out[,variance:=standard_error^2]

out <- out[,setdiff(names(out),c("mean_logit","se_logit","adj_logit","se_adj_logit","mean_logit_household_adjusted","se_logit_household_adjusted","mean_household_adjusted","se_househlold_adjusted","unit_value_as_published")),with=F]
out[,crosswalk_parent_seq:=seq]
out[,unit_value_as_published:=1]
out[,age_group_id:=22]


# Split bundles into fuel types -------------------------------------------

write.xlsx(out[fuel_type=="solid"],paste0(xwalk_dir,"FILEPATH",bundle_versions[1],".xlsx"),sheetName="extraction",row.names=F)
# Uncomment if you're saving fuel types too
# write.xlsx(out[fuel_type=="coal"],paste0(xwalk_dir,"FILEPATH",bundle_versions[2],".xlsx"),sheetName="extraction",row.names=F)
# write.xlsx(out[fuel_type=="crop"],paste0(xwalk_dir,"FILEPATH",bundle_versions[3],".xlsx"),sheetName="extraction",row.names=F)
# write.xlsx(out[fuel_type=="dung"],paste0(xwalk_dir,"FILEPATH",bundle_versions[4],".xlsx"),sheetName="extraction",row.names=F)
# write.xlsx(out[fuel_type=="wood"],paste0(xwalk_dir,"FILEPATH",bundle_versions[5],".xlsx"),sheetName="extraction",row.names=F)


xw1 <- save_crosswalk_version(bundle_version_id = bundle_versions[1], description = description, data_filepath = paste0(xwalk_dir,"FILEPATH",bundle_versions[1],".xlsx"))
# Uncomment if you're saving fuel types too
# xw2 <- save_crosswalk_version(bundle_version_id = bundle_versions[2], description = description, data_filepath = paste0(xwalk_dir,"FILEPATH",bundle_versions[2],".xlsx"))
# xw3 <- save_crosswalk_version(bundle_version_id = bundle_versions[3], description = description, data_filepath = paste0(xwalk_dir,"FILEPATH",bundle_versions[3],".xlsx"))
# xw4 <- save_crosswalk_version(bundle_version_id = bundle_versions[4], description = description, data_filepath = paste0(xwalk_dir,"FILEPATH",bundle_versions[4],".xlsx"))
# xw5 <- save_crosswalk_version(bundle_version_id = bundle_versions[5], description = description, data_filepath = paste0(xwalk_dir,"FILEPATH",bundle_versions[5],".xlsx"))
# xw <- rbind(xw1,xw2,xw3,xw4,xw5)

xw[,bundle_version_id:=bundle_versions]
xw[,fuel_type:=fuel_type]
xw[,date:=as.character(Sys.Date())]

xw_log <- rbind(xw_log,xw,use.names=T,fill=T)

write.csv(xw_log,file.path(xwalk_dir,"xwalk_version_metadata.csv"),row.names=F)





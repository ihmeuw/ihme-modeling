rm(list = ls())
set.seed(7)

## Boilerplate
library(metafor)
library(msm)
library(plyr)
library(dplyr)
library(boot)
library(ggplot2)
library(openxlsx)
library(readxl)
library(reticulate)

save_crosswalks <- TRUE

# Import crosswalk functions and others
reticulate::use_python("/FILEPATH/python")
cw <- import("crosswalk")

source("/FILEPATH/bundle_crosswalk_collapse.R")
invisible(sapply(list.files("/FILEPATH/", full.names = T), source))

`%notin%` <- Negate(`%in%`)

locs <- as.data.table(get_location_metadata(location_set_id = 35, release_id=16))
gbd_round_id <- 16
gbd_round <- "gbd2023"
netid <- ""
date <- format(Sys.Date(), format="%m_%d_%Y")
date <- paste0(date, "_claims_fix")

# Set up file structure
modeling_dir <- paste0("/FILEPATH/", gbd_round, "/", date, "/")
folder_names <- c("xwalk_scalars", "matched_pairs", "step-by-step_adjustments", "age_splitting")
for (n in folder_names) {
  dir.create(paste0(modeling_dir, n), recursive=T)
}

##----------------------------------------------------------------------------
## Bundle pull, initial formatting
##----------------------------------------------------------------------------
gbd21_xwalk <- 41013
bundle_id <- 3
bvid <- 48094
bv <- as.data.table(get_bundle_version(bundle_version_id = bvid))
bv <- bv[year_start >= 1985,]
bv[is.na(group_review), group_review := 1]
bv <- bv[group_review != 0]
bv$crosswalk_parent_seq <- ifelse(!is.na(bv$seq),bv$seq, b$seq_parent)

dutch_nid <- 552339
bv[nid==dutch_nid, cv_inpatient := 0]


################################################################
## RECODE NEW DATA, CLINICAL DATA, and GBD ROUND
################################################################
# clinical recode
bv <- bv[clinical_data_type == "claims, inpatient only", clinical_data_type := "claims"]
bv <- bv[clinical_data_type == "inpatient", cv_inpatient := 1]
bv <- bv[clinical_data_type == "claims", cv_marketscan := 1]

maybe_claims <- bv[field_citation_value %like% "Claims"]
unique(maybe_claims$field_citation_value)
bv[field_citation_value %like% "Claims", cv_marketscan := 1]
bv[field_citation_value %like% "Claims", clinical_data_type := "claims"]

write.csv(bv, paste0(modeling_dir, "bv_",bvid,"_initial_formatting.csv"), row.names=F)

################################################################
## CLINICAL crosswalk
################################################################
all_data <- copy(bv)
emr <- all_data[measure=="mtexcess"]
all_data <- all_data[measure!="mtexcess"]

non_clin_dt <- all_data[clinical_data_type =="",]
non_clin_dt[is.na(cv_inpatient), cv_inpatient:=0] 
non_clin_dt$mean_original <- non_clin_dt$mean

# Non-clinical: Calculate cases and sample size (missing in some data)
sample_size <- with(non_clin_dt, mean*(1-mean)/standard_error^2)
cases <- non_clin_dt$mean * sample_size
non_clin_dt$cases <- as.numeric(ifelse(is.na(non_clin_dt$cases), cases, non_clin_dt$cases))
non_clin_dt$sample_size <- as.numeric(ifelse(is.na(non_clin_dt$sample_size), sample_size, non_clin_dt$sample_size))

# Clinical processing
clin_df <- subset(all_data, clinical_data_type != "")
clin_df$age_end <- ifelse(clin_df$age_end > 99, 99, clin_df$age_end)
clin_df$age_mid <- floor((as.numeric(clin_df$age_start) + clin_df$age_end)/2)
sample_size <- with(clin_df, mean*(1-mean)/standard_error^2)
cases <- with(clin_df, mean * sample_size)
clin_df$cases <- ifelse(is.na(clin_df$cases), cases, clin_df$cases)
clin_df$sample_size <- ifelse(is.na(clin_df$sample_size), sample_size, clin_df$sample_size)
clin_df$lower <- ""
clin_df$upper <- ""
clin_df$cv_inpatient <- ifelse(clin_df$clinical_data_type=="inpatient",1,0) # we do not want to include claims in the inpatient crosswalk because we have a separate claims crosswalk
clin_df$cv_marketscan <- ifelse(clin_df$clinical_data_type=="claims",1,0)
clin_df$mean_original <- clin_df$mean

diarrhea <- rbind.fill(non_clin_dt, clin_df) 
diarrhea <- subset(diarrhea, nid!=440156) 
diarrhea$linear_floor <- median(diarrhea$mean[diarrhea$mean>0]) * 0.01
diarrhea$mean <- ifelse(diarrhea$mean<=0, diarrhea$linear_floor, diarrhea$mean)

## Convert incidence to prevalence
inc <- subset(diarrhea, measure=="incidence")
duration <- 4.2/365
duration_lower <- 4.2/365
duration_upper <- 4.4/365
duration_se <- (duration_upper - duration_lower) / (2*1.96)
inc$prev <- inc$mean * duration
inc$standard_error <- inc$prev*sqrt((inc$standard_error/inc$mean)^2+(duration_se/duration)^2) 
inc$mean <- inc$prev
inc <- subset(inc, select = -c(prev))
inc$measure <- "prevalence"
inc$cases <- inc$mean * inc$sample_size # also convert cases after the prevalence conversion

diarrhea <- rbind(subset(diarrhea, measure != "incidence"), inc) 

##-----------------------------------------------------------------------------
diarrhea_full <- diarrhea
diarrhea_full <- subset(diarrhea_full, select=-c(parent_id))
diarrhea_full$year_id <- floor((diarrhea_full$year_start + diarrhea_full$year_end) / 2)

diarrhea_full <- merge(diarrhea_full, locs[,c("location_id","parent_id")], by="location_id")
diarrhea_full$location_id <- ifelse(diarrhea_full$parent_id == 4749, 4749, diarrhea_full$location_id)

## Age bins
age_bins <- c(0,1,2,5,10,20,40,60,80,100)
#age_bins <- c(0,1,seq(5,100,5))

## Subset to working data frame ##
df <- diarrhea_full[,c("location_name","location_id","source_type","nid","age_start","sex","age_end","year_start","year_end",
                       "mean","standard_error","cases","sample_size", "measure", "source_type", "representative_name", 
                       "urbanicity_type", "recall_type", "recall_type_value",
                       "cv_diag_selfreport","cv_hospital","cv_inpatient","cv_clin_data","is_reference",
                       "is_outlier","group","specificity","group_review","cv_marketscan", "gbd_round","gbd_2019_new", "seq",
                       "crosswalk_parent_seq")]

df <- join(df, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")


## Recode necessary variables that should be numeric for computational purposes later (make sure they aren't characters)
df <- as.data.table(df)
df[is.na(df)] <- ""
df$mean<-as.numeric(df$mean)
df$sample_size<-as.numeric(df$sample_size)
df$cases <- as.numeric(df$cases)
df$age_start<-as.numeric(df$age_start)
df$age_end<-as.numeric(df$age_end)
df <- as.data.frame(df)
df$rownum <- 1:nrow(df)

### Save a step-by-step breakdown of the adjustment process for each row
### This file will be updated each time an adjustment is made
meta_cols <- c("seq","nid","location_id","location_name","year_start","year_end","age_start","age_end","sex","group_review",
               "cv_inpatient","cv_marketscan")
df_summary <- data.frame(df)[,meta_cols]
df_summary$cases <- df$cases
df_summary$sample_size <- df$sample_size
df_summary$raw_mean <- df$mean

adj_path <- paste0("/FILEPATH/",gbd_round,"/",date,"/step-by-step_adjustments/")
if (!dir.exists(adj_path)) {dir.create(adj_path, recursive=TRUE)}
write.csv(df_summary,paste0(adj_path, "step-by-step_adjustments_bundle3_",date,".csv"),row.names=F)

######################################################################
#
# Create matched pairs for crosswalks
# 
######################################################################
# Inpatient matched pairs 
df$inpatient <- ifelse(df$cv_inpatient==1,1,0)
df$inpatient[is.na(df$inpatient)] <- 0
df$is_reference <- ifelse(df$inpatient==1,0,1)
df$is_reference[df$cv_marketscan==1] <- 0 # we do not want claims data as reference rows
df$cv_diag_pcr <- 0 

cv_inpatient <- bundle_crosswalk_collapse(subset(df, nid != dutch_nid),  
                                          reference_name="is_reference", 
                                          covariate_name="inpatient", 
                                          age_cut=age_bins, 
                                          year_cut=c(seq(1980,2015,5),2020), 
                                          merge_type="between", 
                                          location_match="exact", 
                                          include_logit = T,
                                          release_id=16)
# Remove an nid that is not community based from ref_nid (non-inpatient population is outpatients)
cv_inpatient <- subset(cv_inpatient, nid != 221077)
dir.create(paste0(modeling_dir, "/matched_pairs/"))
write.csv(cv_inpatient, paste0(modeling_dir, "/matched_pairs/inpatient_matched_pairs_bundle3_",date,".csv"), row.names=F)

# Claims matched pairs 
df$claims <- ifelse(df$cv_marketscan==1 & df$year_start != 2000,1,0) 
df$claims[is.na(df$claims)] <- 0
df$is_reference <- ifelse(df$claims==1,0,1)
df$is_reference[df$cv_marketscan == 1 & df$year_start == 2000] <- 0 
df$is_reference[df$inpatient == 1] <- 0 
cv_marketscan <- bundle_crosswalk_collapse(subset(df, nid != dutch_nid),  
                                           reference_name="is_reference", 
                                           covariate_name="claims", 
                                           age_cut=age_bins, 
                                           year_cut=c(seq(1980,2015,5),2020), 
                                           merge_type="between", 
                                           location_match="exact", 
                                           include_logit = T,
                                           release_id=16)
write.csv(cv_marketscan, paste0("/FILEPATH/",gbd_round,"/",date,"/matched_pairs/claims_matched_pairs_bundle3_",date,".csv"), row.names=F)


######################################################################
## Clinical data crosswalk. This is for data from the clinical
## informatics team that produces estimates of the incidence of diarrhea.
######################################################################
df$obs_method <- ""
df[which(df$cv_inpatient==1),"obs_method"] <- "inpatient"
df[which(df$cv_inpatient!=1),"obs_method"] <- "non-inpatient"

cv_inpatient$year_id <- floor((cv_inpatient$year_end + cv_inpatient$year_start)/2)
cv_inpatient$age_mid <- floor((cv_inpatient$age_end + cv_inpatient$age_start)/2)
cv_inpatient$study_id <- paste0(cv_inpatient$location_match,"_",cv_inpatient$age_bin)

##########################################################################
############################## Inpatient! ################################
cv_inpatient$match_id <- paste0(cv_inpatient$ref_nid,"_",cv_inpatient$alt_nid)
cv_inpatient$age_u5 <- ifelse(cv_inpatient$age_end <=5, 1, 0)

cv_inpatient$altvar <- "inpatient"
cv_inpatient$refvar <- "non-inpatient"

df_inpatient <- cw$CWData(
  df = cv_inpatient,          # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "altvar",     # column name of the variable indicating the alternative method
  ref_dorms = "refvar",     # column name of the variable indicating the reference method
  study_id = "match_id",  # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

fit_inpatient <- cw$CWModel(
  cwdata = df_inpatient,            # object returned by `CWData()`
  obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specifying predictors in the model; see help(cw$CovModel)
    cw$CovModel(cov_name = "intercept")),
  gold_dorm = "non-inpatient"   # the level of `alt_dorms` that indicates it's the gold standard
)

fit_inpatient$fit(
  inlier_pct=0.9 # trim 10%
)

#saving crosswalk for future reference
#can load again using py_load_object
if(save_crosswalks){
  output_dir <- paste0("/FILEPATH/", gbd_round, "/", "/cv_inpatient_pkl/")
  
  if(!dir.exists(output_dir)){
    dir.create(output_dir,recursive=TRUE)
  }
  
  #generate crosswalk id# and date, as unique crosswalk identifier
  fps <- dir(output_dir)
  if(length(fps)==0){
    suffix <- paste0("0_",Sys.info()["user"],"_",Sys.Date())
  } else{
    ids <- sapply(fps,function(fp){
      id <- strsplit(fp,split="cv_inpatient_") %>% unlist
      id <- id[2] %>% strsplit(split=".RData") %>% unlist
      id <- id %>% strsplit(split="_") %>% unlist
      return(as.numeric(id[1]))
    })
    if(all(is.na(ids))){
      suffix <- paste0("cv_inpatient_",0,"_",Sys.info()["user"],"_",Sys.Date())
    } else{
      suffix <- paste0("cv_inpatient_",max(ids,na.rm=T)+1,"_",Sys.info()["user"],"_",Sys.Date())
    }
  }
  py_save_object(object = fit_inpatient, filename = paste0(output_dir, suffix,".pkl"), pickle = "dill") 
  df_result <- fit_inpatient$create_result_df()
  dir.create(paste0(modeling_dir, "/xwalk_scalars/"))
  write.csv(df_result, paste0(modeling_dir, "/xwalk_scalars/", 
                              "inpatient_crosswalk_coefficient_", 
                              date, ".csv"), row.names=F)
}

preds_inpatient <- fit_inpatient$adjust_orig_vals( 
  df = df,
  orig_dorms = "obs_method",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error",
  data_id = "rownum"   # optional argument to add a user-defined ID to the predictions; name of the column with the IDs
)

df$mean <- preds_inpatient$ref_vals_mean
df$standard_error <- preds_inpatient$ref_vals_sd

### Adding adjustment to summary file
df_summary <- read.csv(paste0(adj_path, "step-by-step_adjustments_bundle3_",date,".csv"))
df_summary$group_review <- ifelse(is.na(df_summary$group_review),"",df_summary$group_review)
df_summary$group_review[is.na(df_summary$group_review)] <- 1
df_summary <- subset(df_summary, group_review != 0)
df_summary <- subset(df_summary, seq %in% df$crosswalk_parent_seq)
setnames(df_summary,"seq","crosswalk_parent_seq")
df_summary <- right_join(df_summary %>% select(-sex),df[,c("mean","crosswalk_parent_seq","age_start","age_end","group_review","sex")],by=c("crosswalk_parent_seq","age_start","age_end","group_review"))
setnames(df_summary,c("mean"),c("inpatient_adj"))
df_summary$inp_beta <- fit_inpatient$beta[1]
write.csv(df_summary,paste0(adj_path, "step-by-step_adjustments_bundle3_",date,".csv"),row.names=F)

##########################################################################
######################## Claims! ################################
cv_marketscan$year_id <- floor((cv_marketscan$year_end + cv_marketscan$year_start)/2)
cv_marketscan$location_id <- cv_marketscan$location_match
cv_marketscan$age_mid <- floor((cv_marketscan$age_end + cv_marketscan$age_start)/2)
cv_marketscan$match_id <- paste0(cv_marketscan$ref_nid,"_",cv_marketscan$alt_nid)

cv_marketscan$altvar <- "marketscan"
cv_marketscan$refvar <- "non-marketscan"

df_marketscan <- cw$CWData(
  df = cv_marketscan,          # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "altvar",     # column name of the variable indicating the alternative method
  ref_dorms = "refvar",     # column name of the variable indicating the reference method
  study_id = "match_id",  # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

fit_marketscan <- cw$CWModel(
  cwdata = df_marketscan,            # object returned by `CWData()`
  obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specifying predictors in the model; see help(cw$CovModel)
    cw$CovModel(cov_name = "intercept")),
  gold_dorm = "non-marketscan"   # the level of `alt_dorms` that indicates it's the gold standard
)

fit_marketscan$fit(
  inlier_pct=0.9 # trim 10%
)

#saving crosswalk for future reference
#can load again using py_load_object
if(save_crosswalks){
  output_dir <- paste0("FILEPATH",gbd_round,"/cv_marketscan_pkl/")
  
  if(!dir.exists(output_dir)){
    dir.create(output_dir,recursive=TRUE)
  }
  
  #generate crosswalk id# and date, as unique crosswalk identifier
  fps <- dir(output_dir)
  if(length(fps)==0){
    suffix <- paste0("0_",Sys.info()["user"],"_",Sys.Date())
  } else{
    ids <- sapply(fps,function(fp){
      id <- strsplit(fp,split="cv_marketscan_") %>% unlist
      id <- id[2] %>% strsplit(split=".RData") %>% unlist
      id <- id %>% strsplit(split="_") %>% unlist
      return(as.numeric(id[1]))
    })
    if(all(is.na(ids))){
      suffix <- paste0("cv_marketscan_",0,"_",Sys.info()["user"],"_",Sys.Date())
    } else{
      suffix <- paste0("cv_marketscan_",max(ids,na.rm=T)+1,"_",Sys.info()["user"],"_",Sys.Date())
    }
  }
  py_save_object(object = fit_marketscan, filename = paste0(output_dir, suffix,".pkl"), pickle = "dill")
  df_result <- fit_marketscan$create_result_df()
  write.csv(df_result, paste0(modeling_dir, "/xwalk_scalars/", 
                              "marketscan_crosswalk_coefficient_", 
                              date, ".csv"), row.names=F)
}

df$obs_method <- ""
df[which(df$claims==1),"obs_method"] <- "marketscan"
df[which(df$claims!=1),"obs_method"] <- "non-marketscan"

preds_marketscan <- fit_marketscan$adjust_orig_vals( 
  df = df,
  orig_dorms = "obs_method",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error",
  data_id = "rownum"   # optional argument to add a user-defined ID to the predictions; name of the column with the IDs
)

df$mean <- preds_marketscan$ref_vals_mean
df$standard_error <- preds_marketscan$ref_vals_sd

### Adding adjustment to summary file
df_summary <- read.csv(paste0(adj_path, "step-by-step_adjustments_bundle3_",date,".csv"))
df_summary$group_review <- ifelse(is.na(df_summary$group_review),"",df_summary$group_review)
df_summary$group_review[is.na(df_summary$group_review)] <- 1
df_summary <- subset(df_summary, group_review != 0)
df_summary <- subset(df_summary, crosswalk_parent_seq %in% df$crosswalk_parent_seq)
df_summary <- right_join(df_summary %>% select(-sex),df[,c("mean","crosswalk_parent_seq","age_start","age_end","group_review","sex")],by=c("crosswalk_parent_seq","age_start","age_end","group_review"))
setnames(df_summary,c("mean"),c("marketscan_adj"))
df_summary$marketscan_beta <- fit_marketscan$beta[1]
write.csv(df_summary,paste0(adj_path, "step-by-step_adjustments_bundle3_",date,".csv"),row.names=F)

#################################################################################
## Data cleaning ## 
#################################################################################
# For any nid that has both alternative and reference, only keep the reference
inp_both <- unique(data.table(df)[,.(nid, cv_inpatient)])
inp_both <- inp_both[,.N,by="nid"]
inp_both <- inp_both[N > 1]

if (nrow(inp_both) > 0) {
  df <- subset(df, !(nid %in% inp_both$nid & cv_inpatient == 1))
}

nrow(data.table(df)[is.na(mean)]) # should be 0
df$mean <- as.numeric(df$mean)
df$cases <- df$mean * df$sample_size

# drop the duplicated source_type column (not sure where it gets created currently)
df <- within(df, rm("source_type.1"))

###################################################################
#
# Move to pydisagg age sex splitting script to complete splitting.
#
###################################################################
age_splitting_dir <- paste0(modeling_dir, "age_splitting/")
dir.create(age_splitting_dir, recursive=T)
write.csv(df, paste0(age_splitting_dir, "parent_diarrhea_", gbd_round, "_data_corrected_diagnostic_",netid, ".csv"), row.names=F)

# End age and sex splitting #
df <- fread(paste0(age_splitting_dir,"/gbd_",gbd_round,"_data_age_sex_split_",etiology, "_",netid,".csv"))

## Both sex data should be duplicated ##
source("FILEPATH/sex_split_mrbrt_weights.R")
df$seq_parent <- ""
df$crosswalk_parent_seq <- ""

sex_df <- duplicate_sex_rows(df)

sex_df$mean <- ifelse(is.na(sex_df$mean), sex_df$cases / sex_df$sample_size, sex_df$mean)
sex_df$group_review[is.na(sex_df$group_review)] <- ""
sex_df <- subset(sex_df, group_review != 0)
sex_df[is.na(sex_df)] <- ""
sex_df$standard_error <- as.numeric(sex_df$standard_error)
sex_df$group_review <- 1
sex_df$uncertainty_type <- ifelse(sex_df$lower=="","", as.character(sex_df$uncertainty_type))
sex_df$uncertainty_type_value <- ifelse(sex_df$lower=="","",sex_df$uncertainty_type_value)

nrow(sex_df[is.na(standard_error)]) # should be 0
sex_df <- as.data.frame(sex_df)

sex_df <- sex_df[, -which(names(sex_df) %in% c("cv_dhs","cv_whs","cv_nine_plus_test","cv_explicit_test","duration","duration_lower","duration_upper",
                                               "cv_clin_data","unnamed..75","unnamed..75.1","survey","original_mean","cv_diag_selfreport","is_reference",
                                               "age_mid","ratio","linear_se","std_clinical","cv_inpatient_lit","hospital_ratio","hospital_linear_se",
                                               "log_hospital_ratio","log_hospital_se","inpatient_lit_ratio","cv_miscoded","inpatient_lit_linear_se","log_inpatient_lit_ratio",
                                               "log_inpatient_lit_se", "raw_mean","raw_standard_error","crosswalk_type","parent_id","cv_hospital_child","level","cv_low_income_hosp","incidence_corrected",
                                               "cv_marketscan_inp_2000","cv_marketscan_all_2000"))]

nrow(data.table(sex_df)[mean>=1]) 
# Make sure cases < sample size
sex_df$sample_size <- as.numeric(sex_df$sample_size)
sex_df$cases <- as.numeric(sex_df$cases)
nrow(data.table(sex_df)[cases > sample_size]) 

sex_df <- join(sex_df, locs[,c("location_id","parent_id","level","region_name","super_region_name")], by="location_id")

## Mark outliers
sex_df <- df
sex_df$is_outlier <- 0

#### THIS IS WHERE THE OUTLIERING PROCESS BEGINS ####
# Create an outlier table
table(sex_df$is_outlier)
sex_df$is_outlier <- ifelse(sex_df$nid %in% 116811 & sex_df$age_end == 99, 1, sex_df$is_outlier) # outlier for now because not age split

sex_df$year_end <- ifelse(sex_df$nid==19950, 1999, sex_df$year_end)

sex_df <- subset(sex_df, standard_error < 1) 

# Outlier from review of high prevalence sources
outlier <- read.xlsx("/FILEPATH/07_06_23_xwalk_high_prevalence.xlsx")
setDT(outlier)
outlier <- outlier[!reasoning %like% "remove|Remove"]
sex_df <- as.data.table(sex_df)
sex_df <- sex_df[nid %in% outlier$nid, is_outlier:=1]

# Outlier from review of spike in age 20
outlier <- read.xlsx("/FILEPATH/07_06_23_dismod_weird_age_pattern.xlsx")
sex_df <- sex_df[nid %in% outlier$nid, is_outlier:=1]


#### BEGIN UPLOAD HERE FOR DATA SET WITH NO OUTLIERS ####
sex_df <- as.data.table(sex_df)
iterative <- sex_df

setDT(iterative)
iterative <- iterative[sample_size > 0 & sample_size < 8000000000.0]
iterative <- iterative[mean <= 1] # some means are over 1
# Add EMR back in
#iterative <- rbind(iterative, emr, fill=T) # add the dummy EMR row back in
gbd21_emr <- get_crosswalk_version(gbd21_xwalk)
gbd21_emr <- gbd21_emr[measure == "mtexcess"]
gbd21_emr[!is.na(crosswalk_parent_seq), seq := ""]
iterative <- rbind(iterative, gbd21_emr, fill=T)

iterative$unit_type <- "Person"
iterative$group <- 1
iterative$specificity <- "include"
iterative$group_review <- 1
write.xlsx(iterative, paste0(modeling_dir, "diarrhea_xwalk_upload_incl_outliers.xlsx"), sheetName="extraction")

##--------------------------------------------------------------------------------
## UPLOAD - WITH OUTLIERS
data_filepath <- paste0(modeling_dir, "diarrhea_xwalk_upload_incl_outliers.xlsx")
description <- "DESCRIPTION" 

save_crosswalk_version(bundle_version_id=bvid
                       , data_filepath=data_filepath
                       , description=description)

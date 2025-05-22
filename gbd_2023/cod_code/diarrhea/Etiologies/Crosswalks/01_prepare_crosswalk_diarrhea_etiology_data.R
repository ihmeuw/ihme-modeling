#######################################################################################
# This file is intended to be a single place to perform all data adjustments for
# diarrheal etiologies that depend on GEMS/MALED for adjustments.
# 
# This includes:
#
# - pulling and transforming extracted data, 
# - adjusting single pathogen etiology testing and inpatient sample populations
# - finally, adjusting for imperfect sensitivity and specificity.
# For ETEC and EPEC, we will also adjust these data such that they are representing
# ST- and typical serotypes, only. 
rm(list=ls())
set.seed(7)
save_crosswalks <- TRUE
run_etec_epec <- FALSE
run_rota <- TRUE
pcr_new <- FALSE # True to run developmental PCR crosswalk
skip_outliers <- FALSE # True to skip outliers from outliering sheet, False to set the outliers

coef_version <- "gbd23_dismod_age_split" # Descriptor used to distinguish crosswalk coefficients 

windows <- Sys.info()[1][["sysname"]]=="Windows"
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
config <- "/FILEPATH/"
netid <- ""
date <- format(Sys.Date(), format="%m_%d_%Y") # date for folder versioning
pkl_date <- format(Sys.Date(), format="%Y-%m-%d") 


# Prepare your needed functions and information
source("/FILEPATH/utilities.R")
library(assertable)
library(msm)
library(openxlsx)
library(matrixStats)
library(plyr)
library(dplyr)
library(boot)
library(data.table)
library(ggplot2)
library(data.table)
library(reticulate)
reticulate::use_python("/FILEPATH/python")
cw <- reticulate::import("crosswalk")

source("/FILEPATH/age_split_dismod.R")
source("/FILEPATH/sex_split_mrbrt_weights.R")
source("/FILEPATH/bundle_crosswalk_collapse.R")
source("/FILEPATH/cluster_functions.R")
invisible(sapply(list.files("/FILEPATH/", full.names = T), source))

locs <- get_location_metadata(location_set_id=35,release_id=16)

eti_info <- read.csv("/FILEPATH/eti_rr_me_ids.csv")
eti_info <- subset(eti_info, model_source=="dismod" & rei_id!=183 & cause_id==302)
rownames(eti_info)<-1:nrow(eti_info) 
setDT(eti_info)

# Pull bundle version sheet
vers <- read.xlsx("/FILEPATH/diarrhea_bundle_crosswalk_version_tracking.xlsx")
setDT(vers)
vers <- vers[best_bv==1]
setnames(vers, "bundle_version","bundle_version_id", skip_absent = TRUE)

eti_info <- join(eti_info, vers, by = "bundle_id")
# Set gbd year
gbd_year <- 2023 

# Pull last round's final model versions (to pull age pattern for cases)
last_round_models <- read.xlsx("/FILEPATH/gbd2021_diarrhea_etio_final_model_metadata.xlsx")
setDT(last_round_models)

# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2

## Load in information for sex splitting ##
diar_sex_weights <- get_outputs(topic="cause", cause_id=302, age_group_id=22, year_id=2021, release_id=9, measure_id=6, sex_id=c(1,2), location_id=1, metric_id=3,compare_version_id=8016)
diar_sex_weights$number <- diar_sex_weights$val

diarrhea_dir <- paste0("/FILEPATH/gbd_", gbd_year, "/", date, "/")
dir.create(diarrhea_dir)

eti_loops <- 9 # 1:13

# Create table to save new crosswalk versions if save_crosswalks=TRUE.
if (save_crosswalks) {
  new_xwalks <- eti_info[eti_loops,.(bundle_id)]
  new_xwalks[,`:=`(bv_id=integer(), crosswalk_version_id=integer())]
  out_xwalk_ids <- paste0("/FILEPATH/diarrhea_etios_xwalk_ids_",date,".csv")
}

gems_maled <- fread("/FILEPATH/GEMS1_GEMS1A_MALED_NIDS.csv")

##############################################################################################
## Step 1.1: Pull and save data. 
##############################################################################################

## Build loop here:
for(i in eti_loops){
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  m_name <- eti_info$modelable_entity_name[i]
  
  if(i == 6){
    m_name <- "EPEC"
  } else if(i == 7){
    m_name <- "ETEC"
  } else if(i == 10){
    m_name <- "Salmonella"
  }
  
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]
  bundle_id <- eti_info$bundle_id[i]
  bundle_version_id <- vers[bundle_id==bundle]$bundle_version_id
  print(paste0("Pulling data for ", name, " bundle_version ", bundle_version_id))
  df <- get_bundle_version(bundle_version_id = bundle_version_id, fetch = 'all')
  df$raw_mean <- df$mean
  df$raw_standard_error <- df$standard_error
  
  means <- df$cases / df$sample_size
  df$mean <- ifelse(is.na(df$mean), means, df$mean)
  df <- subset(df, !is.na(mean))
  
  df <- subset(df, year_end >= 1986)
  
  if (name=="Campylobacter") {
    df$group_review[df$nid %in% gems_maled[Study=="MALED"]$nid & df$cv_diag_pcr == 0] <- 0
    print("Subsetted out MALED nonPCR for Campylobacter")
  }
  
  ## All rows should not have NA values for the key crosswalks
  if("cv_explicit_test" %in% names(df)){
    df$cv_explicit_test[is.na(df$cv_explicit_test)] <- 0
  }
  if("cv_inpatient" %in% names(df)){
    df$cv_inpatient[is.na(df$cv_inpatient)] <- 0
  }
  if("cv_inpatient_sample" %in% names(df)){
    df$cv_inpatient_sample[is.na(df$cv_inpatient_sample)] <- 0
  }
  
  # Keep if proportion
  df <- subset(df, measure=="proportion")
  out_path <- paste0("/FILEPATH/",me_name,"/",bundle,"/FILEPATH/")
  if (!dir.exists(out_path)) {dir.create(out_path, recursive=TRUE)}
  
  write.csv(df, paste0("/FILEPATH/",me_name,"/",bundle,"/FILEPATH/",name,"_data_for_crosswalks_",date,"_", netid,".csv"), row.names=F)
}

##############################################################################################
## Step 1.2: Fill out potentially blank cv_diag_pcr.
##############################################################################################
pcr_nids <- c(264996)

# Collect them all if I want to review simultaneously.
out_df <- data.frame()
for(i in eti_loops){
  
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]
  
  print(paste0("Pulling and appending for ", name))
  in_df <- read.csv(paste0("/FILEPATH/",name,"_data_for_crosswalks_",date,"_", netid,".csv"))
  in_df$bundle_record <- bundle
  
  # Use regular expressions to find key words
  in_df$citation <- as.character(in_df$field_citation_value)
  in_df$cv_diag_pcr <- ifelse(!is.na(in_df$cv_diag_pcr), in_df$cv_diag_pcr,
                              ifelse(in_df$citation %like% "olecular", 1,
                                ifelse(in_df$citation %like% "PCR",1,
                                     ifelse(in_df$citation %like% "rray",1,
                                            ifelse(in_df$citation %like% "ultiplex",1,0)))))
  table(in_df$cv_diag_pcr)
  in_df$cv_diag_pcr <- ifelse(!is.na(in_df$cv_diag_pcr), in_df$cv_diag_pcr,
                              ifelse(in_df$case_diagnostics %like% "olecular", 1,
                                     ifelse(in_df$case_diagnostics %like% "PCR",1,
                                            ifelse(in_df$case_diagnostics %like% "rray",1,
                                                   ifelse(in_df$case_diagnostics %like% "ultiplex",1,0)))))
  # Rewrite those files now that we have the new column
  
  write.csv(in_df, paste0("/FILEPATH/",name,"_data_for_crosswalks_",date,"_", netid,".csv"), row.names=F)
  
  out_df <- rbind.fill(out_df, in_df)
  
  ### Save a step-by-step breakdown of the adjustment process for each row
  ### This file will be updated each time an adjustment is made
  meta_cols <- c("seq","nid","location_id","location_name","year_start","year_end","age_start","age_end","sex","group_review","cv_inpatient","cv_explicit_test","cv_diag_pcr")
  if(bundle==7){ 
    meta_cols <- c(meta_cols,"cv_tepec")
  } else if(bundle==8){ 
    meta_cols <- c(meta_cols,"cv_st_etec")
  }
  
  df_summary <- data.frame(in_df)[,meta_cols]
  df_summary$cases <- in_df$cases
  df_summary$sample_size <- in_df$sample_size
  df_summary$raw_mean <- in_df$mean
  
  adj_path <- paste0("/FILEPATH/03_review/pcr_test/")
  if (!dir.exists(adj_path)) {dir.create(adj_path, recursive=TRUE)}
  write.csv(df_summary,paste0(adj_path, "step-by-step_adjustments_",name,"_",date,".csv"),row.names=F)
}

##############################################################################################
##-- Step 1.3: Adjust typical EPEC and ST-ETEC  ----------------------------------------------
##############################################################################################
if(run_etec_epec){
  if (6 %in% eti_loops == TRUE) {
    print("Launching epec code")
    ## tEPEC ratio ##
    df <- read.csv(paste0("/FILEPATH/EPEC_data_for_crosswalks_",date,"_", netid,".csv"))
    cv_tepec_check <- unique(data.table(df)[case_name == "epec" | case_name == "EPEC",.(case_name, cv_tepec)])
    if (1 %in% cv_tepec_check$cv_tepec) {
      stop("There are rows in the EPEC bundle where case_name is EPEC or epec but cv_tepec = 1. Check, revise, and rerun.")
    }
    cv_tepec_check <- unique(data.table(df)[case_name %like% "tepec|tEPEC",.(case_name, cv_tepec)])
    if (0 %in% cv_tepec_check$cv_tepec) {
      stop("There are rows in the EPEC bundle where case_name is tEPEC or tepec but cv_tepec = 0. Check, revise, and rerun.")
    }
    
    if (sum(is.na(df$cv_tepec)) > 0) {
      df$cv_tepec <- ifelse(is.na(df$cv_tepec),0,df$cv_tepec)
      message("There are some rows without a case_name assigned. cv_tepec assigned 0 for these rows.")
    }
    
    # Only keep tEPEC in dismod for sources with EPEC and tEPEC per nid, location, years, age, sex, year, cv_diag_pcr, cv_inpatient
    setDT(df)
    
    case_name_count <- df[ ,.(count=uniqueN(case_name)), by=c("nid", "location_name", "sex", "age_start","age_end", "year_start", "year_end", "cv_diag_pcr", "cv_inpatient")]
    case_name_count <- case_name_count[count > 1]
    df[paste0(nid, location_name, sex, age_start, age_end, year_start, year_end, cv_diag_pcr, cv_inpatient) %in%
         paste0(case_name_count$nid, case_name_count$location_name, case_name_count$sex, case_name_count$age_start, case_name_count$age_end,
                case_name_count$year_start, case_name_count$year_end, case_name_count$cv_diag_pcr, case_name_count$cv_inpatient) & cv_tepec==0, group_review:=0]
    df <- data.frame(df)
    
    # Apply pcr-specific ratios
    epec_pcr_ratio <- fread("/FILEPATH/epec_pcr_beta_vals.csv")
    epec_pcr_ratio$se <- (epec_pcr_ratio$beta_upper - epec_pcr_ratio$beta_lower)/(2*1.96)
    epec_non_pcr_ratio <- fread("/FILEPATH/epec_nonpcr_beta_vals.csv")
    epec_non_pcr_ratio$se <- (epec_non_pcr_ratio$beta_upper - epec_non_pcr_ratio$beta_lower)/(2*1.96)
    
    df$original_mean <- df$mean
    df$mean <- ifelse(df$cv_tepec==0 & df$cv_diag_pcr == 1, df$mean * epec_pcr_ratio$beta_mean, df$mean)
    df$mean <- ifelse(df$cv_tepec==0 & df$cv_diag_pcr == 0, df$mean * epec_non_pcr_ratio$beta_mean, df$mean)
    
    cols <- c("group",
              "group_review",
              "standard_error",
              "cv_nine_plus_test",
              "original_mean",
              "predicted",
              "pred_std",
              "cv_hospital",
              "cv_outpatient",
              "lat",
              "bm",
              "gbd_round",
              "adjusted_mean_2017",
              "gbd_2019_new",
              "year_mid",
              "age_range",
              "Unnamed..0"
    )
    df[,cols] <- sapply(df[,cols],as.numeric)
    
    df$pred_ratio <- ifelse(df$cv_diag_pcr == 1, epec_pcr_ratio$beta_mean, epec_non_pcr_ratio$beta_mean)
    df$pred_se <- ifelse(df$cv_diag_pcr == 1, epec_pcr_ratio$se, epec_non_pcr_ratio$se)
    df$raw_mean <- df$mean
    df$se_original <- df$standard_error
    df$standard_error <- ifelse(df$cv_tepec!=1,sqrt(df$mean^2 * df$pred_se^2 + df$pred_ratio^2 * df$standard_error^2 + df$standard_error^2 * df$pred_se^2),df$standard_error)
    df$note_modeler <- ifelse(df$cv_tepec!=1,paste0(df$note_modeler, "| The original value was ", df$original_mean," and was adjusted using a pcr-specific ratio from gems, maled, and lit data."),df$note_modeler)
    df[is.na(df)] <- ""
    
    write.csv(df, paste0("/FILEPATH/EPEC_data_for_crosswalks_",date,"_", netid,".csv"))
    
    ### Adding adjustment to summary file
    df$group_review <- ifelse(df$group_review=="",NA,df$group_review)
    df_summary <- read.csv(paste0("/FILEPATH/step-by-step_adjustments_EPEC_",date,".csv"))
    df_summary$group_review <- as.integer(df_summary$group_review)
    df$group_review <- as.integer(df$group_review)
    df_summary <- right_join(df_summary,df[,c("mean","pred_ratio","seq","age_start","age_end","group_review")],by=c("seq","age_start","age_end","group_review"))
    setnames(df_summary,c("mean","pred_ratio"),c("tepec_adj","tepec_ratio"))
    write.csv(df_summary,paste0("/FILEPATH/step-by-step_adjustments_EPEC_",date,".csv"),row.names=F)
    
  }
  
  if (7 %in% eti_loops == TRUE) {
    ############################################################################
    ## ST-ETEC ratio ##
    print("Launching etec code")
    df <- read.csv(paste0("/FILEPATH/ETEC_data_for_crosswalks_",date,"_", netid,".csv"))
    
    # cv_st_etec validations
    cv_st_check <- unique(data.table(df)[case_name %like% "ST|st",.(case_name, cv_st_etec)])
    if (0 %in% cv_st_check$cv_st_etec) {
      stop("There are rows in the ETEC bundle where case_name is ST-ETEC but cv_st_etec = 0. Check, revise, and rerun.")
    }
    cv_st_check <- unique(data.table(df)[case_name == "ETEC" | case_name == "etec",.(case_name, cv_st_etec)])
    if (1 %in% cv_st_check$cv_st_etec) {
      stop("There are rows in the ETEC bundle where case_name is ETEC or etec but cv_st_etec = 1. Check, revise, and rerun.")
    }
    
    if (sum(is.na(df$cv_st_etec)) > 0) {
      df$cv_st_etec <- ifelse(is.na(df$cv_st_etec),0,df$cv_st_etec)
      message("There are some rows without a case_name assigned. cv_st_etec assigned 0 for these rows.")
    }
    
    # Only keep ST-ETEC in dismod for sources with ETEC and ST-ETEC per nid, location, years, age, sex, year, cv_diag_pcr, cv_inpatient
    # If a row has both ST-ETEC combined and ST-ETEC, group_review should already be 0 for ST-ETEC. This captures all instances of ETEC.
    setDT(df)
    case_name_count <- df[ ,.(count=uniqueN(case_name)), by=c("nid", "location_name", "sex", "age_start","age_end", "year_start", "year_end", "cv_diag_pcr", "cv_inpatient")]
    case_name_count <- case_name_count[count > 1]
    df[paste0(nid, location_name, sex, age_start, age_end, year_start, year_end, cv_diag_pcr, cv_inpatient) %in%
         paste0(case_name_count$nid, case_name_count$location_name, case_name_count$sex, case_name_count$age_start, case_name_count$age_end,
                case_name_count$year_start, case_name_count$year_end, case_name_count$cv_diag_pcr, case_name_count$cv_inpatient) & cv_st_etec==0, group_review:=0]
    df <- data.frame(df)
    
    # Apply pcr-specific ratios
    etec_pcr_ratio <- fread("/FILEPATH/etec_pcr_beta_vals.csv")
    etec_pcr_ratio$se <- (etec_pcr_ratio$beta_upper - etec_pcr_ratio$beta_lower)/(2*1.96)
    etec_non_pcr_ratio <- fread("/FILEPATH/etec_nonpcr_beta_vals.csv")
    etec_non_pcr_ratio$se <- (etec_non_pcr_ratio$beta_upper - etec_non_pcr_ratio$beta_lower)/(2*1.96)
    
    df$original_mean <- df$mean
    df$mean <- ifelse(df$cv_st_etec==0 & df$cv_diag_pcr == 1, df$mean * etec_pcr_ratio$beta_mean, df$mean)
    df$mean <- ifelse(df$cv_st_etec==0 & df$cv_diag_pcr == 0, df$mean * etec_non_pcr_ratio$beta_mean, df$mean)
    
    cols <- c("standard_error",
              "year_mid",
              "age_range"
    )
    df[,cols] <- sapply(df[,cols],as.numeric)
    
    df$pred_ratio <- ifelse(df$cv_diag_pcr == 1, etec_pcr_ratio$beta_mean, etec_non_pcr_ratio$beta_mean)
    df$pred_se <- ifelse(df$cv_diag_pcr == 1, etec_pcr_ratio$se, etec_non_pcr_ratio$se)
    df$raw_mean <- df$mean
    df$se_original <- df$standard_error
    df$standard_error <- ifelse(df$cv_st_etec!=1,sqrt(df$mean^2 * df$pred_se^2 + df$pred_ratio^2 * df$standard_error^2 + df$standard_error^2 * df$pred_se^2),df$standard_error)
    df$note_modeler <- ifelse(df$cv_st_etec!=1,paste0(df$note_modeler, "| The original value was ", df$original_mean," and was adjusted using a pcr-specific ratio from gems, maled, and lit data."),df$note_modeler)
    df[is.na(df)] <- ""
    
    write.csv(df, paste0("FILEPATH/ETEC_data_for_crosswalks_",date,"_", netid,".csv"))
    
    ### Adding adjustment to summary file
    df$group_review <- ifelse(df$group_review=="",NA,df$group_review)
    df_summary <- read.csv(paste0("/FILEPATH/step-by-step_adjustments_ETEC_",date,".csv"))
    df_summary$group_review <- as.integer(df_summary$group_review)
    df$group_review <- as.integer(df$group_review)
    df_summary <- right_join(df_summary,df[,c("mean","pred_ratio","seq","age_start","age_end","group_review")],by=c("seq","age_start","age_end","group_review"))
    setnames(df_summary,c("mean","pred_ratio"),c("st_etec_adj","st_etec_ratio"))
    write.csv(df_summary,paste0("/FILEPATH/step-by-step_adjustments_ETEC_",date,".csv"),row.names=F)
  }
}
##############################################################################################
## Step 2: Run crosswalks in MR-BRT ---------------------------------------------------------
## This is also where data is sex-split
##############################################################################################
## Set the type, must be "log" or "logit"
xw_transform <- "logit"

out_res <- data.frame()
for(i in eti_loops){
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]
  
  print(paste0("Modeling an MR-BRT crosswalk for ", name))
  
  df <- read.csv(paste0("/FILEPATH/",name,"_data_for_crosswalks_",date,"_", netid,".csv"))

  # Keep track of the original mean value
  df$raw_mean <- df$mean
  df$raw_standard_error <- df$standard_error
  
  # Keep if year > 1985
  df <- subset(df, year_start >= 1985)
  df$group_review[is.na(df$group_review)] <- 1
  df <- subset(df, group_review != 0)
  df$group_review <- ifelse(df$specificity=="","",df$group_review)
  ##--------------------------------------------------------------------
  # Pull out new data and save in the Upload folder
  df$crosswalk_parent_seq <- ifelse(!is.na(df$seq),df$seq, df$seq_parent)
  df[is.na(df)] <- ""
  
  # This is where data is sex split
  df$is_sexsplit <- ifelse(df$sex=="Both",1,0)
  df <- scalar_sex_rows(df, denominator_weights=diar_sex_weights, bounded=T, global_merge=T, title=name)
  # 
  ##-------------------------------------------------------------------------------------------------
  #### Inpatient sample population ####
  
  df$inpatient <- ifelse(df$cv_inpatient==1,1,ifelse(df$cv_inpatient_sample==1,1,0))
  df$inpatient[is.na(df$inpatient)] <- 0
  df$is_reference <- ifelse(df$inpatient==1,0,1)
  
  df$linear_floor <- median(df$mean[df$mean>0]) * 0.01
  df$mean <- ifelse(df$mean<=0, df$linear_floor, df$mean)
  df$mean <- ifelse(df$mean>=1, 1-df$linear_floor, df$mean)

  df$obs_method <- ""
  df[which(df$cv_inpatient==1),"obs_method"] <- "inpatient"
  df[which(df$cv_inpatient!=1),"obs_method"] <- "non-inpatient"
  df$rownum <- 1:nrow(df)
  
  cv_inpatient <- bundle_crosswalk_collapse(df=df,
                                            reference_name="is_reference", 
                                            covariate_name="inpatient", 
                                            age_cut=c(0,1,2,5,20,40,60,80,100), 
                                            year_cut=c(seq(1980,2015,5),2020), 
                                            merge_type="within", 
                                            location_match="exact", include_logit = T, release_id=16)
  
  # Can be duplicated if there is an age split
  cv_inpatient <- cv_inpatient[!duplicated(cv_inpatient$ratio),]
  
  # Test trimming aggressiveness based on number of observations
  num_obs <- length(cv_inpatient)
  trim_pct <- ifelse(num_obs < 10, 0.05, ifelse(num_obs < 20, 0.1, 0.25))
  trim_pct <- 0.1
  
  # After age-splitting, we might have multiple rows of same values
  cv_inpatient$unique <- paste0(cv_inpatient$nid, "_", round(cv_inpatient$ratio,5))
  cv_inpatient <- subset(cv_inpatient, !duplicated(unique))
  
  # Now doing the crosswalks in logit space #
  if(xw_transform == "log"){
    cv_inpatient$response_var <- cv_inpatient$log_ratio
    cv_inpatient$response_se <- cv_inpatient$delta_log_se
  } else {
    cv_inpatient$response_var <- cv_inpatient$logit_ratio
    cv_inpatient$response_se <- cv_inpatient$logit_ratio_se
  }
  
  cv_inpatient$altvar <- "inpatient"
  cv_inpatient$refvar <- "non-inpatient"
  
  df1 <- cw$CWData(
    df = cv_inpatient,          # dataset for metaregression
    obs = "logit_diff",       # column name for the observation mean
    obs_se = "logit_diff_se", # column name for the observation standard error
    alt_dorms = "altvar",     # column name of the variable indicating the alternative method
    ref_dorms = "refvar",     # column name of the variable indicating the reference method
    study_id = "nid",    # name of the column indicating group membership, usually the matching groups
    add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
  )
  
  fit_inpatient <- cw$CWModel(
    cwdata = df1,            # object returned by `CWData()`
    obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
    cov_models = list(       # specifying predictors in the model; see help(CovModel)
      cw$CovModel(cov_name = "intercept")),
    gold_dorm = "non-inpatient"   # the level of `alt_dorms` that indicates it's the gold standard
  )
  
  fit_inpatient$fit()
  
  ### save fit to folder
  #make folder, if necessary
  output_dir <- paste0("FILEPATH")
  dir.create(paste0("/FILEPATH"), recursive=TRUE)
             
  if(!dir.exists(output_dir)){
    dir.create(output_dir,recursive=TRUE)
  }
  if(save_crosswalks){
    #generate crosswalk id# and date, as unique crosswalk identifier
    fps <- dir(output_dir)
    if(length(fps)==0){
      suffix <- paste0("0_",Sys.info()["user"],"_",Sys.Date())
    } else{
      ids <- sapply(fps,function(fp){
        id <- strsplit(fp,split="cv_inpatient_") %>% unlist
        id <- id[2] %>% strsplit(split=".pkl") %>% unlist
        id <- id %>% strsplit(split="_") %>% unlist
        return(as.numeric(id[1]))
      })
      if(all(is.na(ids))){
        suffix <- paste0("cv_inpatient_",0,"_",Sys.info()["user"],"_",Sys.Date())
      } else{
        suffix <- paste0("cv_inpatient_",max(ids,na.rm=T)+1,"_",Sys.info()["user"],"_",Sys.Date())
      }
    }
    
    #saving crosswalk for future reference
    py_save_object(object = fit_inpatient, filename = paste0(output_dir, suffix,".pkl"), pickle = "dill")
    df_result <- fit_inpatient$create_result_df()
    write.csv(df_result, paste0("/FILEPATH/inpatient_crosswalk_coefficient_", date, "_", coef_version, ".csv"), row.names=F)
    
  }
  
  preds1 <- fit_inpatient$adjust_orig_vals( 
    df = df,
    orig_dorms = "obs_method",
    orig_vals_mean = "mean",
    orig_vals_se = "standard_error",
    data_id = "rownum"   # optional argument to add a user-defined ID to the predictions;
    # name of the column with the IDs
  )
  
  df$mean <- preds1$ref_vals_mean
  df$standard_error <- preds1$ref_vals_sd
  
  ### Adding adjustment to summary file
  df_summary <- read.csv(paste0("/FILEPATH/step-by-step_adjustments_",name,"_",date,".csv"))
  df_summary$group_review <- ifelse(is.na(df_summary$group_review),"",df_summary$group_review)
  df_summary$group_review[is.na(df_summary$group_review)] <- 1 
  df_summary <- subset(df_summary, group_review != 0)
  df_summary <- subset(df_summary, seq %in% df$crosswalk_parent_seq) #
  setnames(df_summary,"seq","crosswalk_parent_seq")
  df_summary <- right_join(df_summary %>% select(-sex),df[,c("mean","crosswalk_parent_seq","age_start","age_end","group_review","sex")],by=c("crosswalk_parent_seq","age_start","age_end","group_review"))
  setnames(df_summary,c("mean"),c("inpatient_adj"))
  df_summary$inp_beta <- fit_inpatient$beta[1]
  write.csv(df_summary,paste0("/FILEPATH/step-by-step_adjustments_",name,"_",date,".csv"),row.names=F)
  
  ##-------------------------------------------------------------------------------------------------
  #### Single pathogen adjustment ####
  df$is_reference <- ifelse(df$cv_explicit_test==1,0,1)
  
  if(exists("cv_explicit")){rm(cv_explicit)}
  try(
    cv_explicit <- bundle_crosswalk_collapse(df=df,
                                              reference_name="is_reference", 
                                              covariate_name="cv_explicit_test", 
                                              age_cut=c(0,1,2,5,20,40,60,80,100), 
                                              year_cut=c(seq(1980,2015,5),2020), 
                                              merge_type="between", 
                                              location_match="exact", include_logit = T, release_id=16)
  )
  
  if(exists("cv_explicit")){
    print("We are able to adjust for cv_explicit_test.")
    if(i==5) { 
      cv_explicit <- subset(cv_explicit,!(age_bin=="(40,60]"&nid %in% c(265480,264989)&n_nid %in% c(265480,264989)))
      cv_explicit <- subset(cv_explicit,!(age_bin=="(40,60]"&nid %in% c(109172,269772)&n_nid %in% c(109172,269772)))
      cv_explicit <- subset(cv_explicit,!(nid==109172&n_nid==269772)) # Dropping this for not matching exactly on age_start and age_end
    }
    if(i==4) { 
      cv_explicit <- subset(cv_explicit,!(age_bin=="(2,5]"&nid %in% c(399159,493671)&n_nid %in%c(399159,493671)))
    }
    if(i==1){
      cv_explicit <- subset(cv_explicit,!(nid==220483&n_nid==440094)) # Dropping this for not matching on location (different thailand subnationals)
    }
    if(i==11){
      cv_explicit <- subset(cv_explicit,!((nid==220483&n_nid==440094)|
                                            (nid==232129&n_nid==109200)|
                                            (nid==137422&n_nid==109200)|
                                            (nid==116741&n_nid==109200)| 
                                            (nid==233489&n_nid==109200))) # Dropping this for not matching exactly on age_start and age_end or location
      
      cv_explicit <- subset(cv_explicit,!((nid==319503&n_nid==439954))) 
    }
    if(i==10){
      cv_explicit <- subset(cv_explicit,!((nid==116741&n_nid==94537))) # Dropping this for not matching exactly on age_start and age_end
    } 
    if(i==9){
      cv_explicit <- subset(cv_explicit,!((nid==116741&n_nid==94537)|  # Dropping this for not matching exactly on age_start and age_end
                                            (nid==94482&n_nid==417814)|
                                            (nid==319504&n_nid==220503)|
                                            (nid==353329&n_nid==397939)|
                                            (nid==319506&n_nid==417744)|
                                            (nid==317460&n_nid==417814)|
                                            (nid==440098&n_nid==116744)|
                                            (nid==440098&n_nid==109089)|
                                            (nid==417062&n_nid==317376)|
                                            (nid==319506&n_nid==116762)|
                                            (nid==317460&n_nid==220503)|
                                            (nid==417074&n_nid==417051)|
                                            (nid==417711&n_nid==417796)|
                                            (nid==281716&n_nid==317313)|
                                            (nid==265477&n_nid==116531)|
                                            (nid==264652&n_nid==230579&age_bin=="(0,1]")|
                                            (nid==264641&n_nid==319527)|
                                            (nid==243582&n_nid==220503)|
                                            (nid==233489&n_nid==109118)|
                                            (nid==230615&n_nid==417814)|
                                            (nid==220419&n_nid==317353)|
                                            (nid==220419&n_nid==116738)|
                                            (nid==220375&n_nid==440096)|
                                            (nid==220341&n_nid==417744)|
                                            (nid==220337&n_nid==439956)|
                                            (nid==220337&n_nid==116744)|
                                            (nid==220296&n_nid==417814)|
                                            (nid==220296&n_nid==220503)|
                                            (nid==219138&n_nid==417814)|
                                            (nid==219138&n_nid==220503)|
                                            (nid==116763&n_nid==116721)| ## this is not an exact age match, due to aggregation
                                            (nid==116758&n_nid==440125)| 
                                            (nid==116758&n_nid==417744)|
                                            (nid==116758&n_nid==116762)|
                                            (nid==116750&n_nid==116531)|
                                            (nid==116748&n_nid==439956)|
                                            (nid==116741&n_nid==94537)| 
                                            (nid==116741&n_nid==109098)|
                                            (nid==116741&n_nid==109097)|
                                            (nid==116728&n_nid==141095)|
                                            (nid==116728&n_nid==141094)|
                                            (nid==116569&n_nid==116742)|
                                            (nid==116522&n_nid==116721)|
                                            (nid==116394&n_nid==109063)|
                                            (nid==109172&n_nid==141097)|
                                            (nid==109172&n_nid==141096)|
                                            (nid==109172&n_nid==141095)|
                                            (nid==109172&n_nid==141094)|
                                            (nid==109172&n_nid==141093)|
                                            (nid==109096&n_nid==109058)|
                                            (nid==293684&n_nid==265480)
      ))
    }
    if(i==8){
      cv_explicit <- subset(cv_explicit,!((nid==353329&n_nid==264689))) # Dropping this for not matching exactly on age_start and age_end
    }
    if(i==7){
      cv_explicit <- subset(cv_explicit,!((nid==116741&n_nid==94537)|(nid==116545&n_nid==116802))) # Dropping this for not matching exactly on age_start and age_end
    }
    cv_explicit <- subset(cv_explicit,age_bin != "(0,20]") # excluding 0-20 age bin because the younger and older age groups aren't comparable
  } else{
    cv_explicit <- data.frame()
    print(paste0("Multipathogen crosswalk not run for ", name, " due to lack of matched pairs."))
  }

  if(nrow(cv_explicit) != 0){ 
    print("Launch explicit test crosswalk")
    cv_explicit$location_match <- ifelse(cv_explicit$location_match=="","Other",cv_explicit$location_match)
    cv_explicit$match_nid <- paste0(cv_explicit$nid,"_",cv_explicit$n_nid)
    
    # After age-splitting, we might have multiple rows of same values
    cv_explicit$unique <- paste0(cv_explicit$match_nid, "_", round(cv_explicit$ratio,5))
    cv_explicit <- subset(cv_explicit, !duplicated(unique))
    
    cv_explicit$altvar <- "explicit"
    cv_explicit$refvar <- "multi-pathogen"
    
    df2 <- cw$CWData(
      df = cv_explicit,          # dataset for metaregression
      obs = "logit_diff",       # column name for the observation mean
      obs_se = "logit_diff_se", # column name for the observation standard error
      alt_dorms = "altvar",     # column name of the variable indicating the alternative method
      ref_dorms = "refvar",     # column name of the variable indicating the reference method
      study_id = "match_nid",    # name of the column indicating group membership, usually the matching groups
      add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
    )
    
    fit_explicit <- cw$CWModel(
      cwdata = df2,            # object returned by `CWData()`
      obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
      cov_models = list(       # specifying predictors in the model; see help(CovModel)
        cw$CovModel(cov_name = "intercept")),
      gold_dorm = "multi-pathogen"   # the level of `alt_dorms` that indicates it's the gold standard
    )
    
    fit_explicit$fit()
    
    if(save_crosswalks){
      ### save fit to folder
      #make folder, if necessary
      output_dir <- paste0("/FILEPATH/gbd_",gbd_year,"/cv_explicit/")
      
      if(!dir.exists(output_dir)){
        dir.create(output_dir,recursive=TRUE)
      }
      
      #generate crosswalk id# and date, as unique crosswalk identifier
      fps <- dir(output_dir)
      if(length(fps)==0){
        suffix <- paste0("0_",Sys.info()["user"],"_",Sys.Date())
      } else{
        ids <- sapply(fps,function(fp){
          id <- strsplit(fp,split="cv_explicit_") %>% unlist
          id <- id[2] %>% strsplit(split=".RData") %>% unlist
          id <- id %>% strsplit(split="_") %>% unlist
          return(as.numeric(id[1]))
        })
        if(all(is.na(ids))){
          suffix <- paste0("cv_explicit_",0,"_",Sys.info()["user"],"_",Sys.Date())
        } else{
          suffix <- paste0("cv_explicit_",max(ids,na.rm=T)+1,"_",Sys.info()["user"],"_",Sys.Date())
        }
      }
      #saving crosswalk for future reference
      py_save_object(object = fit_explicit, filename = paste0(output_dir, suffix,".pkl"), pickle = "dill")
      df_result <- fit_explicit$create_result_df()
      write.csv(df_result, paste0("/FILEPATH/explicit_crosswalk_coefficient_", date, "_", coef_version, ".csv"), row.names=F)
    }
    
    df$linear_floor <- median(df$mean[df$mean>0]) * 0.01
    df$mean <- ifelse(df$mean<=0, df$linear_floor, df$mean)
    df$mean <- ifelse(df$mean>=1, 1-df$linear_floor, df$mean) #offsetting some mean=1 and mean=0 values, since they need to be logit-transformed
    
    df$obs_method <- ""
    df[which(df$cv_explicit_test==1),"obs_method"] <- "explicit"
    df[which(df$cv_explicit_test!=1),"obs_method"] <- "multi-pathogen"
    
    preds1 <- fit_explicit$adjust_orig_vals( 
      df = df,
      orig_dorms = "obs_method",
      orig_vals_mean = "mean",
      orig_vals_se = "standard_error",
      data_id = "rownum"   # optional argument to add a user-defined ID to the predictions;
    )
    df$mean <- preds1$ref_vals_mean
    df$standard_error <- preds1$ref_vals_sd
  } else{
    df <- subset(df,cv_explicit_test!=1) # if there is no multipathogen crosswalk, we can't include single-pathogen studies
  }
  
  df$cases <- df$mean*df$sample_size # cases should be adjusted after crosswalk, for age-splitting purposes
  df <- subset(df, nid != 999999)
  
  setnames(df,"note_SR","note_sr",skip_absent = T)
  df <- df[ , -c("cv_explicit","cv_pcr","round_age_start","round_age_end","dummy_age_split",
                                                           "cv_age_split","logit_mean","delta_logit_se", "pred_mean","pred_linear_se","year_id",
                                                           "age_sum","inpatient","is_reference","log_inpatient","se_log_inpatient",
                                                           "mrbrt_inpatient","mrbrt_se_inpatient","mrbrt_explicit","mrbrt_se_explicit","adjustment_type")]
  df[is.na(df)] <- ""
  df$crosswalk_parent_seq <- ifelse(df$crosswalk_parent_seq=="",df$seq,df$crosswalk_parent_seq)
  df$uncertainty_type <- ifelse(df$lower=="","", as.character(df$uncertainty_type))
  df$uncertainty_type_value <- ifelse(df$lower=="","",df$uncertainty_type_value)
  
  # After Crosswalking, resetting cases, sometimes cases > sample size
  df$cases <- as.numeric(df$cases)
  df$sample_size <- as.numeric(df$sample_size)
  df$cases <- ifelse(df$cases > df$sample_size, df$sample_size * 0.99, df$cases)
  
  df$standard_error <- ifelse(df$standard_error > 1, 0.99, df$standard_error)
  
  ### Adding adjustment to summary file
  df$group_review <- ifelse(df$group_review=="",NA,df$group_review) %>% as.numeric
  df_summary <- read.csv(paste0("/FILEPATH/step-by-step_adjustments_",name,"_",date,".csv"))
  df_summary$group_review <- as.numeric(df_summary$group_review)
  df_summary <- right_join(df_summary,df[,c("mean","crosswalk_parent_seq","age_start","age_end","group_review","sex")],by=c("crosswalk_parent_seq","age_start","age_end","group_review","sex"))
  setnames(df_summary,c("mean"),c("explicit_adj"))
  if(nrow(cv_explicit) != 0){ 
    df_summary$exp_beta <- fit_explicit$beta[1]
  } else{
    df_summary$exp_beta <- NA
  }
  df_summary <- subset(df_summary, year_start >= 1985)
  write.csv(df_summary,paste0("/FILEPATH/step-by-step_adjustments_",name,"_",date,".csv"),row.names=F)
  write.csv(df, paste0(out_path, "/",name,"_",gbd_year,"_data_corrected_diagnostic_",netid,".csv"), row.names=F)
  
  out_path <- paste0("FILEPATH/pcr_test/")
  if (!dir.exists(out_path)) {dir.create(out_path, recursive=TRUE)}
  write.csv(df, paste0(out_path, "/",name,"_",gbd_year,"_data_corrected_diagnostic_",netid,".csv"), row.names=F)
}


##############################################################################################
## Step 3: These data need to be age split. 
##############################################################################################
## Define age groups ##

age_info <- read.csv("/FILEPATH/age_mapping.csv")
age_info <- subset(age_info, !is.na(order))
age_info <- subset(age_info, age_group_id != 33)

## Determine where age splitting should occur. ##
age_map <- read.csv("/FILEPATH/age_mapping.csv")
age_map <- age_map[age_map$age_pull==1,c("age_group_id","age_start","age_end","order","age_pull")]
age_cuts <- c(0,1,5,20,40,65,100)
length <- length(age_cuts) - 1

age_map$age_dummy <- cut(age_map$age_end, age_cuts, labels = paste0("Group ", 1:length))

age_map_ids <- data.table(age_dummy=c("Group 1", "Group 2", "Group 3", "Group 4", "Group 5", "Group 6"),
                          age_group_id=c(28,5,188,203,220,18),
                          age_start=c(0,1,5,20,40,65),
                          age_end=c(1,5,20,40,65,100)) 

## Create prevalence weights to age-split the data from the last best global diarrhea prevalence age pattern ##
dmod_global <- get_outputs(topic="cause",
                           cause_id=302,
                           age_group_id = age_info$age_group_id,
                           year_id=1990:2019,
                           location_id=1,
                           sex_id=3,
                           measure_id=5,
                           metric_id=1,
                           release_id=6,
                           compare_version_id=7244)

# Temporarily duplicate pattern for 2019 into 2020 through 2024 
# We need recent years to merge the age pattern into our etiology data by year_id.
temp_pattern <- dmod_global[year_id==2019]
temp_pattern$dummy <- 1
years <- data.table(year_id=2020:2024, dummy=1)
years <- join(years, temp_pattern[,-"year_id"], by="dummy") # get temp age patterns for 2020:2024
years <- years[,-"dummy"]

dmod_global <- rbind(dmod_global, years)

dmod_global$number <- dmod_global$val
denom_weights <- dmod_global
denom_weights <- join(denom_weights, age_map[,c("age_group_id","age_dummy","order")], by="age_group_id")

denom_age_groups <- aggregate(number ~ location_id + year_id + age_dummy, data=denom_weights, function(x) sum(x)) # aggregate by the age groups we set
denom_all_ages <- aggregate(number ~ location_id + year_id, data=denom_weights, function(x) sum(x)) # get all ages
denom_all_ages$total_number <- denom_all_ages$number

denominator_weights <- join(denom_age_groups, denom_all_ages[,c("location_id","year_id","total_number")], by=c("location_id","year_id"))
write.csv(denominator_weights, paste0(diarrhea_dir, "age_split_denominator_weights.csv"), row.names=F)
##------------------------------------------------------------------------------------------
## Start the etiology loop ##

out_res <- data.frame()
## Build loop here:
for(i in eti_loops){
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]
  etio_rei_id <- eti_info$rei_id[i]
  
  print(paste0("Modeling an age-split for ", name))

  # Pull etiology proportions from last round's best dismod model. Pull age groups corresponding to age_dummy groups.
  if (!i %in% c(2,10)) { 
    etio_global <- get_draws(source = "epi",
                             gbd_id_type = c("modelable_entity_id"),
                             gbd_id = me_id,
                             release_id=9,
                             location_id = 1,
                             year_id=2021,
                             sex_id = 3,
                             age_group_id = age_map_ids$age_group_id,
                             version_id = last_round_models[etiology==name]$model_version_id)
    print(paste0("Finished pulling gbd21 dismod model for ", name))
  } else {
    # Exceptions for rotavirus and salmonella 
    etio_global <- get_draws(source = "epi",
                             gbd_id_type = c("modelable_entity_id"),
                             gbd_id = me_id,
                             release_id=6,
                             location_id = 1,
                             year_id=2019,
                             sex_id = 3,
                             age_group_id = age_map_ids$age_group_id,
                             version_id = last_round_models[etiology==name]$model_version_id)
    print(paste0("Finished pulling gbd19 dismod model for ", name))
  }
  
  # Get age group weights
  total_etio_global <- etio_global[,lapply(.SD,sum), by=c("location_id", "sex_id"), .SDcols=paste0("draw_",0:999)]
  setnames(total_etio_global, paste0("draw_",0:999), paste0("total_",0:999))
  etio_case_weights <- merge(etio_global, total_etio_global)
  etio_case_weights <- etio_case_weights[,paste0("case_weights_",0:999):=
                                           lapply(0:999, function(x){
                                             get(paste0("draw_",x))/get(paste0("total_",x))})]
  
  etio_case_weights[, weights_mean := rowMeans(.SD), .SDcols = (paste0("case_weights_", 0:999))]
  etio_case_weights[, pred_mean := rowMeans(.SD), .SDcols = (paste0("draw_", 0:999))] # get proportion mean
  etio_case_weights[, pred_lower:= apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = (paste0("draw_", 0:999))]
  etio_case_weights[, pred_upper:= apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = (paste0("draw_", 0:999))]
  etio_case_weights$pred_se <- (etio_case_weights$pred_upper - etio_case_weights$pred_lower)/(2*1.96)
  
  etio_case_weights <- etio_case_weights[,-paste0("draw_",0:999)]
  etio_case_weights <- etio_case_weights[,-paste0("total_",0:999)]
  etio_case_weights <- etio_case_weights[,-paste0("case_weights_",0:999)]
  
  etio_case_weights <- merge(etio_case_weights, age_map_ids, by="age_group_id")
  etio_case_weights$age_mid <- (etio_case_weights$age_end + etio_case_weights$age_start) / 2
  setnames(etio_case_weights, 
           c("weights_mean", "age_start","age_end"), 
           c("prop_pred", "age_starts", "age_ends"))
  
  path <- paste0("/FILEPATH/")
  df <- read.csv(paste0(path, "/",name,"_",gbd_year,"_data_corrected_diagnostic_",netid,".csv"))

  cases <- df$mean * df$sample_size
  df$cases <- ifelse(is.na(df$cases), cases, df$cases)

  ################################################################
  ## Pull out age specific data
  ##--------------------------------------------------------------
  df$round_age_start <- round(df$age_start, 1)
  df$round_age_end <- round(df$age_end, 1)
  df$age_sum <- df$age_start + df$age_end

  # Marking which rows need age-splitting
  age_specific <- subset(df, age_end-age_start <25)
  age_specific$cv_age_split <- 0 # cv_age_split is 1 if we need to age split the row, 0 if the row is already granular and we do not need age splitting
  age_non_specific <- subset(df, age_end-age_start >= 25)
  age_non_specific$cv_age_split <- 1
  df <- rbind(age_specific, age_non_specific)

  stopifnot(all(df$crosswalk_parent_seq==df$origin_seq))      
  if(!dir.exists(paste0("/FILEPATH/", gbd_year))){
    dir.create(paste0("/FILEPATH/", gbd_year))
  }

  age_df <- age_split_dismod_weights(df,
                                    case_weights=etio_case_weights,
                                    denominator_weights,
                                    me_name=me_name,
                                    bundle=bundle,
                                    bounded=T,
                                    title=name,
                                    global_merge=T,
                                    date=date)

  stopifnot(all(age_df$cases <= age_df$sample_size,na.rm=T)) 
  stopifnot(all(df$origin_seq != "" & !is.na(df$origin_seq))) 
  age_df$crosswalk_parent_seq <- age_df$origin_seq

  ##----------------------------------------------------------------

  setDT(age_df)
  age_df <- age_df[, -c("note_SR","lat","bm","cause_id","me_id","new","rei_name","raw_mean","raw_standard_error","haqi_cf","acause",
                                                 "short_bundle_name","run_id_get_population","age_group_id_name","unit_adj_denominator","severity","excluded_cases",
                                                 "confirmation_method","unique_group","cv_.","cv.")]
  # This file is used in the Crosswalks Loop #
  age_df <- subset(age_df, group_review != 0)
  write.csv(age_df, paste0("/FILEPATH/",name,"_",gbd_year,"_data_age_split_",netid,".csv"), row.names=F)

  ### Adding age-split data to adjustment summary file
  df_summary <- read.csv(paste0("/FILEPATH/step-by-step_adjustments_",name,"_",date,".csv"))
  df_summary <- subset(df_summary,crosswalk_parent_seq %in% age_df$crosswalk_parent_seq)
  df_summary <- join(select(df_summary,-age_start,-age_end),age_df[,c("crosswalk_parent_seq","seq","mean","sex","age_start","age_end")],by=c("crosswalk_parent_seq","sex"))
  setnames(df_summary,c("mean"),c("age_split"))
  if(nrow(df_summary) != nrow(age_df)){
    stop("Mismatch between adjustment summary file and full dataset.")
  }

  ### Reorganizing the summary data frame
  cols <- c("seq","crosswalk_parent_seq",
            "nid","location_id","location_name","year_start","year_end","age_start","age_end","sex",
            "group_review")
  if(bundle==7){ # we want to include cv_tepec for the EPEC bundle
    cols <- c(cols,"cv_tepec")
  } else if(bundle==8){ # we want to include cv_st_etec for the ETEC bundle
    cols <- c(cols,"cv_st_etec")
  }
  cols <- c(cols,"cv_inpatient","cv_explicit_test","cv_diag_pcr","cases","sample_size","raw_mean")
  if(bundle==7){ # we want to include cv_tepec for the EPEC bundle
    cols <- c(cols,"tepec_adj","tepec_ratio")
  } else if(bundle==8){ # we want to include cv_st_etec for the ETEC bundle
    cols <- c(cols,"st_etec_adj","st_etec_ratio")
  }
  
  cols <- c(cols,"inpatient_adj","inp_beta","explicit_adj","exp_beta")
  if ("pcr_adj" %in% names(df_summary)) {
    cols <- c(cols,,"pcr_adj","pcr_beta")
    
  }
  cols <- c(cols, "age_split")
  # cols <- c(cols,"inpatient_adj","inp_beta","explicit_adj","exp_beta","age_split")
  df_summary <- df_summary[,cols]
  write.csv(df_summary,paste0("/FILEPATH/step-by-step_adjustments_",name,"_",date,".csv"),row.names=F)

  ##-----------------------------------------------------------------------------
  age_df <- age_df[ , -c("cv_explicit","cv_pcr","round_age_start","round_age_end","dummy_age_split","age_sum",
                          "cv_age_split","logit_mean","delta_logit_se", "pred_mean","pred_linear_se","year_id")]
  age_df$group_review[is.na(age_df$group_review)] <- 1
  age_df[is.na(age_df)] <- ""
  age_df$group_review <- ifelse(age_df$specificity=="","",1)
  age_df$uncertainty_type <- ifelse(age_df$lower=="","",as.character(age_df$uncertainty_type))
  age_df$uncertainty_type_value <- ifelse(age_df$lower=="","",age_df$uncertainty_type_value)

  # After age splitting, sometimes cases > sample size
  age_df$cases <- as.numeric(age_df$cases)
  age_df$sample_size <- as.numeric(age_df$sample_size)
  age_df$cases <- ifelse(age_df$cases > age_df$sample_size, age_df$sample_size * 0.99, age_df$cases)

  write.xlsx(age_df, paste0("/FILEPATH/",name,"_",gbd_year,"_age_split.xlsx"), sheetName="extraction")
}

##############################################################################################
## Step 4: Adjust for sensitivity/specificity 
##############################################################################################
if (pcr_new == FALSE) {
  sesp <- read.csv("/FILEPATH/adjustment_matrix_bimodal_used_gbd2019.csv")

  for(i in eti_loops){
    name <- eti_info$name_colloquial[i]
    me_name <- eti_info$modelable_entity[i]
    me_id <- eti_info$modelable_entity_id[i]
    bundle <- eti_info$bundle_id[i]
    rei_name <- eti_info$rei_name[i]

    print(paste0("Adjusting for imperfect sensitivity/specificity for ", name))
    
    df <- read.xlsx(paste0("/FILEPATH/",name,"_",gbd_year,"_age_split.xlsx"))
    df$numeric_mean <- as.numeric(df$mean)
    df$standard_error <- as.numeric(df$standard_error)
    df$crosswalk_mean <- df$mean
    df$linear_floor <- median(df$mean[df$mean>0]) * 0.01

    # Adjust mean and standard error for sensitivity/specificity
    # (from Lang/Reiczigel 2014, "Confidence limits for prevalence of disease adjusted for estimated sensitivity and specificity")
    se <- sesp$mean_sen[which(sesp$bundle_id==bundle)]
    sp <- sesp$mean_spe[which(sesp$bundle_id==bundle)]
    #subset to sensitivity, specificity
    sens_draws <- sesp[which(sesp$bundle_id==bundle),draw_inds(sesp,"sensitivity")[[1]]]
    spec_draws <- sesp[which(sesp$bundle_id==bundle),draw_inds(sesp,"specificity")[[1]]]
    se_var <- sd(sens_draws)^2
    sp_var <- sd(spec_draws)^2

    df$mean <- sapply(1:nrow(df),function(n){
      orig_mean <- df$mean[n]
      adj_mean <- (orig_mean + sp - 1)/(se + sp - 1)
      out_mean <- ifelse(df$cv_diag_pcr[n]==0,adj_mean,orig_mean)
      return(out_mean)
    })

    df$standard_error <- sapply(1:nrow(df),function(n){
      orig_sd <- df$standard_error[n]
      orig_var <- df$standard_error[n]^2
      mean <- df$mean[n]
      adj_sd <- sqrt((orig_var + mean^2*se_var + (1-mean)^2*sp_var)/(se + sp - 1)^2)
      out_sd <- ifelse(df$cv_diag_pcr[n]==0,adj_sd,orig_sd)
      return(adj_sd)
    })
    df$standard_error <- ifelse(df$standard_error > 1, 1, df$standard_error)
    ## Replace mean = 0 values
    df$mean <- ifelse(df$crosswalk_mean==0, 0, df$mean)
    df$mean <- ifelse(df$mean < 0, df$linear_floor, df$mean) # sometimes the adjustment goes outside of the [0,1] range
    df$mean <- ifelse(df$mean > 1, 1-df$linear_floor, df$mean) # means need to be capped to prevent this

    ## Clear lower/upper (will be filled in uploading data)
    df$lower <- ""
    df$upper <- ""
    df$uncertainty_type <- ""
    df$uncertainty_type_value <- ""
    df[is.na(df)] <- ""
    df$mean <- as.numeric(df$mean)
    df$note_modeler <- ifelse(df$cv_diag_pcr==0, paste0(df$note_modeler, "| These data were adjusted for diagnostic sensitivity and specificity of laboratory to molecular diagnostics."), df$note_modeler)
    df <- df[,-(which(names(df) %in% c("crosswalk_mean","age_median","age_bin","linear_floor","lg_mean","delta_log_se")))]

    write.csv(df, paste0("FILEPATH",name,"_",gbd_year,"_data_corrected_diagnostic_",netid,".csv"), row.names=F)

    ### Adding adjustment to summary file
    df_summary <- read.csv(paste0("/FILEPATH/step-by-step_adjustments_",name,"_",date,".csv"))
    df_summary <- right_join(df_summary,df[,c("mean","crosswalk_parent_seq","age_start","age_end","sex")],by=c("crosswalk_parent_seq","age_start","age_end","sex"))
    setnames(df_summary,"mean","sens_spec_adj")
    df_summary$sensitivity <- sesp[which(sesp$bundle_id==bundle),]$mean_sen
    df_summary$specificity <- sesp[which(sesp$bundle_id==bundle),]$mean_spe
    write.csv(df_summary,paste0("/FILEPATH/step-by-step_adjustments_",name,"_",date,".csv"),row.names=F)
  }
}

##############################################################################################
## Step 5: Adjust rotavirus data for vaccine efficacy prior to dismod -----------------------
##############################################################################################

if(run_rota){
  locs <- get_location_metadata(location_set_id=35,release_id=16)
  loc_pull <- locs$location_id[locs$level>=3]
  
  #####################################
  ## Get rotavirus vaccine coverage, SDI ##
  rcov <- get_covariate_estimates(covariate_id=1075, location_id=loc_pull, year_id=1990:2024, release_id=16)
  setnames(rcov, c("mean_value","lower_value","upper_value"), c("rota_cov","rota_cov_lower","rota_cov_upper"))
  
  sdi <- get_covariate_estimates(covariate_id=881, location_id=loc_pull, year_id=1990:2024, release_id=16)
  setnames(sdi, c("mean_value","lower_value","upper_value"), c("sdi","sdi_lower","sdi_upper"))
  
  #############################################################################################
  ## Load in the file for the predicted values of rotavirus VE.
  #############################################################################################
  rota_ve <- fread("/FILEPATH/rota_ve_location_year_with_doses_covariate.csv")
  setnames(rota_ve, c("rota_ve_mean", "rota_ve_lower", "rota_ve_upper"), c("pred_ve", "pred_lb", "pred_ub"))
  rota_ve$pred_se <- (rota_ve$pred_ub - rota_ve$pred_lb) / (2*1.96)
    
  #########################################################
  # Using data adjusted for sensitivity/specificity and age-sex split
  df <- read.csv(paste0("/FILEPATH/Rotavirus_",gbd_year,"_data_corrected_diagnostic_",netid,".csv"))
  df_save <- df
  df <- subset(df, !is.na(nid))
  
  df$year_id <- floor((df$year_start + df$year_end)/2)
  df$year_id <- ifelse(df$year_id < 1990, 1990, df$year_id)
  
  df <- join(df, rcov[,c("location_id","year_id","rota_cov","rota_cov_lower","rota_cov_upper")], by=c("location_id","year_id"))
  df <- join(df, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
  
  df <- join(df, rota_ve[,c("location_id","year_id","pred_ve","pred_se","pred_lb","pred_ub")], by=c("location_id","year_id"))
  
  df <- subset(df, !is.na(pred_ve))
  
  ## Determine the impact of the vaccine ##
  df$rota_se <- (df$rota_cov_upper - df$rota_cov_lower) / 2 / qnorm(0.975)
  df$impact <- 1 - df$rota_cov * df$pred_ve
  df$se_impact <- sqrt(with(df, rota_se^2 * pred_se^2 + rota_se^2*pred_ve^2 + rota_cov^2*pred_se^2))
  
  df$mean_adj_ve <- df$mean / df$impact
  df$impact_mean <- 1/df$impact
  df$se_adj_ve <- sqrt(with(df, standard_error^2 * se_impact^2 + standard_error^2*impact_mean^2 + mean^2*se_impact^2))
  
  # If the age_start < 5 and vaccine coverage > 0, some children
  # will receive the vaccine and those data should be adjusted
  # as if there was no vaccine.
  
  df$mean <- ifelse(df$age_start < 5, df$mean_adj_ve, df$mean)
  df$standard_error <- ifelse(df$age_start < 5 & df$rota_cov > 0, df$se_adj_ve, df$standard_error)
  
  df$mean <- ifelse(df$mean >= 1, 0.99, df$mean)
  df$standard_error <- ifelse(df$standard_error > 1, 0.99, df$standard_error)
  
  ## Remove some columns, save
  df <- subset(df, !is.na(mean))
  
  df_save2 <- df[,names(df_save)] 
  write.csv(df_save2, "/FILEPATH/Rotavirus_gbd2023_VE_adjusted_2023.csv")
  
  ### Adding adjustment to summary file
  df$age_start <- signif(df$age_start,3) # fixing a rounding issue in the age_end column
  df$age_end <- signif(df$age_end,3)
  
  df_summary <- read.csv(paste0("/FILEPATH/step-by-step_adjustments_Rotavirus_",date,".csv"))
  df_summary$age_start <- signif(df_summary$age_start,3)
  df_summary$age_end <- signif(df_summary$age_end,3)
  df_summary <- right_join(df_summary,df[,c("mean","rota_cov","pred_ve","crosswalk_parent_seq","age_start","age_end","sex")],by=c("crosswalk_parent_seq","age_start","age_end","sex"))
  setnames(df_summary,c("mean","rota_cov","pred_ve"),c("rota_ve_adj","vaccine_coverage","vaccine_efficacy"))
  write.csv(df_summary,paste0("/FILEPATH/step-by-step_adjustments_Rotavirus_",date,".csv"),row.names=F)
}



##############################################################################################
## Step 6: Apply basic outliering  ----------------------------------------------------------
##############################################################################################
cutoffs_df <- read.csv("/FILEPATH/cutoffs.csv") 

for(i in eti_loops){
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  bundle <- eti_info$bundle_id[i]
  bundle_version_id <- vers$bundle_version_id[which(vers$bundle_id==bundle)]
  print(paste0("Applying outliering to ", name))
  
  # load data
  bv <- get_bundle_version(bundle_version_id)
  
  if(bundle==12){
    df <- read.csv(paste0("/FILEPATH/",name,"_gbd2023_VE_adjusted_2023.csv"))
  } else{
    df <- read.csv(paste0("/FILEPATH/",name,"_",gbd_year,"_data_corrected_diagnostic_",netid,".csv"))
  }
  
  # Make sure all nids from the bundle version are in the crosswalk version
  if(FALSE %in% unique(df$nid %in% bv$nid)){
    stop("Some nids from the bundle version are missing in the crosswalk version. These are the nids:")
    print(unique(bv[!nid %in% df$nid]$nid))
  }
  
  # apply threshold-based outliering
  if(bundle!=5){
    max_mean <- max(subset(bv,is_outlier!=1&sample_size>=10)$mean) 
    max_nid <- subset(bv,is_outlier!=1&sample_size>=10)$nid[which.max(subset(bv,is_outlier!=1&sample_size>=10)$mean)]
  } else{
    max_mean <- quantile(df$mean,0.925) 
    max_nid <- NA
  }
  df$is_outlier <- ifelse(df$mean > max_mean,1,df$is_outlier)
  
  if (!skip_outliers) {
    # apply manual outliers from spreadsheet
    outliers <- read.xlsx("/FILEPATH/Diarrhea Etiologies - GBD 2023 DisMod Final Outliers.xlsx",sheet=eti_info$name_colloquial[i])
    outliers <- subset(outliers, Outlier==1)
    for(n in 1:nrow(outliers)){
      if(nrow(outliers)==0){
        break #skip if outliers spreadsheet is empty
      }
      inds <- which(df$nid==outliers$nid[n] &
                      df$sex==outliers$sex[n] &
                      df$crosswalk_parent_seq==outliers$crosswalk_parent_seq[n] &
                      round(df$age_start,3)==round(outliers$age_start[n],3) &
                      round(df$age_end,3)==round(outliers$age_end[n],3))
      if(length(inds) != 1){stop("outliering spreadsheet not one-to-one with crosswalk version")} # assumption: each row in the outliering spreadsheet should correspond to a single row in the crosswalk version
      df$is_outlier[inds] <- outliers$Outlier[n]
    }
  }
  
  write.csv(df,paste0("/FILEPATH/",name,"_outliered.csv"),row.names=F)
  
  ## Save outliering thresholds
  d <- Sys.Date() %>% as.Date(origin="1970-01-01") %>% as.character 
  cutoffs_df[which(cutoffs_df$bundle_id==bundle),]$date <- d
  cutoffs_df[which(cutoffs_df$bundle_id==bundle),]$bundle_version_id <- bundle_version_id
  cutoffs_df[which(cutoffs_df$bundle_id==bundle),]$cutoff <- max_mean
  cutoffs_df[which(cutoffs_df$bundle_id==bundle),]$NID <- max_nid
  print("Outliers applied!")
}

#############################################################################
####### UPLOAD ####################
for(i in eti_loops){
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  m_name <- eti_info$modelable_entity_name[i]
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]
  bundle_version_id <- vers$bundle_version_id[which(vers$bundle_id==bundle)]
  print(paste0("Uploading modified data for ", name))
  out <- fread(paste0("/FILEPATH/",name,"_outliered.csv"))
  out[recall_type == "Period: weeks" & is.na(recall_type_value), recall_type:="Point"] 
  out <- data.frame(out)
  if(bundle==12){
   # Run validation tests on adjustments summary file
   colname <- "rota_ve_adj"
   df_summary <- read.csv(paste0("/FILEPATH/step-by-step_adjustments_",name,"_",date,".csv"))
   if(any(is.na(df_summary[,colname]))){
     stop("NAs in adjustment summary file.")
   } else if(nrow(df_summary) != nrow(out)){
     stop("Mismatch between adjustment summary file and upload dataset.")
   }
   
   # Clean up and upload
   out$note_sr <- gsub(pattern="\"\"\"",replacement="",x=out$note_sr) 
   out$site_memo <- gsub(pattern="\"\"\"",replacement="",x=out$site_memo)
   out$case_diagnostics <- gsub(pattern="\"\"\"",replacement="",x=out$case_diagnostics)
   out$effective_sample_size <- ifelse(out$effective_sample_size < out$cases, NA, out$effective_sample_size)
   if("all_note" %in% names(out)){out$all_note <- gsub(pattern="\"\"\"",replacement="",x=out$all_note)}
   write.xlsx(out,paste0("FILEPATH",name,"_gbd2023_VE_adjusted_2023.xlsx"),sheetName="extraction")
   
   # Check for inpatient crosswalk file - if missing, crosswalk was not run and df should not be uploaded.
   fps <- list.files(paste0("FILEPATH/gbd_", gbd_year, "/cv_inpatient/"), full.names=T)
   inp_latest <- fps[grep(pkl_date,fps)]
   if(save_crosswalks & length(inp_latest) > 0){
     result <- save_crosswalk_version(bundle_version_id = bundle_version_id,
                            data_filepath = paste0("/FILEPATH/",name,"_gbd2023_VE_adjusted_2023.xlsx"),
                            description = paste0("DESCRIPTION, outlier threshold=",round(max_mean,3),"; bundle_version_id=",bundle_version_id,";"))
     new_xwalks[bundle_id==bundle, `:=`(bv_id=bundle_version_id, crosswalk_version_id=result$crosswalk_version_id)]
   } else if (save_crosswalks & length(inp_latest == 0)) {
     stop(paste0("The inpatient crosswalk was not run for ", name, ". Crosswalk not uploaded."))
   }
  } else{
   # Run validation tests on adjustments summary file
   # colname <- "pcr_adj"
    colname <- "sens_spec_adj"
   df_summary <- read.csv(paste0("FILEPATH/step-by-step_adjustments_",name,"_",date,".csv"))
   if(any(is.na(df_summary[,colname]))){
     stop("NAs in adjustment summary file.")
   } else if(nrow(df_summary) != nrow(out)){
     stop("Mismatch between adjustment summary file and upload dataset.")
   }

   # Clean up and upload
   if("note_sr" %in% names(out)){out$note_sr <- gsub(pattern="\"\"\"",replacement="",x=out$note_sr)} 
   out$site_memo <- gsub(pattern="\"\"\"",replacement="",x=out$site_memo) 
   out$case_diagnostics <- gsub(pattern="\"\"\"",replacement="",x=out$case_diagnostics)
   if("all_note" %in% names(out)){out$all_note <- gsub(pattern="\"\"\"",replacement="",x=out$all_note)}
   out$specificity <- as.character(out$specificity)
   out$specificity <- ifelse(is.na(out$group)&is.na(out$group_review),NA,out$specificity)
   out$group_review <- ifelse(is.na(out$group)&(out$specificity==""|is.na(out$specificity)),NA,out$group_review) 
   out$effective_sample_size <- ifelse(out$effective_sample_size < out$cases, NA, out$effective_sample_size)
   out$upper <- ifelse(out$lower > out$cases | out$lower > out$mean, NA, out$upper)
   out$lower <- ifelse(out$lower > out$cases | out$lower > out$mean, NA, out$lower)
   out$uncertainty_type_value <- ifelse(is.na(out$lower), NA, out$uncertainty_type_value)
   write.xlsx(out,paste0("/FILEPATH/",name,"_",gbd_year,"_data_corrected_diagnostic_",netid,".xlsx"),sheetName="extraction")
   
   # Check for inpatient crosswalk file - if missing, crosswalk was not run and df should not be uploaded.
   fps <- list.files(paste0("/FILEPATH/gbd_", gbd_year, "/cv_inpatient/"), full.names=T)
   inp_latest <- fps[grep(pkl_date,fps)]
   if(save_crosswalks & length(inp_latest) > 0){
     result <- save_crosswalk_version(bundle_version_id = bundle_version_id,
                            data_filepath = paste0("/FILEPATH/",name,"_",gbd_year,"_data_corrected_diagnostic_",netid,".xlsx"),
                            description = paste0("DESCRIPTION, outlier threshold=","; bundle_version_id=",bundle_version_id,"; fixed pcr coefs"))
     new_xwalks[bundle_id==bundle, `:=`(bv_id=bundle_version_id, crosswalk_version_id=result$crosswalk_version_id)]
   } else if (save_crosswalks & length(inpatient_latest) == 0) {
     stop(paste0("The inpatient crosswalk was not run for ", name, ". Crosswalk not uploaded."))
    }
  }
  print("Uploaded PCR Se/Sp adjusted iterative")
  write.csv(new_xwalks, out_xwalk_ids, row.names=F)
}

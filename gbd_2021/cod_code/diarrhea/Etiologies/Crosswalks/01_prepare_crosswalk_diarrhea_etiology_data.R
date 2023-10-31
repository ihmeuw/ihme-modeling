#######################################################################################
# The file is intended to be a single place to perform all data adjustments for the
# diarrheal etiologies that depend on GEMS/MALED for adjustments.
# 
# The intention for this code is to prepare the etiology data for diarrhea
# so that they are ready for modeling. This includes:
#
# - pulling and saving data, 
# - creating dataframes for running out of DisMod crosswalks for explicit etiology testing and inpatient sample
# populations
# - finally, adjusting for imperfect sensitivity and specificity.
# For ETEC and EPEC, we will also adjust these data such that they are representing
# ST- and typical serotypes, only. For Norovirus, we also adjust for high-risk
# of diagnostic bias (gastroenteritis) and for the use of PCR as the diagnostic (MRBRT).
######################################################################################
save_crosswalks <- TRUE
run_etec_epec <- FALSE
run_rota <- FALSE

os <- .Platform$OS.type
if (os == "windows") {
  source("FILEPATH")
} else {
  source("FILEPATH")
}

windows <- Sys.info()[1][["sysname"]]=="Windows"
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
config <- read.csv(paste0("FILEPATH"))

# Prepare your needed functions and information
library(metafor)
library(msm)
library(openxlsx)
library(matrixStats)
library(plyr)
library(dplyr)
library(boot)
library(data.table)
library(ggplot2)
library(openxlsx)
library(data.table)
library(scales)
library(viridis)

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
invisible(sapply(list.files("FILEPATH", full.names = T), source))


date <- format(Sys.Date(), format="%m_%d_%Y") # this is used for printing age_splitting plots
pkl_date <- format(Sys.Date(), format="%Y-%m-%d") # this is used to check the existence of inpatient crosswalk pkl files.

locs <- data.frame(get_location_metadata(location_set_id=35,gbd_round_id=7,decomp_step="iterative"))

eti_info <- read.csv("FILEPATH")
eti_info <- subset(eti_info, model_source=="dismod" & rei_id!=183 & cause_id==302)
rownames(eti_info)<-1:nrow(eti_info) 

# Pull bundle version sheet
vers <- read.xlsx("FILEPATH")
setDT(vers)

eti_info <- join(eti_info, vers, by = "bundle_id")

# Set gbd year
gbd_year <- 2020


# DisMod values #
dmvs <- read.csv("FILEPATH")

dmvs_full <- read.csv("FILEPATH")

# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2

## Find Saudi Arabia subnationals, England areas (like Northwest, West Midlands) to remove ##
locs_old <- read.csv("FILEPATH")
bad_ids <- c(locs_old$location_id[locs_old$parent_id==152], locs_old$location_id[locs_old$parent_id==73 & locs_old$most_detailed==0])
bad_ids <- ifelse(bad_ids==93,95,bad_ids)
bad_ids <- bad_ids[which(bad_ids != 95)]

## Load in information for sex splitting ##
diar_sex_weights <- get_outputs(topic="cause", cause_id=302, age_group_id=22, year_id=2019, gbd_round_id=6, measure_id=6, sex_id=c(1,2), location_id=1, metric_id=3,decomp_step="step5",compare_version_id=ID)
diar_sex_weights$number <- diar_sex_weights$val

gbd_round_id <- 9 ## ----> set gbd round for reference in functions and naming convention
gbd_round <- 'gbd2020' ##------> set for file naming 

eti_loops <- 12 #2:13

# Create table to save new crosswalk versions if save_crosswalks=TRUE.
if (save_crosswalks) {
  new_xwalks <- data.table(bundle_id=eti_info[eti_loops,]$bundle_id, bv_id=integer(), crosswalk_version_id=integer())
  out_xwalk_ids <- "FILEPATH"
}

gems_maled <- fread("FILEPATH")

##############################################################################################
## Step 1.1: Pull and save decomp step 1/2 data. Should only be done once!
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
  
  # Some data are too old. This is a good opportunity to only model newer data.
  # Chose 1986 because default time window in DisMod is 5 years (1990-4)
  df <- subset(df, year_end >= 1986)
  
  # Remove MALED nonPCR for campylobacter because enzyme immunoassay was used, which overestimates campylobacter
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
  write.csv(df, paste0("FILEPATH"), row.names=F)
}

##############################################################################################
## Step 1.2: Fill out cv_diag_pcr. The starting point is appending all data
## so that there is some ability to find and populate a new column cv_diag_pcr
##############################################################################################
pcr_nids <- c("ID")

out_df <- data.frame()
for(i in eti_loops){
  
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]
  
  print(paste0("Pulling and appending for ", name))
  in_df <- read.csv(paste0("FILEPATH"))
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
  
  write.csv(in_df, paste0("FILEPATH"), row.names=F)
  
  out_df <- rbind.fill(out_df, in_df)
  
  ### Save a step-by-step breakdown of the adjustment process for each row
  ### This file will be updated each time an adjustment is made
  meta_cols <- c("seq","nid","location_id","location_name","year_start","year_end","age_start","age_end","sex","group_review","cv_inpatient","cv_explicit_test","cv_diag_pcr")
  if(bundle==7){ # we want to include cv_tepec for the EPEC bundle
    meta_cols <- c(meta_cols,"cv_tepec")
  } else if(bundle==8){ # we want to include cv_st_etec for the ETEC bundle
    meta_cols <- c(meta_cols,"cv_st_etec")
  }
  
  df_summary <- data.frame(in_df)[,meta_cols]
  df_summary$cases <- in_df$cases
  df_summary$sample_size <- in_df$sample_size
  df_summary$raw_mean <- in_df$mean
  write.csv(df_summary,paste0("FILEPATH"),row.names=F)
}

##############################################################################################
##-- Step 1.3: Adjust typical EPEC and ST-ETEC  ----------------------------------------------
##############################################################################################
if(run_etec_epec){
  ## tEPEC ratio ##
  # This file is used in the Crosswalks Loop #
  df <- read.csv(paste0("FILEPATH"))
  
  # cv_tepec validations
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
  epec_pcr_ratio <- fread("FILEPATH")
  epec_pcr_ratio$se <- (epec_pcr_ratio$beta_upper - epec_pcr_ratio$beta_lower)/(2*1.96)
  epec_non_pcr_ratio <- fread("FILEPATH")
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
  
  write.csv(df, "FILEPATH")
  
  ### Adding adjustment to summary file
  df$group_review <- ifelse(df$group_review=="",NA,df$group_review)
  df_summary <- read.csv(paste0("FILEPATH"))
  df_summary$group_review <- as.integer(df_summary$group_review)
  df$group_review <- as.integer(df$group_review)
  df_summary <- right_join(df_summary,df[,c("mean","pred_ratio","seq","age_start","age_end","group_review")],by=c("seq","age_start","age_end","group_review"))
  setnames(df_summary,c("mean","pred_ratio"),c("tepec_adj","tepec_ratio"))
  write.csv(df_summary,paste0("FILEPATH"),row.names=F)
  
  ############################################################################
  ## ST-ETEC ratio ##
  
  df <- read.csv("FILEPATH")
  
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
  etec_pcr_ratio <- fread("FILEPATH")
  etec_pcr_ratio$se <- (etec_pcr_ratio$beta_upper - etec_pcr_ratio$beta_lower)/(2*1.96)
  etec_non_pcr_ratio <- fread("FILEPATH")
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
  
  write.csv(df, "FILEPATH")
  
  ### Adding adjustment to summary file
  df$group_review <- ifelse(df$group_review=="",NA,df$group_review)
  df_summary <- read.csv(paste0("FILEPATH"))
  df_summary$group_review <- as.integer(df_summary$group_review)
  df$group_review <- as.integer(df$group_review)
  df_summary <- right_join(df_summary,df[,c("mean","pred_ratio","seq","age_start","age_end","group_review")],by=c("seq","age_start","age_end","group_review"))
  setnames(df_summary,c("mean","pred_ratio"),c("st_etec_adj","st_etec_ratio"))
  write.csv(df_summary,paste0("FILEPATH"),row.names=F)
}
##############################################################################################
## Step 2: Run crosswalks in MR-BRT ---------------------------------------------------------
## This is also where data is sex-split
##############################################################################################
## Set the type, must be "log" or "logit"
xw_transform <- "logit"

out_res <- data.frame()
## Build loop here:
for(i in eti_loops){
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]
  
  print(paste0("Modeling an MR-BRT crosswalk for ", name))
  
  df <- read.csv(paste0("FILEPATH"))
  
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
  
  # Pull out new data, duplicate sexes (can't upload sex="Both"), remove group_review = 0, and save in the Upload folder
  df$is_sexsplit <- ifelse(df$sex=="Both",1,0)
  df <- scalar_sex_rows(df, denominator_weights=diar_sex_weights, bounded=T, global_merge=T, title=name)
  
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
  
  cv_inpatient <- bundle_crosswalk_collapse(df, ref_name="is_reference", alt_name="inpatient", age_cut=c(0,1,2,5,20,40,60,80,100), year_cut=c(seq(1980,2015,5),2020), merge_type="within", location_match="exact", include_logit = T)
  
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
  
  library(crosswalk002, lib.loc = "FILEPATH")
  
  df1 <- CWData(
    df = cv_inpatient,          # dataset for metaregression
    obs = "logit_ratio",       # column name for the observation mean
    obs_se = "logit_ratio_se", # column name for the observation standard error
    alt_dorms = "altvar",     # column name of the variable indicating the alternative method
    ref_dorms = "refvar",     # column name of the variable indicating the reference method
    study_id = "nid",    # name of the column indicating group membership, usually the matching groups
    add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
  )
  
  fit_inpatient <- CWModel(
    cwdata = df1,            # object returned by `CWData()`
    obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
    cov_models = list(       # specifying predictors in the model; see help(CovModel)
      CovModel(cov_name = "intercept")),
    # CovModel(cov_name = "x1"),
    # CovModel(cov_name = "x1", spline = XSpline(knots = c(0,1,2,3,4), degree = 3L, l_linear = TRUE, r_linear = TRUE), spline_monotonicity = "increasing"),
    # CovModel(cov_name = "x2") ),
    max_iter = 1000L,
    gold_dorm = "non-inpatient"   # the level of `alt_dorms` that indicates it's the gold standard
    # this will be useful when we can have multiple "reference" groups in NMA
  )
  
  ### save fit to folder
  #make folder, if necessary
  output_dir <- paste0("FILEPATH")
  dir.create(paste0("FILEPATH"), recursive=TRUE)
  
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
    #can loaded again using py_load_object in the reticulate package
    py_save_object(object = fit_inpatient, filename = paste0(output_dir, suffix,".pkl"), pickle = "dill")
    df_result <- fit_inpatient$create_result_df()
    write.csv(df_result, paste0("FILEPATH"), row.names=F)
    
  }
  
  preds1 <- adjust_orig_vals( 
    fit_object = fit_inpatient, # object returned by `CWModel()`
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
  df_summary <- read.csv(paste0("FILEPATH"))
  df_summary$group_review <- ifelse(is.na(df_summary$group_review),"",df_summary$group_review)
  df_summary$group_review[is.na(df_summary$group_review)] <- 1 
  df_summary <- subset(df_summary, group_review != 0)
  df_summary <- subset(df_summary, seq %in% df$crosswalk_parent_seq)
  setnames(df_summary,"seq","crosswalk_parent_seq")
  df_summary <- right_join(df_summary %>% select(-sex),df[,c("mean","crosswalk_parent_seq","age_start","age_end","group_review","sex")],by=c("crosswalk_parent_seq","age_start","age_end","group_review"))
  setnames(df_summary,c("mean"),c("inpatient_adj"))
  df_summary$inp_beta <- fit_inpatient$beta[1]
  write.csv(df_summary,paste0("FILEPATH"),row.names=F)
  
  ##-------------------------------------------------------------------------------------------------
  #### Explicit testing ####
  df$is_reference <- ifelse(df$cv_explicit_test==1,0,1)
  
  # Make sure new data aren't used!
  if(exists("cv_explicit")){rm(cv_explicit)}
  try(
    cv_explicit <- bundle_crosswalk_collapse(df, alt_name="cv_explicit_test",ref_name="is_reference", age_cut=c(0,1,2,5,20,40,60,80,100), year_cut=c(seq(1980,2015,5),2020),
                                             merge_type="between", location_match="exact", include_logit = T),
    silent=T
  )
  
  if(exists("cv_explicit")){
    if(i==5) { 
      cv_explicit <- subset(cv_explicit,!(age_bin=="(40,60]"&ref_nid %in% IDS &alt_nid %in% IDS))
      cv_explicit <- subset(cv_explicit,!(ref_nid==ID & alt_nid==ID)) # Dropping this for not matching exactly on age_start and age_end
    }
    if(i==4) { 
      cv_explicit <- subset(cv_explicit,!(age_bin=="(2,5]"&ref_nid %in% IDS &alt_nid %in% IDS))
    }
    if(i==1){
      cv_explicit <- subset(cv_explicit,!(ref_nid==ID &alt_nid==ID)) # Dropping this for not matching on location (different thailand subnationals)
    }
    if(i==11){
      cv_explicit <- subset(cv_explicit,!((ref_nid %in% IDS &alt_nid %in% IDS))) # Dropping this for not matching exactly on age_start and age_end or location
    }
    if(i==10){
      cv_explicit <- subset(cv_explicit,!((ref_nid==ID&alt_nid==ID))) # Dropping this for not matching exactly on age_start and age_end
    } 
    if(i==9){
      cv_explicit <- subset(cv_explicit,!((ref_nid %in% IDS &alt_nid %in% IDS))) # Dropping this for not matching exactly on age_start and age_end or location
    }
    if(i==8){
      cv_explicit <- subset(cv_explicit,!((ref_nid==ID&alt_nid==ID))) # Dropping this for not matching exactly on age_start and age_end. This is the only matched pair for norovirus.
    }
    if(i==7){
      cv_explicit <- subset(cv_explicit,!((ref_nid %in% IDS&alt_nid %in% IDS))) # Dropping this for not matching exactly on age_start and age_end
    }
    cv_explicit <- subset(cv_explicit,age_bin != "(0,20]") # quick patch: excluding 0-20 age bin because the younger and older age groups aren't comparable
  } else{
    cv_explicit <- data.frame()
    print(paste0("Multipathogen crosswalk not run for ", name, " due to lack of matched pairs."))
  }
  
  if(nrow(cv_explicit) != 0){ 
    cv_explicit$location_match <- ifelse(cv_explicit$location_match=="","Other",cv_explicit$location_match)
    cv_explicit$match_nid <- paste0(cv_explicit$ref_nid,"_",cv_explicit$alt_nid)
    
    # After age-splitting, we might have multiple rows of same values
    cv_explicit$unique <- paste0(cv_explicit$match_nid, "_", round(cv_explicit$ratio,5))
    cv_explicit <- subset(cv_explicit, !duplicated(unique))
    
    cv_explicit$altvar <- "explicit"
    cv_explicit$refvar <- "multi-pathogen"
    
    df2 <- CWData(
      df = cv_explicit,          # dataset for metaregression
      obs = "logit_ratio",       # column name for the observation mean
      obs_se = "logit_ratio_se", # column name for the observation standard error
      alt_dorms = "altvar",     # column name of the variable indicating the alternative method
      ref_dorms = "refvar",     # column name of the variable indicating the reference method
      study_id = "match_nid",    # name of the column indicating group membership, usually the matching groups
      add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
    )
    
    fit_explicit <- CWModel(
      cwdata = df2,            # object returned by `CWData()`
      obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
      cov_models = list(       # specifying predictors in the model; see help(CovModel)
        CovModel(cov_name = "intercept")),
      # CovModel(cov_name = "x1"),
      # CovModel(cov_name = "x1", spline = XSpline(knots = c(0,1,2,3,4), degree = 3L, l_linear = TRUE, r_linear = TRUE), spline_monotonicity = "increasing"),
      # CovModel(cov_name = "x2") ),
      gold_dorm = "multi-pathogen"   # the level of `alt_dorms` that indicates it's the gold standard
      # this will be useful when we can have multiple "reference" groups in NMA
    )
    
    if(save_crosswalks){
      ### save fit to folder
      #make folder, if necessary
      output_dir <- paste0("FILEPATH")
      
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
      #can loaded again using py_load_object in the reticulate package
      py_save_object(object = fit_explicit, filename = paste0(output_dir, suffix,".pkl"), pickle = "dill")
      df_result <- fit_explicit$create_result_df()
      write.csv(df_result, paste0("FILEPATH"), row.names=F)
    }
    
    df$linear_floor <- median(df$mean[df$mean>0]) * 0.01
    df$mean <- ifelse(df$mean<=0, df$linear_floor, df$mean)
    df$mean <- ifelse(df$mean>=1, 1-df$linear_floor, df$mean) #offsetting some mean=1 and mean=0 values, since they need to be logit-transformed
    
    df$obs_method <- ""
    df[which(df$cv_explicit_test==1),"obs_method"] <- "explicit"
    df[which(df$cv_explicit_test!=1),"obs_method"] <- "multi-pathogen"
    
    preds1 <- adjust_orig_vals( 
      fit_object = fit_explicit, # object returned by `CWModel()`
      df = df,
      orig_dorms = "obs_method",
      orig_vals_mean = "mean",
      orig_vals_se = "standard_error",
      data_id = "rownum"   # optional argument to add a user-defined ID to the predictions;
      # name of the column with the IDs
    )
    df$mean <- preds1$ref_vals_mean
    df$standard_error <- preds1$ref_vals_sd
  } else{
    df <- subset(df,cv_explicit_test!=1) # if there is no multipathogen crosswalk, we can't include single-pathogen studies
  }
  
  df$cases <- df$mean*df$sample_size # cases should be adjusted after crosswalk, for age-splitting purposes
  df <- subset(df, nid != 999999)
  
  ## Save the modified file ##
  df$note_modeler <- paste0(df$note_modeler, " | The crosswalks for cv_inpatient and cv_explicit were performed using MR-BRT. Inpatient data were
                            merged based on within study observations while explicit data had to be merged to nearby data based on location.")
  
  
  write.csv(df, paste0("FILEPATH"), row.names=F)
  
  setnames(df,"note_SR","note_sr",skip_absent = T)
  df <- df[ , -which(names(df) %in% c("cv_explicit","cv_pcr","round_age_start","round_age_end","dummy_age_split",
                                      "cv_age_split","logit_mean","delta_logit_se", "pred_mean","pred_linear_se","year_id",
                                      "age_sum","inpatient","is_reference","log_inpatient","se_log_inpatient",
                                      "mrbrt_inpatient","mrbrt_se_inpatient","mrbrt_explicit","mrbrt_se_explicit","adjustment_type"))]
  df[is.na(df)] <- ""
  df$crosswalk_parent_seq <- ifelse(df$crosswalk_parent_seq=="",df$seq,df$crosswalk_parent_seq)
  df$uncertainty_type <- ifelse(df$lower=="","", as.character(df$uncertainty_type))
  df$uncertainty_type_value <- ifelse(df$lower=="","",df$uncertainty_type_value)
  
  # After Crosswalking, resetting cases, sometimes cases > sample size
  df$cases <- as.numeric(df$cases)
  df$sample_size <- as.numeric(df$sample_size)
  df$cases <- ifelse(df$cases > df$sample_size, df$sample_size * 0.99, df$cases)
  
  df$standard_error <- ifelse(df$standard_error > 1, 0.99, df$standard_error)
  
  df <- subset(df, !(location_id %in% bad_ids))
  
  write.csv(df, paste0("FILEPATH"), row.names=F)
  
  ### Adding adjustment to summary file
  df$group_review <- ifelse(df$group_review=="",NA,df$group_review) %>% as.numeric
  df_summary <- read.csv(paste0("FILEPATH"))
  df_summary$group_review <- as.numeric(df_summary$group_review)
  df_summary <- right_join(df_summary,df[,c("mean","crosswalk_parent_seq","age_start","age_end","group_review","sex")],by=c("crosswalk_parent_seq","age_start","age_end","group_review","sex"))
  setnames(df_summary,c("mean"),c("explicit_adj"))
  if(exists("fit_explicit")){
    df_summary$exp_beta <- fit_explicit$beta[1]
  } else{
    df_summary$exp_beta <- NA
  }
  df_summary <- subset(df_summary, year_start >= 1985)
  write.csv(df_summary,paste0("FILEPATH"),row.names=F)
}

##############################################################################################
## Step 3: These data need to be age split. Use an MR-BRT age curve.
##############################################################################################
## Determine where age splitting should occur. ##
age_map <- read.csv("FILEPATH")
age_map <- age_map[age_map$age_pull==1,c("age_group_id","age_start","age_end","order","age_pull")]
# age_cuts <- c(0,1,5,20,40,60,80,100)
age_cuts <- c(0,1,5,20,40,65,100)
length <- length(age_cuts) - 1

age_map$age_dummy <- cut(age_map$age_end, age_cuts, labels = paste0("Group ", 1:length))

## Create prevalence weights to age-split the data from the global diarrhea prevalence age pattern in GBD 2019 ##
dmod_global <- get_outputs(topic="cause", cause_id=302, age_group_id = age_info$age_group_id, year_id=1990:2020, location_id=1, sex_id=3, measure_id=5, metric_id=1, gbd_round_id=6, decomp_step = "step5", compare_version_id=ID)
dmod_global$number <- dmod_global$val
denom_weights <- dmod_global
denom_weights <- join(denom_weights, age_map[,c("age_group_id","age_dummy","order")], by="age_group_id")

denom_age_groups <- aggregate(number ~ location_id + year_id + age_dummy, data=denom_weights, function(x) sum(x))
denom_all_ages <- aggregate(number ~ location_id + year_id, data=denom_weights, function(x) sum(x))
denom_all_ages$total_number <- denom_all_ages$number

denominator_weights <- join(denom_age_groups, denom_all_ages[,c("location_id","year_id","total_number")], by=c("location_id","year_id"))

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
  
  print(paste0("Modeling an age-split for ", name))
  
  df <- read.csv(paste0("FILEPATH"))
  
  cases <- df$mean * df$sample_size
  df$cases <- ifelse(is.na(df$cases), cases, df$cases)
  
  ################################################################
  ## Pull out age specific data
  ##--------------------------------------------------------------
  ## Three different ways to identify non-age-specific data rows
  # Part 1, Test a dummy variable where age_start/end rounded doesn't equal age_start/end
  df$round_age_start <- round(df$age_start, 1)
  df$round_age_end <- round(df$age_end, 1)
  df$age_sum <- df$age_start + df$age_end
  
  # Marking which rows need age-splitting
  df$dummy_age_split <- ifelse(df$age_end-df$age_start >= 25,1,0)
  
  age_specific <- subset(df, dummy_age_split == 0)
  age_non_specific <- subset(df, dummy_age_split == 1)
  
  # Some age-split data have already been duplicated, all rows should be removed
  age_specific$cv_age_split <- 0
  age_non_specific$cv_age_split <- 1
  
  ##------------------------------------------------------
  
  df <- rbind(age_specific, age_non_specific)
  
  ## Guidance is to age split any data more than 25 year span
  df$cv_age_split <- ifelse(df$age_end - df$age_start > 25, 1, df$cv_age_split)
  
  ##---------------------------------------------------------------
  # Age splitting function: It takes
  # an estimated ratio of the observed to expected data for all age-specific
  # data points and runs that through MR-BRT. The result is then predicted
  # for six age groups which are then merged 1:m with the age-non-specific
  # data to make six new points with the new proportion equal to the
  # original mean times the modeled ratio. It keeps a set of rows
  # from the age-non-specific data to mark as group_review = 1.
  
  stopifnot(all(df$crosswalk_parent_seq==df$origin_seq))      
  # to make the crosswalk_parent_seq reassignment work
  if(!dir.exists(paste0("FILEPATH"))){
    dir.create(paste0("FILEPATH"))
  }
  
  if(i %in% c(11,12,13)){ # we prefer to use GBD 2019 age curves, if possible
    age_df <- age_split_mrbrt_weights(df, denominator_weights, output_dir=paste0("FILEPATH"), bounded=T, title=name, global_merge=T, use_old_model=F)
    print_age_splitting_plots(me_name, date, use_old_model=FALSE, user, title=name)
  } else{
    age_df <- age_split_mrbrt_weights(df, denominator_weights, output_dir=paste0("FILEPATH"), bounded=T, title=name, global_merge=T, use_old_model=T)
    print_age_splitting_plots(me_name, date, use_old_model=TRUE, user, title=name)
  }
  
  stopifnot(all(age_df$cases <= age_df$sample_size,na.rm=T)) # in extreme cases, we can end up with cases > sample size after age-splitting (which is not uploadable)
  stopifnot(all(df$origin_seq != "" & !is.na(df$origin_seq))) # to make the crosswalk_parent_seq reassignment work
  age_df$crosswalk_parent_seq <- age_df$origin_seq
  
  ##----------------------------------------------------------------
  
  age_df <- subset(age_df, !(location_id %in% c(bad_ids, 4619)))
  
  age_df$is_agesplit <- ifelse(age_df$cv_age_split == 1, 1, 0)
  
  age_df <- age_df[, -which(names(age_df) %in% c("note_SR","lat","bm","cause_id","me_id","new","rei_name","raw_mean","raw_standard_error","haqi_cf","acause",
                                                 "short_bundle_name","run_id_get_population","age_group_id_name","unit_adj_denominator","severity","excluded_cases",
                                                 "confirmation_method","unique_group","cv_.","cv."))]
  
  # This file is used in the Crosswalks Loop #
  age_df <- subset(age_df, group_review != 0)
  
  write.csv(age_df, paste0("FILEPATH"), row.names=F)
  
  ### Adding age-split data to adjustment summary file
  df_summary <- read.csv(paste0("FILEPATH"))
  df_summary <- subset(df_summary,crosswalk_parent_seq %in% age_df$crosswalk_parent_seq)
  df_summary <- join(select(df_summary,-age_start,-age_end),age_df[,c("crosswalk_parent_seq","seq","mean","sex","age_start","age_end")],by=c("crosswalk_parent_seq","sex"))
  setnames(df_summary,c("mean"),c("age_split"))
  
  ### Reorganizing the data frame
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
  cols <- c(cols,"inpatient_adj","inp_beta","explicit_adj","exp_beta","age_split")
  df_summary <- df_summary[,cols]
  write.csv(df_summary,paste0("FILEPATH"),row.names=F)
  
  
  # This file is to reset bundle_data
  data_for_bundle <- subset(age_df, specificity == "Non-age specific data")
  data_for_bundle <- subset(data_for_bundle, !is.na(nid) & nid != 999999 & bundle_id != "")
  
  data_for_bundle[is.na(data_for_bundle)] <- ""
  data_for_bundle <- data_for_bundle[ , -which(names(data_for_bundle) %in% c("cv_explicit","cv_pcr","round_age_start","round_age_end","dummy_age_split","age_sum",
                                                                             "cv_age_split","logit_mean","delta_logit_se", "pred_mean","pred_linear_se","year_id","cv_.","cv."))]
  
  write.xlsx(data_for_bundle, paste0("FILEPATH"), sheetName="extraction")
  
  ##-----------------------------------------------------------------------------
  age_df <- age_df[ , -which(names(age_df) %in% c("cv_explicit","cv_pcr","round_age_start","round_age_end","dummy_age_split","age_sum",
                                                  "cv_age_split","logit_mean","delta_logit_se", "pred_mean","pred_linear_se","year_id"))]
  age_df$group_review[is.na(age_df$group_review)] <- 1
  age_df[is.na(age_df)] <- ""
  age_df$group_review <- ifelse(age_df$specificity=="","",1)
  age_df$uncertainty_type <- ifelse(age_df$lower=="","",as.character(age_df$uncertainty_type))
  age_df$uncertainty_type_value <- ifelse(age_df$lower=="","",age_df$uncertainty_type_value)
  
  # After age splitting, sometimes cases > sample size
  age_df$cases <- as.numeric(age_df$cases)
  age_df$sample_size <- as.numeric(age_df$sample_size)
  age_df$cases <- ifelse(age_df$cases > age_df$sample_size, age_df$sample_size * 0.99, age_df$cases)
  
  write.xlsx(age_df[age_df$bundle_id!="",], paste0("FILEPATH"), sheetName="extraction")
  
}

##############################################################################################
## Step 4: Adjust for sensitivity/specificity
##############################################################################################
sesp <- read.csv("FILEPATH")

for(i in eti_loops){
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]
  
  print(paste0("Adjusting for imperfect sensitivity/specificity for ", name))
  
  # Shouldn't have to age/sex split again, use this:
  df <- read.csv(paste0("FILEPATH"))
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
  
  
  p <- ggplot(df, aes(x=crosswalk_mean, y=mean)) + theme_bw(base_size=12) +
    #geom_point(size=3, aes(col=factor(age_bin))) +
    geom_point(size=3) +
    geom_abline(intercept=0, slope=1, col="red") +
    xlab("Post-crosswalk mean") + ylab("Adjusted diagnostics mean") + ggtitle(name) + scale_color_discrete("Age group\n(separate linear floors)")
  print(p)
  p <- ggplot(df, aes(x=crosswalk_mean, y=mean)) + theme_bw(base_size=12) +
    #geom_point(size=3, aes(col=factor(age_bin))) +
    geom_point(size=3) +
    geom_abline(intercept=0, slope=1, col="red") +
    scale_x_log10("Post-crosswalk mean (log10)") + scale_y_log10("Adjusted diagnostics mean (log10)") + ggtitle(name) + scale_color_discrete("Age group\n(separate linear floors)")
  print(p)
  #p <- ggplot(df, aes(x=raw_mean, y=mean)) + geom_point(size=3) + geom_abline(intercept=0, slope=1, col="red") + theme_bw(base_size=12) +
  #  xlab("Raw mean") + ylab("Final mean") + ggtitle(name)
  #print(p)
  
  ## Clear lower/upper (will be filled in uploading data)
  df$lower <- ""
  df$upper <- ""
  df$uncertainty_type <- ""
  df$uncertainty_type_value <- ""
  df[is.na(df)] <- ""
  df$mean <- as.numeric(df$mean)
  df$note_modeler <- ifelse(df$cv_diag_pcr==0, paste0(df$note_modeler, "| These data were adjusted for diagnostic sensitivity and specificity of laboratory to molecular diagnostics."), df$note_modeler)
  df <- df[,-(which(names(df) %in% c("crosswalk_mean","age_median","age_bin","lg_mean","delta_log_se")))]
  
  step2 <- subset(df, gbd_round!=2019)
  step4 <- subset(df, gbd_round==2019)
  
  write.csv(df, paste0("FILEPATH"), row.names=F)
  write.xlsx(step2, paste0("FILEPATH"), sheetName="extraction")
  write.xlsx(step4, paste0("FILEPATH"), sheetName="extraction")
  
  ### Adding adjustment to summary file
  df_summary <- read.csv(paste0("FILEPATH"))
  df_summary <- right_join(df_summary,df[,c("mean","crosswalk_parent_seq","age_start","age_end","sex", "linear_floor")],by=c("crosswalk_parent_seq","age_start","age_end","sex"))
  setnames(df_summary,"mean","sens_spec_adj")
  df_summary$sensitivity <- sesp[which(sesp$bundle_id==bundle),]$mean_sen
  df_summary$specificity <- sesp[which(sesp$bundle_id==bundle),]$mean_spe
  write.csv(df_summary,paste0("FILEPATH"),row.names=F)
}

##############################################################################################
## Step 5: Adjust rotavirus data for vaccine efficacy prior to dismod -----------------------
##############################################################################################

if(run_rota){
  ## Source fuctions, import required information. ##
  # Prepare your needed functions and information
  locs <- get_location_metadata(location_set_id=35,gbd_round_id=7,decomp_step="iterative")
  loc_pull <- locs$location_id[locs$level>=3]
  ages <- get_age_ids(7)
  
  #####################################
  ## Get rotavirus vaccine coverage, SDI ##
  rcov <- get_covariate_estimates(covariate_id=1075, location_id=loc_pull, year_id=1990:2020, gbd_round_id=7, decomp_step="iterative")
  setnames(rcov, c("mean_value","lower_value","upper_value"), c("rota_cov","rota_cov_lower","rota_cov_upper"))
  
  sdi <- get_covariate_estimates(covariate_id=881, location_id=loc_pull, year_id=1990:2020, gbd_round_id=7, decomp_step="iterative")
  setnames(sdi, c("mean_value","lower_value","upper_value"), c("sdi","sdi_lower","sdi_upper"))
  
  #############################################################################################
  ## Load in the file for the predicted values of rotavirus VE.
  ## These estimates come from an MR-BRT analysis of RCTs, CCs, and Before/after studies
  ## and in that analysis, SDI was chosen as the best predictor in MR_BRT Lasso
  #############################################################################################
  rota_ve <- read.csv(fix_path("FILEPATH"))
  
  #########################################################
  # Using data adjusted for sensitivity/specificity
  df <- read.csv("FILEPATH")
  df_save <- df
  df <- subset(df, !is.na(nid))

  df$year_id <- floor((df$year_start + df$year_end)/2)
  df$year_id <- ifelse(df$year_id < 1990, 1990, df$year_id)
  
  df <- join(df, rcov[,c("location_id","year_id","rota_cov","rota_cov_lower","rota_cov_upper")], by=c("location_id","year_id"))
  df <- join(df, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
  
  df <- join(df, rota_ve[,c("location_id","year_id","pred_ve","pred_se","pred_lb","pred_ub")], by=c("location_id","year_id"))
  
  # Drop locations that don't match ("Western Europe", "Southern sub-Saharan Africa")
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
  
  # df$mean_record <- df$mean
  # df$se_record <- df$standard_error
  df$mean <- ifelse(df$age_start < 5, df$mean_adj_ve, df$mean)
  df$standard_error <- ifelse(df$age_start < 5 & df$rota_cov > 0, df$se_adj_ve, df$standard_error)
  
  # Make sure they don't exceed 1...
  df$mean <- ifelse(df$mean >= 1, 0.99, df$mean)
  df$standard_error <- ifelse(df$standard_error > 1, 0.99, df$standard_error)
  
  ## Remove some columns, save!
  df <- subset(df, !is.na(mean))
  # df <- df[,-which(names(df) %in% c("rota_cov_lower","rota_cov_upper","dsev_lower","dsev_upper","dsev_match"))]
  
  # df[is.na(df)] <- ""
  df_save2 <- df[,names(df_save)] #forcing the dataframe to the original shape, for upload
  write.csv(df_save2, fix_path("FILEPATH"))
  
  ### Adding adjustment to summary file
  df$age_start <- signif(df$age_start,3) # fixing a rounding issue in the age_end column
  df$age_end <- signif(df$age_end,3)
  
  df_summary <- read.csv(paste0("FILEPATH"))
  df_summary$age_start <- signif(df_summary$age_start,3)
  df_summary$age_end <- signif(df_summary$age_end,3)
  df_summary <- right_join(df_summary,df[,c("mean","rota_cov","pred_ve","crosswalk_parent_seq","age_start","age_end","sex")],by=c("crosswalk_parent_seq","age_start","age_end","sex"))
  setnames(df_summary,c("mean","rota_cov","pred_ve"),c("rota_ve_adj","vaccine_coverage","vaccine_efficacy"))
  write.csv(df_summary,paste0("FILEPATH"),row.names=F)
}



##############################################################################################
## Step 6: Apply basic outliering  ----------------------------------------------------------
##############################################################################################
cutoffs_df <- read.csv("FILEPATH") 
for(i in eti_loops){
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  bundle <- eti_info$bundle_id[i]
  bundle_version_id <- vers$bundle_version_id[which(vers$bundle_id==bundle)]
  print(paste0("Applying outliering to ", name))
  
  # load data
  bv <- get_bundle_version(bundle_version_id)
  if(bundle==12){
    df <- read.csv(paste0("FILEPATH"))
  } else{
    df <- read.csv(paste0("FILEPATH"))
  }
  
  # apply threshold-based outliering
  if(bundle!=5){
    max_mean <- max(subset(bv,is_outlier!=1&sample_size>=10)$mean) 
    max_nid <- subset(bv,is_outlier!=1&sample_size>=10)$nid[which.max(subset(bv,is_outlier!=1&sample_size>=10)$mean)]
  } else{
    max_mean <- quantile(df$mean,0.925) ## Salmonella is a special case
    max_nid <- NA
  }
  df$is_outlier <- ifelse(df$mean > max_mean,1,df$is_outlier)
  
  
  # apply manual outliers from spreadsheet
  outliers <- read.xlsx(fix_path("FILEPATH"),sheet=eti_info$name_colloquial[i])
  outliers <- subset(outliers, Outlier==1)
  #outliers <- subset(outliers, !nid %in% remove$nid)
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
  
  
  write.csv(df,paste0("FILEPATH"),row.names=F)
  
  ## Save outliering thresholds
  d <- Sys.Date() %>% as.Date(origin="1970-01-01") %>% as.character # this needs to be set so it isn't read as a numeric
  cutoffs_df[which(cutoffs_df$bundle_id==bundle),]$date <- d
  cutoffs_df[which(cutoffs_df$bundle_id==bundle),]$bundle_version_id <- bundle_version_id
  cutoffs_df[which(cutoffs_df$bundle_id==bundle),]$cutoff <- max_mean
  cutoffs_df[which(cutoffs_df$bundle_id==bundle),]$NID <- max_nid
  write.csv(cutoffs_df,"FILEPATH",row.names=F)
  
  print("Outliers applied!")
}

#############################################################################
## Now I am going to build out some loops to upload, etc ####################
for(i in eti_loops){
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  m_name <- eti_info$modelable_entity_name[i]
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]
  bundle_version_id <- vers$bundle_version_id[which(vers$bundle_id==bundle)]
  print(paste0("Uploading modified data for ", name))
  
  out <- read.csv(paste0("FILEPATH"))
  max_mean <- (read.csv("FILEPATH") %>% subset(bundle_id==bundle))$cutoff
  if(bundle==12){
    # Run validation tests on adjustments summary file
    colname <- "rota_ve_adj"
    df_summary <- read.csv(paste0("FILEPATH"))
    if(any(is.na(df_summary[,colname]))){
      stop("NAs in adjustment summary file.")
    } else if(nrow(df_summary) != nrow(out)){
      stop("Mismatch between adjustment summary file and upload dataset.")
    }
    #look for NAs in sens_spec_adj OR rota_ve_adj
    
    # Clean up and upload
    out$note_sr <- gsub(pattern="\"\"\"",replacement="",x=out$note_sr) 
    out$site_memo <- gsub(pattern="\"\"\"",replacement="",x=out$site_memo) 
    out$case_diagnostics <- gsub(pattern="\"\"\"",replacement="",x=out$case_diagnostics)
    if("all_note" %in% names(out)){out$all_note <- gsub(pattern="\"\"\"",replacement="",x=out$all_note)}
    write.xlsx(out,paste0("FILEPATH"),sheetName="extraction")
    
    # Check for inpatient crosswalk file - if missing, crosswalk was not run and df should not be uploaded.
    fps <- with(eti_info,dir(fix_path(paste0("FILEPATH")),full.names=T))
    inp_latest <- fps[grep(pkl_date,fps)]
    if(save_crosswalks & length(inp_latest) > 0){
      result <- save_crosswalk_version(bundle_version_id = bundle_version_id,
                                       data_filepath = paste0("FILEPATH"),
                                       description = paste0("description",round(max_mean,3),"; bundle_version_id=",bundle_version_id))
      new_xwalks[bundle_id==bundle, `:=`(bv_id=bundle_version_id, crosswalk_version_id=result$crosswalk_version_id)]
    } else if (save_crosswalks & length(inp_latest == 0)) {
      stop(paste0("The inpatient crosswalk was not run for ", name, ". Crosswalk not uploaded."))
    }
  } else{
    # Run validation tests on adjustments summary file
    colname <- "sens_spec_adj"
    df_summary <- read.csv(paste0("FILEPATH"))
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
    write.xlsx(out,paste0("FILEPATH"),sheetName="extraction")
    
    # Check for inpatient crosswalk file - if missing, crosswalk was not run and df should not be uploaded.
    fps <- with(eti_info,dir(fix_path(paste0("FILEPATH")),full.names=T))
    inp_latest <- fps[grep(pkl_date,fps)]
    if(save_crosswalks & length(inp_latest) > 0){
      result <- save_crosswalk_version(bundle_version_id = bundle_version_id,
                                       data_filepath = paste0("FILEPATH"),
                                       description = paste0("description"))
      new_xwalks[bundle_id==bundle, `:=`(bv_id=bundle_version_id, crosswalk_version_id=result$crosswalk_version_id)]
    } else if (save_crosswalks & length(inpatient_latest) == 0) {
      stop(paste0("The inpatient crosswalk was not run for ", name, ". Crosswalk not uploaded."))
    }
  }
  print("Uploaded PCR Se/Sp adjusted iterative")
  write.csv(new_xwalks, out_xwalk_ids, row.names=F)
}
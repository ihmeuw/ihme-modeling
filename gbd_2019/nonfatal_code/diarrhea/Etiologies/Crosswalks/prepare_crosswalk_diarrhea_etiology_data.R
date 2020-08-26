#######################################################################################
## This code does a lot!!
## The file is intended to be a single place to perform all data adjustments for the
## diarrheal etiologies that depend on GEMS/MALED for adjustments.
# The intention for this code is to prepare the etiology data for diarrhea
# so that they are ready for modeling during decomp step 2. This includes
# pulling and saving the decomp step 1 data, creating dataframes for running
# out of DisMod crosswalks for explicit etiology testing and inpatient sample
# populations and finally, adjusting for imperfect sensitivity and specificity.
# For ETEC and EPEC, we will also adjust these data such that they are representing
# ST- and typical serotypes, only. For Norovirus, we also adjust for high-risk
# of diagnostic bias (gastroenteritis) and for the use of PCR as the diagnostic (MRBRT).
######################################################################################

# Prepare your needed functions and information
library(metafor)
library(msm)
library(openxlsx)
library(matrixStats)
library(plyr)
library(boot)
library(data.table)
library(ggplot2)
source() # filepaths to central functions for data upload/download
source() # filepaths to central functions for MR-BRT
source("/filepath/bundle_crosswalk_collapse.R")
source("/filepath/age_split_mrbrt_weights.R")
source("/filepath/sex_split_mrbrt_weights.R")

locs <- read.csv("filepath")

eti_info <- read.csv("filepath")
eti_info <- subset(eti_info, model_source=="dismod" & rei_id!=183 & cause_id==302)

# DisMod values #
  dmvs <- read.csv("filepath")
  dmvs_full <- read.csv("filepath")

# Calculate the standard error as the product of variances
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2

## Find Saudi Arabia subnationals, England areas (like Northwest, West Midlands) to remove ##
locs_old <- read.csv("filepath")
bad_ids <- c(locs_old$location_id[locs_old$parent_id==152], locs_old$location_id[locs_old$parent_id==73 & locs_old$most_detailed==0])
bad_ids <- ifelse(bad_ids==93,95,bad_ids)

## Load in information for sex splitting ##
  diar_sex_weights <- get_outputs(topic="cause", cause_id=302, age_group_id=22, year_id=2017, gbd_round_id=5, measure_id=6, sex_id=c(1,2), location_id=1, metric_id=3)
  diar_sex_weights$number <- diar_sex_weights$val

##############################################################################################
## Step 1: Pull and save decomp step 1/2 data. Should only be done once!
##############################################################################################
## Build loop here:
for(i in 1:11){
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

  print(paste0("Pulling data for ", name))

  ## Already done for all etiologies, to add new data, run this loop with uncommented section ##

   # df <- get_bundle_data(bundle_id=eti_info$bundle_id[i], decomp_step="step1")
   # df$raw_mean <- df$mean
   # df$raw_standard_error <- df$standard_error
   #  write.csv(df, f"ilepath", row.names=F)

  clean_df <- read.csv("filepath")
  clean_df <- subset(clean_df, !is.na(nid) & nid!=999999 & bundle_id != "")
  clean_df[is.na(clean_df)] <- ""
  clean_df <- clean_df[,-(which(names(clean_df) %in% c("cv_pcr","cv_explicit","raw_mean","raw_standard_error","rei_name","province_name",
                                                       "note_SR","lat","bm","cause_id","me_id","new","rei_name","raw_mean","raw_standard_error","haqi_cf","acause",
                                                       "short_bundle_name","run_id_get_population","age_group_id_name","unit_adj_denominator","severity","excluded_cases",
                                                       "confirmation_method","unique_group","cv_.","cv.")))]
  clean_df <- subset(clean_df, !(location_id %in% bad_ids))
  clean_df$uncertainty_type <- ifelse(clean_df$lower == "", "", as.character(clean_df$uncertainty_type))
  clean_df$uncertainty_type_value <- ifelse(clean_df$lower == "", "", clean_df$uncertainty_type_value)
  write.xlsx(clean_df, "filepath", sheetName="extraction")
}

## Overwrite the EPEC data with the data with data where adjusted mean was replaced with
## The raw mean values (pre-typical EPEC adjustment)
# Create a new file with the raw means
epec <- read.csv("filepath")
  epec$adjusted_mean_2017 <- epec$mean
  epec$mean <- epec$original_mean
  epec$cases <- epec$mean * epec$sample_size
  epec$standard_error <- sqrt(epec$mean * (1-epec$mean)/epec$effective_sample_size)

  # file to overwrite what is currently in DB
  epec[is.na(epec)] <- ""
  write.xlsx(epec, "filepath", sheetName="extraction")

  # New data for 2019 and crosswalks.
  new_dat <- read.csv("filepath")
  df <- rbind.fill(epec, new_dat)

write.csv(epec, "filepath")

## Upload the unadjusted data to the bundles (manually pulled out new lit data) ##
# Done! 5/21/19: 4.30PM
#upload_bundle_data(bundle_id=7, decomp_step="step2", filepath = "filepath")
epec <- get_bundle_data(bundle_id = 7, decomp_step = "step2")
write.csv(epec, "filepath")

##############################################################################################
## Step 1.5: Try to adjust for PCR diagnostics. The starting point is appending all data
## so that there is some ability to find and populate a new column cv_diag_pcr
##############################################################################################
pcr_nids <- c(264996)

# Collect them all to review simultaneously.
out_df <- data.frame()
for(i in 1:11){

  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]

  print(paste0("Pulling and appending for ", name))
  in_df <- read.csv("filepath")
    in_df$bundle_record <- bundle

  # Use regular expressions to find key words
    in_df$citation <- as.character(in_df$field_citation_value)
    in_df$cv_diag_pcr <- ifelse(in_df$citation %like% "olecular", 1,
                                 ifelse(in_df$citation %like% "PCR",1,
                                        ifelse(in_df$citation %like% "rray",1,
                                               ifelse(in_df$citation %like% "ultiplex",1,0))))
    table(in_df$cv_diag_pcr)
    in_df$cv_diag_pcr <- ifelse(in_df$cv_diag_pcr == 1, 1,
                                 ifelse(in_df$case_diagnostics %like% "olecular", 1,
                                        ifelse(in_df$case_diagnostics %like% "PCR",1,
                                               ifelse(in_df$case_diagnostics %like% "rray",1,
                                                      ifelse(in_df$case_diagnostics %like% "ultiplex",1,0)))))
  # Rewrite those files now that we have the new column
    write.csv(in_df, "filepath", row.names=F)

  out_df <- rbind.fill(out_df, in_df)
}

# Remove unnecessary columns
out_df <- out_df[ , -which(names(out_df) %in% c("cv_explicit","cv_pcr","round_age_start","round_age_end","dummy_age_split","age_sum",
                                                            "cv_age_split","logit_mean","delta_logit_se", "pred_mean","pred_linear_se","year_id",
                                                "cv.","cause_id","acause","pmid_doi_pmcID_URL","start_date","end_date","short_bundle_name","run_id_get_population",
                                                "haqi_cf","lat","bm","age_group_id_name","unit_adj_denominator","severity","excluded_cases","unique_group","cv_.",
                                                "data_sheet_fiel_path","province_name","step2_locatoni_year","super_region_id","predicted","pred_std","all_note","strsplit",
                                                "X","pred_ratio","pred_se","se_original","gbd_round.1","X_data_id"))]
write.csv(out_df, "filepath", row.names=F)

##############################################################################################
## Decomp step 4: Add new data ##

for(i in 1:11){
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  
  m_name <- eti_info$modelable_entity_name[i]
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]

  # Add new data for GBD 2019
    df <- read.csv("filepath")
      df$gbd_round[is.na(df$gbd_round)] <- 2017
    new_dat <- read.csv("filepath")

    df <- rbind.fill(df, new_dat)

  write.csv(df, "filepath", row.names=F)
}

##############################################################################################
## Step 2: These data need to be age split. Use an MR-BRT age curve.
##############################################################################################
## Determine where age splitting should occur. ##
  age_map <- read.csv("filepath")
  age_map <- age_map[age_map$age_pull==1,c("age_group_id","age_start","age_end","order","age_pull")]
# age_cuts <- c(0,1,5,20,40,60,80,100)
  age_cuts <- c(0,1,5,20,40,65,100)
  length <- length(age_cuts) - 1

  age_map$age_dummy <- cut(age_map$age_end, age_cuts, labels = paste0("Group ", 1:length))

## Create prevalence weights to age-split the data from the global diarrhea prevalence age pattern in GBD 2017 ##
  dmod_global <- get_outputs(topic="cause", cause_id=302, age_group_id = age_info$age_group_id, year_id=1990:2017, location_id=1, sex_id=3, measure_id=5, metric_id=1, gbd_round_id=5)
  dmod_global$number <- dmod_global$val
  denom_weights <- dmod_global
  denom_weights <- join(denom_weights, age_map[,c("age_group_id","age_dummy","order")], by="age_group_id")

  denom_age_groups <- aggregate(number ~ location_id + year_id + age_dummy, data=denom_weights, function(x) sum(x))
  denom_all_ages <- aggregate(number ~ location_id + year_id, data=denom_weights, function(x) sum(x))
  denom_all_ages$total_number <- denom_all_ages$number

  denominator_weights <- join(denom_age_groups, denom_all_ages[,c("location_id","year_id","total_number")], by=c("location_id","year_id"))

##------------------------------------------------------------------------------------------
## Start the etiology loop ##
pdf("filepath", height=8, width=9)
out_res <- data.frame()
  for(i in 1:11){
    name <- eti_info$name_colloquial[i]
    me_name <- eti_info$modelable_entity[i]
    me_id <- eti_info$modelable_entity_id[i]
    bundle <- eti_info$bundle_id[i]
    rei_name <- eti_info$rei_name[i]

    print(paste0("Modeling an age-split for ", name))

  # New data
    df <- read.csv("filepath")

    # Some data are too old. This is a good opportunity to only model newer data.
      # Chose 1986 because default time window in DisMod is 5 years (1990-4)
    df <- subset(df, year_end >= 1986)

    ## All rows should not have NA values for the key crosswalks
    df$cv_explicit_test[is.na(df$cv_explicit_test)] <- 0
    df$cv_inpatient[is.na(df$cv_inpatient)] <- 0
    df$cv_inpatient_sample[is.na(df$cv_inpatient_sample)] <- 0

    # Keep if proportion
    df <- subset(df, measure=="proportion")

    # Some rows don't have means
      means <- df$cases / df$sample_size
      df$mean <- ifelse(is.na(df$mean), means, df$mean)
      df <- subset(df, !is.na(mean))

    # Some rows don't have cases
      cases <- df$mean * df$sample_size
      df$cases <- ifelse(is.na(df$cases), cases, df$cases)

    # specificity is missing from entamoeba
      if(i == 3){
        df$specificity <- ""
      }

################################################################
## Pull out age specific data
##--------------------------------------------------------------
## Three different ways to identify non-age-specific data rows
    # Part 1, Test a dummy variable where age_start/end rounded doesn't equal age_start/end
      df$round_age_start <- round(df$age_start, 1)
      df$round_age_end <- round(df$age_end, 1)
      df$age_sum <- df$age_start + df$age_end

      df$dummy_age_split <- with(df, ifelse(age_end > 10, ifelse(df$round_age_start != df$age_start, 1, ifelse(df$round_age_end != df$age_end, 1, 0)), 0))

    # Part 2, where the age_range is not 0-99
      #df$note_modeler <- ifelse(df$age_start == 0 & df$age_end == 99, paste0("Original age end 99. ", df$note_modeler), as.character(df$note_modeler))
      df$dummy_age_split <- ifelse(df$dummy_age_split==1,1,ifelse(df$age_start == 0 & df$age_end == 99,1,0))

    # Part 3, where the age_start/end rounded isn't equal to the unrounded value
      df$dummy_age_split <- ifelse(df$dummy_age_split==1,1,ifelse(df$note_modeler %like% "riginal age end", 1, 0))

    age_specific <- subset(df, dummy_age_split == 0)
    age_non_specific <- subset(df, dummy_age_split == 1)

    # Some age-split data have already been duplicated, all rows should be removed
      age_specific$cv_age_split <- 0
      age_non_specific$cv_age_split <- 1

##------------------------------------------------------
# Get rid of now duplicated age split data #
# In previous split, a data point 0-99 might be split to 0-15.4 15.5-99.
# Assumption is that in those splits, only one data point will start with 0 age
# This is a dummy to do this (age sum)

    age_group_review <- subset(age_non_specific, age_sum > 100)
    age_group_review <- data.frame(seq = age_group_review[,"seq"])

##------------------------------------------------------

    age_non_specific <- subset(age_non_specific, age_sum < 100)

    df <- rbind(age_specific, age_non_specific)

## Guidance is to age split any data more than 25 year span
    df$cv_age_split <- ifelse(df$age_end - df$age_start > 25, 1, df$cv_age_split)

##---------------------------------------------------------------
# This is the function to do the age splitting. It takes
# an estimated ratio of the observed to expected data for all age-specific
# data points and runs that through MR-BRT. The result is then predicted
# for six age groups which are then merged 1:m with the age-non-specific
# data to make six new points with the new proportion equal to the
# original mean times the modeled ratio. It keeps a set of rows
# from the age-non-specific data to mark as group_review = 1.

    age_df <- age_split_mrbrt_weights(df, denominator_weights, output_dir="filepath", bounded=T, title=name, global_merge=T)

##----------------------------------------------------------------

  # subset out Saudi subnationals and other unneeded location_ids #
    age_df <- subset(age_df, !(location_id %in% c(bad_ids, 71, 23, 4619)))

    age_df$is_agesplit <- ifelse(age_df$cv_age_split == 1, 1, 0)

  # Get rid of a bunch of unneeded columns
    age_df <- age_df[, -which(names(age_df) %in% c("note_SR","lat","bm","cause_id","me_id","new","rei_name","raw_mean","raw_standard_error","haqi_cf","acause",
                                                               "short_bundle_name","run_id_get_population","age_group_id_name","unit_adj_denominator","severity","excluded_cases",
                                                               "confirmation_method","unique_group","cv_.","cv."))]

  # This file is used in the Crosswalks Loop #
   write.csv(age_df, "filepath", row.names=F)

  # This file is to reset bundle_data
   data_for_bundle <- subset(age_df, specificity == "Non-age specific data")
   data_for_bundle <- subset(data_for_bundle, !is.na(nid) & nid != 999999 & bundle_id != "")

   data_for_bundle <- rbind.fill(data_for_bundle, age_group_review) # 'age_group_review' are seqs where there are multiple, previously splitting age-non-specific data that are to be cleared.
   data_for_bundle[is.na(data_for_bundle)] <- ""
   data_for_bundle <- data_for_bundle[ , -which(names(data_for_bundle) %in% c("cv_explicit","cv_pcr","round_age_start","round_age_end","dummy_age_split","age_sum",
                                                                              "cv_age_split","logit_mean","delta_logit_se", "pred_mean","pred_linear_se","year_id","cv_.","cv."))]

     write.xlsx(data_for_bundle,"filepath", sheetName="extraction")

##-----------------------------------------------------------------------------
  # Pull out new data, duplicate sexes (can't upload sex="Both"),
  # remove group_review = 0, save in the Upload folder
    age_sex_df <- duplicate_sex_rows(age_df)
    age_sex_df <- scalar_sex_rows(age_df, denominator_weights=diar_sex_weights, bounded=T, global_merge=T, title=name)

    age_sex_df <- age_sex_df[ , -which(names(age_sex_df) %in% c("cv_explicit","cv_pcr","round_age_start","round_age_end","dummy_age_split","age_sum",
                                                                "cv_age_split","logit_mean","delta_logit_se", "pred_mean","pred_linear_se","year_id"))]
    age_sex_df$group_review[is.na(age_sex_df$group_review)] <- 1
    age_sex_df <- subset(age_sex_df, group_review != 0)
    age_sex_df[is.na(age_sex_df)] <- ""
    age_sex_df$group_review <- ifelse(age_sex_df$specificity=="","",1)
    age_sex_df$uncertainty_type <- ifelse(age_sex_df$lower=="","",as.character(age_sex_df$uncertainty_type))
    age_sex_df$uncertainty_type_value <- ifelse(age_sex_df$lower=="","",age_sex_df$uncertainty_type_value)

  # After age splitting, sometimes cases > sample size
      age_sex_df$cases <- as.numeric(age_sex_df$cases)
      age_sex_df$sample_size <- as.numeric(age_sex_df$sample_size)
    age_sex_df$cases <- ifelse(age_sex_df$cases > age_sex_df$sample_size, age_sex_df$sample_size * 0.99, age_sex_df$cases)

    write.xlsx(age_sex_df[age_sex_df$bundle_id!="",], "filepath", sheetName="extraction")

  }
dev.off()


##############################################################################################
##-- Step 2.5: Adjust typical EPEC and ST-ETEC  ----------------------------------------------
##############################################################################################

# Right now the ratios vary by GBD super region. There seems to be variation for ST-
# in the ratio depending on if the data come from MAL-ED compared to GEMS, suggesting that
# the frequency of ST- in severe diarrhea is higher than non-severe.
# We have not done this in MR_BRT because of this regional variation.

## tEPEC ratio ##
# This file is used in the Crosswalks Loop #
  df <- read.csv("filepath")
  df <- join(df, locs[,c("location_id","super_region_name")], by="location_id")
  epec_ratio <- read.csv("filepath")
  df <- join(df, epec_ratio, by="super_region_name")
  df$mean <- df$mean * df$pred_ratio
  df$raw_mean <- df$mean
  df$se_original <- df$standard_error
  df$standard_error <- sqrt(df$mean^2 * df$pred_se^2 + df$pred_ratio^2 * df$standard_error^2 + df$standard_error^2 * df$pred_se^2)
  df$note_modeler <- paste0(df$note_modeler, "| The original value was ", df$original_mean," and was adjusted using a super-region proportion.")
  df[is.na(df)] <- ""

  write.csv(df, "filepath")

## A version to be uploaded as a crosswalk version
  age_sex_df <- duplicate_sex_rows(df)
  age_sex_df$group_review[is.na(age_sex_df$group_review)] <- 1
  age_sex_df <- subset(age_sex_df, group_review != 0)
  age_sex_df[is.na(age_sex_df)] <- ""
  age_sex_df$group_review <- ifelse(age_sex_df$specificity=="","",1)
  write.xlsx(age_sex_df[age_sex_df!="",], paste0("filepath"), sheetName="extraction")

############################################################################
## ST-ETEC ratio ##
  df <- read.csv("filepath")
  df <- join(df, locs[,c("location_id","super_region_name")], by="location_id")
  etec_ratio <- read.csv("filepath")
  df <- join(df, etec_ratio, by=c("super_region_name", "cv_inpatient"))
  df$mean <- df$mean * df$pred_ratio
  df$raw_mean <- df$mean
  df$se_original <- df$standard_error
  df$standard_error <- sqrt(df$mean^2 * df$pred_se^2 + df$pred_ratio^2 * df$standard_error^2 + df$standard_error^2 * df$pred_se^2)
  df$note_modeler <- paste0(df$note_modeler, "| The original value was ", df$original_mean," and was adjusted using a super-region proportion.")
  df[is.na(df)] <- ""

  write.csv(df, "filepath")

  ## A version to be uploaded as a crosswalk version
  age_sex_df <- duplicate_sex_rows(df)
  age_sex_df$group_review[is.na(age_sex_df$group_review)] <- 1
  age_sex_df <- subset(age_sex_df, group_review != 0)
  age_sex_df[is.na(age_sex_df)] <- ""
  age_sex_df$group_review <- ifelse(age_sex_df$specificity=="","",1)
  write.xlsx(age_sex_df[age_sex_df!="",], "filepath", sheetName="extraction")

##############################################################################################
## Step 3: Run crosswalks in MR-BRT ---------------------------------------------------------
##############################################################################################
## Set the type, must be "log" or "logit"
xw_transform <- "logit"
pdf("filepath", height=8, width=9)
out_res <- data.frame()
## Build loop here:
for(i in 1:11){
    name <- eti_info$name_colloquial[i]
    me_name <- eti_info$modelable_entity[i]
    me_id <- eti_info$modelable_entity_id[i]
    bundle <- eti_info$bundle_id[i]
    rei_name <- eti_info$rei_name[i]

    print(paste0("Modeling an MR-BRT crosswalk for ", name))

    df <- read.csv("filepath")

  # Keep track of the original mean value
    df$raw_mean <- df$mean
    df$raw_standard_error <- df$standard_error

  # Keep if year > 1985
    df <- subset(df, year_start >= 1985)

##-------------------------------------------------------------------------------------------------
  #### Inpatient sample population ####
    df$inpatient <- ifelse(df$cv_inpatient==1,1,ifelse(df$cv_inpatient_sample==1,1,0))
    df$inpatient[is.na(df$inpatient)] <- 0
    df$is_reference <- ifelse(df$inpatient==1,0,1)

# Make sure new data aren't used!
  df$gbd_round[is.na(df$gbd_round)] <- 2017
  xw_df <- subset(df, gbd_round != 2019)

  cv_inpatient <- bundle_crosswalk_collapse(xw_df, covariate_name="inpatient", age_cut=c(0,1,5,20,40,60,80,100), year_cut=c(seq(1980,2015,5),2019), merge_type="within", location_match="exact", include_logit = T)

  # Rbind the GEMS1A and MALED results
      g1m <- read.csv("filepath")
    
    g1m$sex <- "Both"

# It has been extracted already but uses MSD/LSD as its comparisons for cv_inpatient, which is not consistent with the crosswalk
# So I am going to pull out those data for the actual crosswalk
    cv_inpatient <- subset(cv_inpatient, !(nid %in% c(224856,224855,224854,224849,224848,223566,224853)))

    cv_inpatient <- rbind(cv_inpatient, g1m)

    # Drop if the ratio is 1. Likely this is an error in extraction.
    cv_inpatient <- subset(cv_inpatient, ratio!= 1)

    # Can be duplicated if there is an age split
    cv_inpatient <- cv_inpatient[!duplicated(cv_inpatient$ratio),]

    # Save that file for a record
    write.csv(cv_inpatient, "filepath", row.names=F)

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

  # Including NID as a study id
    fit1 <- run_mr_brt(
      output_dir = "filepath",
      model_label = "cv_inpatient",
      data = cv_inpatient,
      mean_var = "response_var",
      se_var = "response_se",
      study_id = "nid",
      trim_pct = trim_pct,
      method = "trim_maxL",
      overwrite_previous = TRUE
    )

    check_for_outputs(fit1)
      df_pred <- data.frame(intercept = 1)
    pred1 <- predict_mr_brt(fit1, newdata = df_pred)
      check_for_preds(pred1)
      pred_object <- load_mr_brt_preds(pred1)
      preds <- pred_object$model_summaries

    # Calculate log se
      preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
    # Convert the mean and standard_error to linear space
      if(xw_transform == "log"){
        preds$linear_se <- deltamethod(~exp(x1), preds$Y_mean, preds$se^2)
        preds$ratio <- exp(preds$Y_mean)
        preds$transformation <- "log"
      } else {
        preds$linear_se <- deltamethod(~exp(x1)/(1+exp(x1)), preds$Y_mean, preds$se^2)
        preds$ratio <- inv.logit(preds$Y_mean)
        preds$transformation <- "logit"
      }

    ## produce approximation of a funnel plot
      mod_data <- fit1$train_data
      mod_data$outlier <- ceiling(abs(mod_data$w - 1))
      mod_data$location_id <- mod_data$location_match
      mod_data$row_num <- 1:length(mod_data$mean)
      mod_data <- join(mod_data, locs[,c("location_id","location_name")], by="location_id")
      f <- ggplot(mod_data, aes(x=response_var, y=response_se, col=factor(outlier))) + geom_point(size=3) + geom_vline(xintercept=preds$Y_mean) + scale_y_reverse("Standard error") +
        theme_bw() + scale_x_continuous("Transformed ratio") + scale_color_manual("", labels=c("Used","Trimmed"), values=c("#0066CC","#CC0000")) + ggtitle(paste0(name, " inpatient ratio")) +
        geom_vline(xintercept=preds$Y_mean_lo, lty=2) + geom_vline(xintercept=preds$Y_mean_hi, lty=2)
      print(f)
    ## Create essentially a forest plot
      mod_data$label <- with(mod_data, paste0(nid,"_",location_name,"_",age_start,"-",age_end,"_",year_start,"-",year_end,"_",sex,"_",row_num))
      f <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=response_var + response_se*1.96, ymin=response_var - response_se*1.96)) + geom_point(aes(y=response_var, x=label)) + geom_errorbar(aes(x=label), width=0) +
        theme_bw() + ylab("Transformed ratio") + xlab("") + coord_flip() + ggtitle(paste0(name, " inpatient ratio (",round(preds$ratio,3),")")) +
        geom_hline(yintercept=0) + geom_hline(yintercept=preds$Y_mean, col="purple") +
        geom_rect(data=preds, aes(ymin=Y_mean_lo, ymax=Y_mean_hi, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
      print(f)

    # Keep a record of the values
      p <- preds[,c("ratio","linear_se","Y_mean","Y_mean_lo","Y_mean_hi","transformation")]
      p$etiology <- name
      p$rei_name <- rei_name
      p$variable <- "cv_inpatient"
      p$count_obs <- length(cv_inpatient$ratio)
      p$dismod_d1 <- 1/dmvs$value_inpatient[i]
      p$dismod_sig <- dmvs$sig_inpatient[i]
      out_res <- rbind(out_res, p)

      df$log_inpatient <- preds$Y_mean
      df$se_log_inpatient <- preds$se
      df$mrbrt_inpatient <- preds$ratio
      df$mrbrt_se_inpatient <- preds$linear_se

      out_inp <- p

    # Convert the mean by the crosswalk!
      if(xw_transform == "log"){
        df$mean <- ifelse(df$cv_inpatient==1, df$mean * df$mrbrt_inpatient, df$mean)
      } else {
        df$logit_mean <- logit(df$mean)
        df$mean <- ifelse(df$cv_inpatient==1, inv.logit(df$logit_mean + preds$Y_mean), df$mean)
      }
      df$mean <- ifelse(df$raw_mean == 0, 0, df$mean)
      std_inpatient <- sqrt(with(df, standard_error^2 * mrbrt_se_inpatient^2 + standard_error^2*mrbrt_inpatient^2 + mrbrt_se_inpatient^2*mean^2))
      df$standard_error <- ifelse(df$cv_inpatient==1, std_inpatient, df$standard_error)
    # ggplot(df, aes(x=raw_mean, y=mean, col=factor(cv_inpatient))) + geom_point()

##-------------------------------------------------------------------------------------------------
  #### Explicit testing ####
    df$is_reference <- ifelse(df$cv_explicit_test==1,0,1)

# Make sure new data aren't used
    cv_explicit <- bundle_crosswalk_collapse(df[df$gbd_round!=2019,], covariate_name="cv_explicit_test", age_cut=c(0,1,2,5,20,40,60,80,100), year_cut=c(seq(1980,2015,5),2019),
                                             merge_type="between", location_match="country", include_logit = T)

    cv_explicit$location_match <- ifelse(cv_explicit$location_match=="","Other",cv_explicit$location_match)
    cv_explicit$match_nid <- paste0(cv_explicit$nid,"_",cv_explicit$n_nid)

    # Save that if we want to run MR-BRT
    write.csv(cv_explicit, "filepath", row.names=F)

    # After age-splitting, we might have multiple rows of same values
      cv_explicit$unique <- paste0(cv_explicit$match_nid, "_", round(cv_explicit$ratio,5))
      cv_explicit <- subset(cv_explicit, !duplicated(unique))

    # Now doing the crosswalks in logit space #
    if(xw_transform == "log"){
      cv_explicit$response_var <- cv_explicit$log_ratio
      cv_explicit$response_se <- cv_explicit$delta_log_se
    } else {
      cv_explicit$response_var <- cv_explicit$logit_ratio
      cv_explicit$response_se <- cv_explicit$logit_ratio_se
    }

    fit1 <- run_mr_brt(
      output_dir = "filepath",
      model_label = "cv_explicit",
      data = cv_explicit,
      mean_var = "response_var",
      se_var = "response_se",
      trim_pct = 0.1,
      study_id = "match_nid",
      method = "trim_maxL",
      overwrite_previous = TRUE
    )

      check_for_outputs(fit1)
    df_pred <- data.frame(intercept = 1)
    pred1 <- predict_mr_brt(fit1, newdata = df_pred)
      check_for_preds(pred1)
    pred_object <- load_mr_brt_preds(pred1)
    preds <- pred_object$model_summaries

    # Calculate log se
    preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
    # Convert the mean and standard_error to linear space
    if(xw_transform == "log"){
      preds$linear_se <- deltamethod(~exp(x1), preds$Y_mean, preds$se^2)
      preds$ratio <- exp(preds$Y_mean)
      preds$transformation <- "log"
    } else {
      preds$linear_se <- deltamethod(~exp(x1)/(1+exp(x1)), preds$Y_mean, preds$se^2)
      preds$ratio <- inv.logit(preds$Y_mean)
      preds$transformation <- "logit"
    }

  ## produce an approximation of a funnel plot
    mod_data <- fit1$train_data
    mod_data$outlier <- ceiling(abs(mod_data$w - 1))
    mod_data$row_num <- 1:length(mod_data$mean)
    f <- ggplot(mod_data, aes(x=response_var, y=response_se, col=factor(outlier))) + geom_point(size=3) + geom_vline(xintercept=preds$Y_mean) + scale_y_reverse("Standard error") +
      theme_bw() + scale_x_continuous("Transformed ratio") + scale_color_manual("", labels=c("Used","Trimmed"), values=c("#0066CC","#CC0000")) + ggtitle(paste0(name, " explicit ratio"))
    print(f)
  ## Create essentially a forest plot
    mod_data$label <- with(mod_data, paste0(match_nid,"_",location_match,"_",age_bin,"_",year_bin,"_",row_num))
    f <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=response_var + response_se*1.96, ymin=response_var - response_se*1.96)) + geom_point(aes(y=response_var, x=label)) + geom_errorbar(aes(x=label), width=0) +
      theme_bw() + ylab("Transformed ratio") + xlab("") + coord_flip() + ggtitle(paste0(name, " explicit ratio (",round(preds$ratio,3),")")) +
      geom_hline(yintercept=0) + geom_hline(yintercept=preds$Y_mean, col="purple") +
      geom_rect(data=preds, aes(ymin=Y_mean_lo, ymax=Y_mean_hi, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
    print(f)

   # Keep a record of the values
      p <- preds[,c("ratio","linear_se","Y_mean","Y_mean_lo","Y_mean_hi","transformation")]
      p$etiology <- name
      p$rei_name <- rei_name
      p$variable <- "cv_explicit"
      p$count_obs <- length(cv_explicit$ratio)
      p$dismod_d1 <- 1/dmvs$value_explicit[i]
      p$dismod_sig <- dmvs$sig_explicit[i]
      out_res <- rbind(out_res, p)

      df$mrbrt_explicit <- preds$ratio
      df$mrbrt_se_explicit <- preds$linear_se

      out_exp <- p

      # Convert the mean by the crosswalk!
      if(xw_transform == "log"){
        df$mean <- ifelse(df$cv_explicit_test==1, df$mean * df$mrbrt_explicit, df$mean)
      } else {
        df$logit_mean <- logit(df$mean)
        trans_mean <- inv.logit(df$logit_mean + preds$Y_mean)
        df$mean <- ifelse(df$cv_explicit_test==1, trans_mean, df$mean)
      }
      df$mean <- ifelse(df$raw_mean == 0, 0, df$mean)
      std_explicit <- sqrt(with(df, standard_error^2 * mrbrt_se_explicit^2 + standard_error^2*mrbrt_explicit^2 + mrbrt_se_explicit^2*mean^2))
      df$standard_error <- ifelse(df$cv_explicit_test==1, std_explicit, df$standard_error)

    ## Make a diagnostic ggplot ##
      df$adjustment_type <- ifelse(df$inpatient==1 & df$cv_explicit_test==1, "Inpatient and Explicit", ifelse(df$inpatient==1, "Inpatient",ifelse(df$cv_explicit_test==1,"Explicit","Reference")))
      g <- ggplot(data=subset(df, !is.na(adjustment_type)), aes(x=raw_mean, y=mean, col=adjustment_type)) + geom_point(size=3) + geom_abline(intercept=0, slope=1) + theme_bw(base_size=12) +
        xlab("Raw mean") + ylab("Adjustment mean") + scale_color_discrete("") +
        geom_abline(intercept=0, slope=1/dmvs$value_inpatient[i], lty=2, col="#669933") + geom_abline(intercept=0, slope=1/dmvs$value_explicit[i], lty=2, col="#FF0033") +
        geom_point(size=3) +
        ggtitle(paste0(name,"\nInpatient ratio ", round(out_inp$ratio/0.5,3)," (SE = ",round(out_inp$linear_se,3),") [DisMod decomp1: ",round(1/dmvs$value_inpatient[i],3),"]",
                       "\nExplicit ratio ",round(out_exp$ratio/0.5,3)," (SE = ",round(out_exp$linear_se,3),") [DisMod decomp1: ",round(1/dmvs$value_explicit[i],3),"]"))
     print(g)

    # Dummy data for scatter
      plot_df <- df[,c("mean","raw_mean","inpatient","cv_explicit_test","adjustment_type","age_start","age_end","standard_error","raw_standard_error","location_id")]
      plot_df$dismod_mean <- plot_df$raw_mean
      plot_df$dismod_mean <- with(plot_df, ifelse(inpatient==1, dismod_mean * 1/dmvs$value_inpatient[i], dismod_mean))
      plot_df$dismod_mean <- with(plot_df, ifelse(cv_explicit_test==1, dismod_mean * 1/dmvs$value_explicit[i], dismod_mean))

      if(xw_transform == "log"){
        note <- ""
      } else {
        note <- "Note that logit transformation was used, coefficients not directly comparable"
      }
      g <- ggplot(data=subset(plot_df, !is.na(adjustment_type)), aes(x=dismod_mean, y=mean, col=adjustment_type)) + geom_point(size=3, alpha=0.75) + geom_abline(intercept=0, slope=1) + theme_bw(base_size=12) +
        xlab("Mean with DisMod coefficients") + ylab("Mean with MR-BRT coefficients") +
        geom_abline(intercept=0, slope=1) + scale_color_discrete("") +
        ggtitle(paste0(name,"\nInpatient: MR-BRT (",round(df$mrbrt_inpatient/0.5,3),"), DisMod (",round(1/dmvs$value_inpatient[i],3),
                            ")\nExplicit: MR-BRT (",round(df$mrbrt_explicit/0.5,3),"), Dismod (",round(1/dmvs$value_explicit[i],3),")\n",note))
      print(g)

    ## Sometimes using nid == 999999 to indicate data to be used only for crosswalking
      df <- subset(df, nid != 999999)
      df <- subset(df, !is.na(bundle_id))

    ## Save the modified file ##
      df$note_modeler <- paste0(df$note_modeler, " | The crosswalks for cv_inpatient and cv_explicit were performed using MR-BRT. Inpatient data were
                                merged based on within study observations while explicit data had to be merged to nearby data based on location.")
      write.csv(df, "filepath", row.names=F)

##--------------------------------------------------------------------
  # Pull out new data and save in the Upload folder
    df$crosswalk_parent_seq <- ifelse(!is.na(df$seq),df$seq, df$seq_parent)
    df[is.na(df)] <- ""

  # Pull out new data, duplicate sexes (can't upload sex="Both"), remove group_review = 0, and save in the Upload folder
    xw_sex_df <- duplicate_sex_rows(df)
    xw_sex_df <- scalar_sex_rows(df, denominator_weights=diar_sex_weights, bounded=T, global_merge=T, title=name)

    xw_sex_df <- xw_sex_df[ , -which(names(xw_sex_df) %in% c("cv_explicit","cv_pcr","round_age_start","round_age_end","dummy_age_split","note_SR",
                                                                "cv_age_split","logit_mean","delta_logit_se", "pred_mean","pred_linear_se","year_id",
                                                             "age_sum","inpatient","is_reference","log_inpatient","se_log_inpatient",
                                                             "mrbrt_inpatient","mrbrt_se_inpatient","mrbrt_explicit","mrbrt_se_explicit","adjustment_type"))]
    xw_sex_df$group_review[is.na(xw_sex_df$group_review)] <- 1
    xw_sex_df <- subset(xw_sex_df, group_review != 0)
    xw_sex_df$group_review <- ifelse(xw_sex_df$specificity=="","",xw_sex_df$group_review)
    xw_sex_df[is.na(xw_sex_df)] <- ""
    xw_sex_df$crosswalk_parent_seq <- ifelse(xw_sex_df$crosswalk_parent_seq=="",xw_sex_df$seq,xw_sex_df$crosswalk_parent_seq)
    xw_sex_df$uncertainty_type <- ifelse(xw_sex_df$lower=="","", as.character(xw_sex_df$uncertainty_type))
    xw_sex_df$uncertainty_type_value <- ifelse(xw_sex_df$lower=="","",xw_sex_df$uncertainty_type_value)

    # After Crosswalking, resetting cases, sometimes cases > sample size
      xw_sex_df$cases <- as.numeric(xw_sex_df$cases)
      xw_sex_df$sample_size <- as.numeric(xw_sex_df$sample_size)
    xw_sex_df$cases <- ifelse(xw_sex_df$cases > xw_sex_df$sample_size, xw_sex_df$sample_size * 0.99, xw_sex_df$cases)

    # Apparently sometimes standard error > 1
      xw_sex_df$standard_error <- ifelse(xw_sex_df$standard_error > 1, 0.99, xw_sex_df$standard_error)

    # Remove data from Liverpool
      xw_sex_df <- subset(xw_sex_df, !(location_id %in% bad_ids))

    # Save step 2 and step 4 data separately
      xw_sex_df <- subset(xw_sex_df$bundle_id!="")
      step2 <- subset(xw_sex_df, gbd_round != 2019)
      step4 <- subset(xw_sex_df, gbd_round == 2019)

      write.xlsx(step2, "filepath", sheetName="extraction")
      write.xlsx(step4, "filepath", sheetName="extraction")

    # All data for Sensitivity/Specificity adjustment
      write.csv(xw_sex_df, "filepath", row.names=F)

}

  # Plot a scatter showing differences
  if(xw_transform == "log"){
    p <-  ggplot(out_res, aes(x=dismod_d1, y=ratio, col=etiology, shape = variable)) + geom_point(size = 3) + geom_abline(intercept=0, slope=1) + theme_bw() +
        scale_color_discrete("") + scale_shape_discrete("", labels=c("Single pathogen","Inpatient")) + geom_hline(yintercept=1, lty=2) + geom_vline(xintercept=1, lty=2) +
        xlab("Value from DisMod decomp 1") + ylab("Value from MR-BRT") + ggtitle("Product of values shown and cv_ == 1 is the adjusted value\nSo values greater than 1 indicate mean(cv_ == 1) is less than mean(cv_ == 0)")
    print(p)
  } else {
    p <-  ggplot(out_res, aes(x=dismod_d1, y=ratio, col=etiology, shape = variable)) + geom_point(size = 3) + geom_abline(intercept=0, slope=0.5) + theme_bw() +
      scale_color_discrete("") + scale_shape_discrete("", labels=c("Single pathogen","Inpatient")) + geom_hline(yintercept=0.5, lty=2) + geom_vline(xintercept=1, lty=2) +
      xlab("Value from DisMod decomp 1 (log)") + ylab("Value from MR-BRT (logit)") + ggtitle("Sum of values in logit space shown and cv_ == 1 is the adjusted value\nSo values greater than 0.5 indicate mean(cv_ == 1) is less than mean(cv_ == 0)")
    print(p)
  }

dev.off()

## Save the summary file of all the crosswalks
  setnames(out_res, "variable","crosswalk_name")
  out_res <- join(out_res, dmvs_full, by=c("etiology","crosswalk_name"))
write.csv(out_res, "filepath", row.names=F)

##--------------------------------------------------------------------------------------------

# ##############################################################################################
# ## Step 3.1: Run crosswalks in MR-BRT for Norovirus PCR and High Risk of Bias ----------------
# ##############################################################################################
## No longer making these adjustments.
## The high risk of bias is not statistically significant and not clearly defined for the
## motivation. Further, we are testing making the adjustment for non-PCR data using a crosswalk
## and Norovirus shouldn't be adjusted twice ##

##############################################################################################
## Step 4: Adjust for sensitivity/specificity like GBD 2017 approach but before modeling
##############################################################################################
sesp <- read.csv("filepath")

pdf("filepath", height=8, width=9)
  for(i in 1:11){
    name <- eti_info$name_colloquial[i]
    me_name <- eti_info$modelable_entity[i]
    me_id <- eti_info$modelable_entity_id[i]
    bundle <- eti_info$bundle_id[i]
    rei_name <- eti_info$rei_name[i]

    print(paste0("Adjusting for imperfect sensitivity/specificity for ", name))

    # Shouldn't have to age/sex split again, use this:
    df <- read.csv("filepath")
    df$crosswalk_mean <- df$mean

    ## Want to do this by draw.
      # df$age_median <- (df$age_end + df$age_start) / 2
      # df$age_bin <- cut(df$age_median, c(0,5,20,60,100), labels = c("0 to 5","6 to 20","21 to 60","60+"))
      # #df$age_bin <- cut(df$age_median, c(0,100), labels=c("All"))
      #
      # floors <- aggregate(mean ~ age_bin, data=df, function(x) median(x)*0.01)
      # setnames(floors, "mean","linear_floor")
      # df <- join(df, floors, by="age_bin")

      df$linear_floor <- median(df$mean[df$mean>0]) * 0.01
    # shift 0s up
      df$mean <- ifelse(df$mean==0, df$linear_floor, df$mean)

    # Prep proportion in logit space
      df$lg_mean <- logit(df$mean)
      
      df$delta_log_se <- sapply(1:nrow(df), function(i) {
        ratio_i <- df[i, "mean"]
        ratio_se_i <- df[i, "standard_error"]
        deltamethod(~log(x1/(1-x1)), ratio_i, ratio_se_i^2)
      })

    # equation is (proportion + specificity - 1) / (sensitivity + specificity - 1)
        # draw_matrix <- data.frame(nid=df$nid)
        #   for(d in 1:1000){
        #     d_prop <- inv.logit(rnorm(n=length(df$nid), df$lg_mean, df$delta_log_se))
        #     sp <- sesp[sesp$bundle_id==bundle,paste0("specificity_",d)]
        #     se <- sesp[sesp$bundle_id==bundle,paste0("sensitivity_",d)]
        #     d_adj <- (d_prop + sp - 1) / (se + sp - 1)
        #     d_adj <- ifelse(d_adj <= 0, df$linear_floor, ifelse(d_adj >= 1, 1 - df$linear_floor, d_adj))
        #     draw_matrix[,paste0("draw_",d)] <- d_adj
        #   }

    # More complicated but conceptually more similar to how PAF code does this:
        prop_matrix <- c()
        for(d in 1:1000){
          d_prop <- inv.logit(rnorm(n=length(df$nid), df$lg_mean, df$delta_log_se))
          prop_matrix <- cbind(prop_matrix, d_prop)
        }
        linear_floor <- rowMedians(prop_matrix) * 0.01
        draw_matrix <- data.frame(nid=df$nid)
        for(d in 1:1000){
          d_prop <- prop_matrix[,d]
          sp <- sesp[sesp$bundle_id==bundle,paste0("specificity_",d)]
          se <- sesp[sesp$bundle_id==bundle,paste0("sensitivity_",d)]
          d_adj <- (d_prop + sp - 1) / (se + sp - 1)
          d_adj <- ifelse(d_adj <= 0, linear_floor, ifelse(d_adj >= 1, 1 - linear_floor, d_adj))
          draw_matrix[,paste0("draw_",d)] <- d_adj
        }

    draw_mean <- rowMeans(draw_matrix[,2:1001])
    draw_se <- apply(draw_matrix[,2:1001], 1, function(x) sd(x))
    df$mean <- ifelse(df$cv_diag_pcr==0, draw_mean, df$mean)
    df$standard_error <- ifelse(df$cv_diag_pcr==0, draw_se, df$standard_error)

    ## Replace mean = 0 values
    df$mean <- ifelse(df$crosswalk_mean==0, 0, df$mean)

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

    df$note_modeler <- ifelse(df$cv_diag_pcr==0, paste0(df$note_modeler, "| These data were adjusted for diagnostic sensitivity and specificity of laboratory to molecular diagnostics."), df$note_modeler)
    df <- df[,-(which(names(df) %in% c("crosswalk_mean","age_median","age_bin","linear_floor","lg_mean","delta_log_se")))]

    ## Save the modified files for step 2 and 4 ##
      step2 <- subset(df, gbd_round!=2019)
      step4 <- subset(df, gbd_round==2019)

      write.csv(df, "filepath", row.names=F)
      write.xlsx(step2, "filepath", sheetName="extraction")
      write.xlsx(step4, "filepath", sheetName="extraction")

}
dev.off()

##############################################################################################
## Step 5: Adjust for lab/PCR ratio as a crosswalk
##############################################################################################
## This is being done in a separate file: adjust_data_pcr_premodel.R
#############################################################################

## Now I am going to build out some loops to upload, etc ####################
# 1). X_updated_bundle_decomp2 contains any data adjustments identified during crosswalk
# 2). bundle_reassign_non_age_specific resets non-age-specific data. It changes group_review = 0
      # for one of the rows with non-specific data and clears the other rows. For example:
      # if there are two rows with age_start = c(0,50) and age_end = c(51,99), the first will
      # be reset as 0,99 and the other row will be cleared from the database.
# 3). X_decomp2_age_split: These are the data after just age/sex splitting (crosswalk version 1)
# 4). X_decomp2_crosswalked: These are the data after age/sex splitting and MR-BRT crosswalks (crosswalk version 2)
# 5). X_decomp2_diagnostic_pcr: These are the data after age/sex splitting, MR-BRT, and PCR diagnostic adjustment (crosswalk version 3)
## For ETEC and EPEC, there is an additional one to upload. Following age_split, we have one for typical_EPEC and ST_ETEC adjustments!

for(i in 1:11){
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  m_name <- eti_info$modelable_entity_name[i]
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]

  print(paste0("Uploading modified data for ", name))

  # 1) Fix any wrong data
  # upload_bundle_data(bundle_id = bundle, filepath, decomp_step="step2")
  #
  # # 2) Reassign non-age-specific data
  # upload_bundle_data(bundle_id = bundle, filepath, decomp_step="step2")

##---------------------------------------------------------------------------##
  ## We need to get a single bundle_version_id for all crosswalk versions. ##
  # s <- save_bundle_version(bundle_id = bundle, decomp_step="step2")
  # s$bundle_version_id

  # Upload the sex-split raw data #
  # raw_data <- get_bundle_version(bundle_version_id = s$bundle_version_id)
  #   raw_data$crosswalk_parent_seq <- ""
  #   raw_data$seq_parent <- ""
  #   raw_data <- duplicate_sex_rows(raw_data)
  #   raw_data$group_review[is.na(raw_data$group_review)] <- 1
  #   raw_data <- subset(raw_data, group_review!=0 & !(location_id %in% bad_ids))
  #   # raw_data$group_review <- ""
  #   raw_data$group_review <- ifelse(raw_data$specificity=="","",raw_data$group_review)
  #   write.xlsx(raw_data, "filepath", sheetName="extraction")
  # save_crosswalk_version(bundle_version_id = s$bundle_version_id, data_filepath = "filepath",
  #                        description = "Unadjusted data only")
  # print("Uploaded raw data")

  # Upload the age_split data, only
  # save_crosswalk_version(bundle_version_id = s$bundle_version_id, data_filepath = "filepath",
  #                        description = "Changes to sex and age splitting")
  # print("Uploaded age split")
################################################################
  # # Upload the age_split and MR-BRT crosswalk data, only
  # save_crosswalk_version(bundle_version_id = s$bundle_version_id, data_filepath = "filepath",
  #                        description = "Inpatient/MSD MR-BRT crosswalks")
  # print("Uploaded MR-BRT")

  # # Upload the data with a crosswalk dependent on prevalence in Lab for PCR diagnostic
  # save_crosswalk_version(bundle_version_id = s$bundle_version_id, data_filepath = "filepath",
  #                        description = "Data with a PCR crosswalk dependent on lab prevalence")
  # print("Uploaded PCR prevalence crosswalk")
#################################################################
  # Upload the data with the sensitivity/specificity adjustment on Lab data #
    # Needs an iterative bundle
    # s <- save_bundle_version(bundle_id = bundle, decomp_step="iterative")
    #   s$bundle_version_id
    # save_crosswalk_version(bundle_version_id = s$bundle_version_id, data_filepath = "filepath",
    #                        description = "PCR adjusted by sensitivity/specificity (only lab data), fixed draws")
    # print("Uploaded PCR Se/Sp adjusted iterative")

  # Also save these data as a step 2 model so that they can exist within the decomp system #
    s <- save_bundle_version(bundle_id = bundle, decomp_step="step2")
    s$bundle_version_id
    save_crosswalk_version(bundle_version_id = s$bundle_version_id, data_filepath = "filepath",
                           description = "PCR adjusted by sensitivity/specificity (only lab data), fixed draws")
    print("Uploaded PCR Se/Sp adjusted for step 2")

#################################################################
  # If this is for EPEC, upload the data after adjusting for typical/atypical
  # if(bundle==7){
  #   save_crosswalk_version(bundle_version_id = s$bundle_version_id, data_filepath = "filepath",
  #                          description = "Typical EPEC only, age split, no MR-BRT")
  #   print("Uploaded typical EPEC")
  # }
  # # If this is for ETEC, upload the data after adjusting for ST-/LT-
  # if(bundle==8){
  #   save_crosswalk_version(bundle_version_id = s$bundle_version_id, data_filepath = "filepath",
  #                          description = "ST-ETEC only, age split, no MR-BRT")
  #   print("Uploaded ST-ETEC")
  # }
}


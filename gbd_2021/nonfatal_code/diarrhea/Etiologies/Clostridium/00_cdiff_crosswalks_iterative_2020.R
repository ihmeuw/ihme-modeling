#####################################################
## Crosswalk C diff hospital data outside DisMod ##
#####################################################
# Prepare your needed functions and information
library(metafor, lib.loc="FILEPATH")
library(doParallel) 
library(msm)
library(plyr)
library(boot)
library(data.table)
library(ggplot2)


source("/FILEPATH/get_bundle_data.R")
source("/FILEPATH/get_bundle_version.R")
source("/FILEPATH/upload_bundle_data.R")
source("/FILEPATH/save_bundle_version.R")
source("/FILEPATH/save_crosswalk_version.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/bundle_crosswalk_collapse.R")
source("/FILEPATH/mr_brt_functions.R")
source("/FILEPATH/plot_mr_brt_function.R")
source("/FILEPATH/run_mr_brt_function.R")

## Note which is being used.
source("/FILEPATH/cdiff_age_split_mrbrt_weights.R")
source("/FILEPATH/cdiff_sex_split_mrbrt_weights.R")

locs <- get_location_metadata(location_set_id = 35, gbd_round_id=7)

## Find Saudi Arabia subnationals, England areas (like Northwest, West Midlands) to remove ##
locs_old <- read.csv("/FILEPATH/ihme_loc_metadata_2016.csv")
bad_ids <- c(locs_old$location_id[locs_old$parent_id==152], locs_old$location_id[locs_old$parent_id==73 & locs_old$most_detailed==0])
bad_ids <- ifelse(bad_ids==93,95,bad_ids)

df <- get_bundle_version(38723, fetch="all")

df$original_mean <- df$mean

df <- subset(df, is_outlier==0)
df$group_review[is.na(df$group_review)] <- 1
df <- subset(df, group_review != 0)
df$unit_value_as_published <- 1
df$unit_type <- "Person"
df$recall_type <- "Point"
df$urbanicity_type <- "Mixed/both"
df$extractor <- ifelse(df$extractor=="","clinical_info", as.character(df$extractor))

df$cv_inpatient <- ifelse(df$extractor == "clinical_info", 1, 0)

df$is_reference <- ifelse(df$cv_inpatient==1,0,1)

df$sample_size[is.na(df$sample_size)] <- 1000
missing_cases <- df$mean * df$sample_size
df$cases <- ifelse(is.na(df$cases), missing_cases, df$cases)

##--------------------------------------------------------------------
# Save those data as a raw bundle for modeling
  # Part 1: Test a dummy variable where age_start/end rounded doesn't equal age_start/end
  df$round_age_start <- round(df$age_start, 1)
  df$round_age_end <- round(df$age_end, 1)
  df$age_sum <- df$age_start + df$age_end

  df$dummy_age_split <- with(df, ifelse(age_end > 10, ifelse(df$round_age_start != df$age_start, 1, ifelse(df$round_age_end != df$age_end, 1, 0)), 0))

  # Part 2, where the age_range is not 0-99
  #df$note_modeler <- ifelse(df$age_start == 0 & df$age_end == 99, paste0("Original age end 99. ", df$note_modeler), as.character(df$note_modeler))
  df$dummy_age_split <- ifelse(df$dummy_age_split==1,1,ifelse(df$age_start == 0 & df$age_end == 99,1,0))

  # Part 3, where the age_start/end rounded isn't equal to the unrounded value
  df$dummy_age_split <- ifelse(df$dummy_age_split==1,1,ifelse(df$note_modeler %like% "riginal age end", 1, 0))

  age_specific <- subset(df, !(note_modeler %like% "riginal age end"))
  age_specific <- subset(df, dummy_age_split == 0)
  age_non_specific <- subset(df, note_modeler %like% "riginal age end")
  age_non_specific <- subset(df, dummy_age_split == 1)

  # Some age-split data have already been duplicated, all rows should be removed
  age_specific$cv_age_split <- 0
  age_non_specific$cv_age_split <- 1

  ##------------------------------------------------------
  # Get rid of now duplicated age split data #

  age_group_review <- subset(age_non_specific, age_sum > 100)
  age_group_review <- data.frame(seq = age_group_review[,"seq"])

  age_non_specific <- subset(age_non_specific, age_sum < 100)

  df <- rbind(age_specific, age_non_specific)

  ## Determine where age splitting should occur. ##
  age_map <- read.csv("/FILEPATH/age_mapping.csv") 
  age_map <- age_map[age_map$age_pull==1,c("age_group_id","age_start","age_end","order","age_pull")]
  age_cuts <- c(0,1,5,20,40,65,100)
  length <- length(age_cuts) - 1
  age_map$age_dummy <- cut(age_map$age_end, age_cuts, labels = paste0("Group ", 1:length))

  ## Create prevalence weights to age-split the data from the global diarrhea prevalence age pattern in GBD 2019 ##
  source("/FILEPATH/get_outputs.R")

  dmod_global <- get_outputs(topic="cause", cause_id=302, age_group_id = age_map$age_group_id, year_id=1990:2019, location_id=1, sex_id=3, measure_id=5, metric_id=1, gbd_round_id=6, decomp_step="step5", version="latest")
  dmod_global$number <- dmod_global$val
  denom_weights <- dmod_global
  denom_weights$age_dummy <- with(denom_weights, ifelse(age_group_id < 5, "Group 1", ifelse(age_group_id ==5, "Group 2",
                                                                                            ifelse(age_group_id < 9, "Group 3", ifelse(age_group_id < 13, "Group 4",
                                                                                                                                       ifelse(age_group_id < 17, "Group 5",
                                                                                                                                              ifelse(age_group_id < 30, "Group 6", "Group 7")))))))
  denom_age_groups <- aggregate(number ~ location_id + year_id + age_dummy, data=denom_weights, function(x) sum(x))
  denom_all_ages <- aggregate(number ~ location_id + year_id, data=denom_weights, function(x) sum(x))
  denom_all_ages$total_number <- denom_all_ages$number

  denominator_weights <- join(denom_age_groups, denom_all_ages[,c("location_id","year_id","total_number")], by=c("location_id","year_id"))

  # Age splitting function. It takes
  # an estimated ratio of the observed to expected data for all age-specific
  # data points and runs that through MR-BRT. The result is then predicted
  # for six age groups which are then merged 1:m with the age-non-specific
  # data to make six new points with the new proportion equal to the
  # original mean times the modeled ratio. It keeps a set of rows
  # from the age-non-specific data to mark as group_review = 1.
  source("/FILEPATH/cdiff_age_split_dismod.R")

  age_df <- age_split_dismod(df[df$measure=="incidence",], denominator_weights, model_version_id = 464714, bounded=F, title="Clostridium",
                             age_map = age_map, gbd_round_id=6, measure_id=6, location_id=1, sex_id=2)


  age_df$group_review <- ""
  age_df$crosswalk_parent_seq <- ""
  age_df$seq_parent <- ""
  write.csv(age_df, paste0(getwd(), "/cdiff_age_split_gbd2020.csv"), row.names=F)

  #sex_split <- duplicate_sex_rows(age_df)

  source("/FILEPATH/get_outputs.R")
    dmod_global <- get_outputs(topic="cause", cause_id=302, age_group_id = 22, year_id=2019, location_id=1, sex_id=c(1,2), measure_id=6, metric_id=1, gbd_round_id=6, decomp_step="step5")
    dmod_global$number <- dmod_global$val

  sex_split <- scalar_sex_rows(age_df, denominator_weights=dmod_global, bounded=F, global_merge = T)

  sex_split[is.na(sex_split)] <- ""
  sex_split$group_review <- ifelse(sex_split$group != "", 1, "")
  
  write.csv(sex_split, paste0(getwd(), "/cdiff_age_sex_split_gbd2020.csv"), row.names=F)




##--------------------------------------------------------------------------------------------------------
  sex_split <- read.csv(paste0(getwd(),"/FILEPATH/cdiff_age_sex_split_gbd2020.csv"))
  sex_split <- data.table(sex_split)
  temp <- get_bundle_version(38723, fetch="all")
  sex_split <- rbindlist(list(sex_split, temp[nid==475945]), fill=TRUE) # Manually adding in dropped EMR rows to prep for save xwalk version
  df <- sex_split


  df[is.na(df)] <- ""
  df <- data.frame(df)
  df <- df[, -which(names(df) %in% c("cv_age_split","X","age_mid","X_intercept","X_age_mid","Y_mean","Y_negp","Y_mean_lo","Y_mean_hi",
                                     "Y_mean_fe","Y_mean_fe_lo","Y_mean_fe_hi","se","ratio","linear_se","og_mean"))]
  df <- subset(df, !(location_id %in% bad_ids))
  df <- data.table(df)
  df[nid==141108, is_outlier:=1]
  
  write.xlsx(df, paste0(getwd(),"/FILEPATH/cdiff_age_sex_split_gbd2020_upload.xlsx"), sheetName="extraction")

##------------------------------------------------------------
  source("/FILEPATH/save_crosswalk_version.R")
  ## Save crosswalk_version
  save_crosswalk_version(bundle_version_id = 38723, data_fileFILEPATH = paste0(getwd(),"/FILEPATH/cdiff_age_sex_split_gbd2020_upload.xlsx"),
                         description = "No data crosswalks, age split, sex split using ratio, new EMR, bv id 38723, cf2, outliered nid 141108")
  

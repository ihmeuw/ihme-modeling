#####################################################
## Crosswalk C diff hospital data outside DisMod ##
#####################################################
# Prepare your needed functions and information
library(metafor)
library(msm)
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

## Find Saudi Arabia subnationals, England areas (like Northwest, West Midlands) to remove ##
locs_old <- read.csv("filepath")
bad_ids <- c(locs_old$location_id[locs_old$parent_id==152], locs_old$location_id[locs_old$parent_id==73 & locs_old$most_detailed==0])
bad_ids <- ifelse(bad_ids==93,95,bad_ids)

df <- get_bundle_version(bundle_version_id = 3242)

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

## Lots of data are missing cases and sample size. So, in a Poisson, mean = variance.
# Leaving means alone, can make up dummy numbers for cases and sample size, 
df$sample_size[is.na(df$sample_size)] <- 1000
missing_cases <- df$mean * df$sample_size
df$cases <- ifelse(is.na(df$cases), missing_cases, df$cases)

##--------------------------------------------------------------------
# Save those data as a raw bundle for modeling
  # Part 1
  # Test a dummy variable where age_start/end rounded doesn't equal age_start/end
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
  # Assuming that in those splits, only one data point will start with 0 age
 
  age_group_review <- subset(age_non_specific, age_sum > 100)
  age_group_review <- data.frame(seq = age_group_review[,"seq"])

  age_non_specific <- subset(age_non_specific, age_sum < 100)

  df <- rbind(age_specific, age_non_specific)

  ## Determine where age splitting should occur. ##
  age_map <- read.csv("filepath")
  age_map <- age_map[age_map$age_pull==1,c("age_group_id","age_start","age_end","order","age_pull")]
  age_cuts <- c(0,1,5,20,40,65,100)
  length <- length(age_cuts) - 1
  age_map$age_dummy <- cut(age_map$age_end, age_cuts, labels = paste0("Group ", 1:length))

  ## Create prevalence weights to age-split the data from the global diarrhea prevalence age pattern in GBD 2017 ##
  dmod_global <- get_outputs(topic="cause", cause_id=302, age_group_id = age_info$age_group_id, year_id=1990:2017, location_id=1, sex_id=3, measure_id=5, metric_id=1, gbd_round_id=5)
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

  # This is the function to do the age splitting. It takes
  # an estimated ratio of the observed to expected data for all age-specific
  # data points and runs that through MR-BRT. The result is then predicted
  # for six age groups which are then merged 1:m with the age-non-specific
  # data to make six new points with the new proportion equal to the
  # original mean times the modeled ratio. It keeps a set of rows
  # from the age-non-specific data to mark as group_review = 1.
  source("/filepath/age_split_dismod.R")
  age_df <- age_split_dismod(df[df$measure=="incidence",], denominator_weights, model_version_id = 330185, bounded=F, title="Clostridium",
                             age_map = age_map, gbd_round_id=6, measure_id=6, location_id=1, sex_id=2)


  age_df$group_review <- ""
  age_df$crosswalk_parent_seq <- ""
  age_df$seq_parent <- ""
  write.csv(age_df, "filepath", row.names=F)

  #sex_split <- duplicate_sex_rows(age_df)

    dmod_global <- get_outputs(topic="cause", cause_id=302, age_group_id = 22, year_id=2017, location_id=1, sex_id=c(1,2), measure_id=6, metric_id=1, gbd_round_id=5)
    dmod_global$number <- dmod_global$val

  sex_split <- scalar_sex_rows(age_df, denominator_weights=dmod_global, bounded=F, global_merge = T)

  sex_split[is.na(sex_split)] <- ""
  sex_split$group_review <- ifelse(sex_split$group != "", 1, "")

  write.csv(sex_split, "filepath", row.names=F)

##-----------------------------------------------------------------------
  ## Upload a crosswalk_version for that ##
  # save_crosswalk_version(bundle_version_id = 791, data_filepath = "filepath", description = "Sex split data")

##-----------------------------------------------------------------------
## Quite a bit of testing reveals no systematic difference that I can
## discern between inpatient/reference or claims/reference. 
## Trying a model without crosswalks, just age and sex splitting.


## Crosswalk incidence
  df <- age_df
  df <- subset(df, measure=="incidence")
  df <- join(df, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")

  # Try to determine if the clinical data are systematically different
  df$extractor <- ifelse(df$extractor=="","clinical_info", as.character(df$extractor))

  ggplot(subset(df, extractor == "clinical_info"), aes(x=factor(nid), y=mean, col=clinical_data_type)) + geom_boxplot() + scale_y_log10() + theme(axis.text.x=element_text(angle=90, hjust=1))
  ggplot(subset(df, substr(ihme_loc_id,1,3)=="USA"), aes(x=factor(age_start), y=mean, col=clinical_data_type)) + geom_boxplot() + scale_y_log10()

  # Single crosswalk.
  #df$cv_inpatient <- ifelse(df$extractor == "clinical_info", 1, 0)
  # Claims and literature are comparable:
    ggplot(df, aes(x=clinical_data_type, y=mean, col=clinical_data_type)) + geom_boxplot() + scale_y_log10()
    df$cv_inpatient <- ifelse(df$clinical_data_type == "inpatient", 1, 0)

  # Pay attention to what is used for the reference ##
  # df$is_reference <- ifelse(df$cv_inpatient==1,0,1)
  df$is_reference <- ifelse(df$clinical_data_type=="", 1, 0)
  df$sample_size <- as.numeric(df$sample_size)

  cv_inpatient <- bundle_crosswalk_collapse(df, covariate_name="cv_inpatient", age_cut=c(0,1,5,10,20,30,40,50,60,70,80,90,100), year_cut=c(seq(1980,2015,5),2019), merge_type="between", location_match="exact")
    cv_inpatient$age_mid <- (cv_inpatient$age_end + cv_inpatient$age_start) / 2

  df$cv_marketscan <- ifelse(df$clinical_data_type=="claims",1,0)
  cv_marketscan <- bundle_crosswalk_collapse(df, covariate_name="cv_marketscan", age_cut=c(0,1,5,10,20,30,40,50,60,70,80,90,100), year_cut=c(seq(1980,2015,5),2019), merge_type="between", location_match="exact")
    cv_marketscan$age_mid <- (cv_marketscan$age_end + cv_marketscan$age_start) / 2

  # Look at the resulting data:
    name <- "Clostridium"

      p2 <- ggplot(cv_inpatient, aes(x=age_bin, y=ratio)) + geom_boxplot() + theme_bw() + ggtitle(paste0("Inpatient ratio by age for ", name)) + scale_y_log10()
      p3 <- ggplot(cv_inpatient, aes(x=year_bin, y=ratio)) + geom_boxplot() + theme_bw() + ggtitle(paste0("Inpatient ratio by year for ", name)) + scale_y_log10() + geom_point(alpha=0.2)
      p4 <- ggplot(cv_inpatient, aes(x=factor(location_match), y=ratio)) + geom_boxplot() + theme_bw() + ggtitle(paste0("Clinical data ratio by location_id for C difficile")) + 
        scale_y_log10() + theme(axis.text.x=element_text(angle=90, hjust=1))
      p5 <- ggplot(cv_inpatient, aes(x=age_mid, y=ratio)) + geom_point() + theme_bw() + scale_y_log10() + stat_smooth(method="loess", se=F)
      print(p2)
      print(p3)
      print(p4)
      print(p5)
      ggplot(df, aes(x=factor(age_start), y=mean, col=factor(clinical_data_type))) + geom_boxplot(position=position_dodge(width=0.8)) + theme_bw() + scale_y_log10()
        ggplot(subset(df, location_name=="Washington"), aes(x=factor(age_start), y=mean, col=factor(clinical_data_type))) + geom_boxplot(position=position_dodge(width=0.8)) + theme_bw() + scale_y_log10()
      ggplot(df, aes(x=factor(year_start), y=mean, col=factor(clinical_data_type))) + geom_boxplot(position=position_dodge(width=0.8)) + theme_bw() + scale_y_log10()

    cv_inpatient <- cv_inpatient[!duplicated(cv_inpatient$ratio),]
    cv_inpatient$match_nid <- paste0(cv_inpatient$nid,"_",cv_inpatient$n_nid)

    cv_marketscan <- cv_marketscan[!duplicated(cv_marketscan$ratio),]
    cv_marketscan$match_nid <- paste0(cv_marketscan$nid,"_",cv_marketscan$n_nid)

#   # Save that file for a record
#   # write.csv(cv_inpatient, "filepath", row.names=F)

##--------------------------------------------------------------------------------------------------------
## Great, now we need to get all the sex_specific data, non-incidence data. Append, crosswalk, save. ##
  sex_split <- read.csv("filepath")

  df <- sex_split

  df[is.na(df)] <- ""
  df <- df[, -which(names(df) %in% c("cv_age_split","X","age_mid","X_intercept","X_age_mid","Y_mean","Y_negp","Y_mean_lo","Y_mean_hi",
                                     "Y_mean_fe","Y_mean_fe_lo","Y_mean_fe_hi","se","ratio","linear_se","og_mean"))]
  df <- subset(df, !(location_id %in% bad_ids))

  write.csv(df, "filepath", row.names=F)

##------------------------------------------------------------
  ## Save crosswalk_version
  save_crosswalk_version(bundle_version_id = 3242, data_filepath = "filepath",
                         description = "No data crosswalks, age split, sex split using ratio")

  df <- read.xlsx("filepath")

  ## Try a version dropping all claims data
  nclaims <- subset(df, clinical_data_type != "claims")
    write.xlsx(nclaims, "filepath", sheetName="extraction")

  ## And one without China data
  nchina <- subset(nclaims, nid != 337619)
    write.xlsx(nchina, "filepath", sheetName="extraction")

  save_crosswalk_version(bundle_version_id = 3242, data_filepath = "filepath",
                         description = "No data crosswalks, age split, sex split using ratio, no claims")
  save_crosswalk_version(bundle_version_id = 3242, data_filepath = "filepath",
                         description = "No data crosswalks, age split, sex split using ratio, no China & no claims")

## ---------------------------------------------------------------------------------------
## Get step 4 clinical data, upload as crosswalk version ##
  save_bundle_version(bundle_id = 14, decomp_step = "step4", include_clinical = T)

  # cdiff_s4 <- get_bundle_version(bundle_version_id = 13352)
  #
  # cdiff_s4$crosswalk_parent_seq <- cdiff_s4$seq
  # cdiff_s4$seq <- ""
  #
  # write.xlsx(cdiff_s4, "filepath", sheetName="extraction")

    cdiff_s4 <- data.frame(cdiff_s4)
  write.xlsx(cdiff_s4[cdiff_s4$clinical_data_type != "claims",], "filepath", sheetName="extraction")

  save_crosswalk_version(bundle_version_id = 13352, data_filepath = "filepath",
                         description = "Step 4 data to be used for model, no claims")



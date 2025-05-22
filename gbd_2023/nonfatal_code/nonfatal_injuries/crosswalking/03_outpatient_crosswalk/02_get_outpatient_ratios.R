# team: GBD Injuries
# project: Crosswalking for GBD 
# script: Prep data for MR-BRT crosssalk

library(reticulate)
# Filepath to use shared code for IHME crosswalk package
reticulate::use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")

# bundle_df <- read.csv("/share/injuries/crosswalks/bundle_versions.csv")
# bundle_df <- bundle_df[!is.na(bundle_df$save),]
# bundles <- bundle_df$bundle_id

# Specific bundles to update 
bundles <- c(6749, 276, 370, 264, 265, 274, 356:359)

for (i in 1:length(bundles)) {
  
  bundle <- bundles[i]
  print(bundle)
  
  # get all clinical data saved
  ci_data <-
    read.csv(
      paste0(
        "FILEPATH/bundle_",
        as.character(bundle),
        ".csv"
      )
    )
  
  # for all bundles except 271, the only location/year matches we have are USA data
  if (bundle != 271) {
    ci_data <- ci_data[ci_data$location_id==102,]
  }
  
  ci_data <- ci_data[, c("location_id", "sex", "year_start", "year_end", "age_start", 
                         "age_end", "mean", "standard_error", "clinical_data_type")]
  
  # inpatient data is in 5-year bins
  ci_data[ci_data$clinical_data_type=="inpatient" & ci_data$year_start==1993, "year_id"] <- 1
  ci_data[ci_data$clinical_data_type=="inpatient" & ci_data$year_start==1998, "year_id"] <- 2
  ci_data[ci_data$clinical_data_type=="inpatient" & ci_data$year_start==2003, "year_id"] <- 3
  ci_data[ci_data$clinical_data_type=="inpatient" & ci_data$year_start==2008, "year_id"] <- 4
  
  # outpatient data is in single years
  ci_data[ci_data$clinical_data_type=="outpatient" & ci_data$year_start %in% seq(1993, 1997, 1), "year_id"] <- 1
  ci_data[ci_data$clinical_data_type=="outpatient" & ci_data$year_start %in% seq(1998, 2002, 1), "year_id"] <- 2
  ci_data[ci_data$clinical_data_type=="outpatient" & ci_data$year_start %in% seq(2003, 2007, 1), "year_id"] <- 3
  ci_data[ci_data$clinical_data_type=="outpatient" & ci_data$year_start %in% seq(2008, 2012, 1), "year_id"] <- 4
  
  # match new age groups in inpatient to old age groups in outpatient
  ci_data[ci_data$clinical_data_type=="inpatient" & ci_data$age_start==1, "age_end"] <- 4
  ci_data[ci_data$clinical_data_type=="inpatient" & ci_data$age_end==4, "age_start"] <- 1
  
  # clean the overall dataframe
  ci_data <- ci_data[!is.na(ci_data$year_id),]
  ci_data <- ci_data[, c("location_id", "sex", "year_id", "age_start", "age_end", 
                         "mean", "standard_error", "clinical_data_type")]
  
  # define age midpoint for matching (instead of age_start, age_end)
  ci_data[ci_data$age_end == 124, "age_end"] <- 99
  ci_data$age_midpoint <- (ci_data$age_start + ci_data$age_end)/2
  
  # rename columns before merging
  inpatient <- ci_data[ci_data$clinical_data_type=="inpatient",]
  colnames(inpatient)[colnames(inpatient)=="mean"] <- "inc_ref"
  colnames(inpatient)[colnames(inpatient)=="standard_error"] <- "inc_se_ref"
  colnames(inpatient)[colnames(inpatient)=="clinical_data_type"] <- "dorm_ref"
  
  outpatient <- ci_data[ci_data$clinical_data_type=="outpatient",]
  colnames(outpatient)[colnames(outpatient)=="mean"] <- "inc_alt"
  colnames(outpatient)[colnames(outpatient)=="standard_error"] <- "inc_se_alt"
  colnames(outpatient)[colnames(outpatient)=="clinical_data_type"] <- "dorm_alt"
  
  # get matched data (match on age_midpoint instead of age_start & age_end)
  df_matched <- merge(inpatient, outpatient, by=c("location_id", "sex", "year_id", "age_midpoint"))
  
  # deal with mean 0
  df_matched <- df_matched[df_matched$inc_alt != 0,]
  df_matched <- df_matched[df_matched$inc_ref != 0,]

  # delta transform means and standard errors
  dat_diff <- as.data.frame(cbind(
    delta_transform(
      mean = df_matched$inc_alt, 
      sd = df_matched$inc_se_alt,
      transformation = "linear_to_log"),
    delta_transform(
      mean = df_matched$inc_ref, 
      sd = df_matched$inc_se_ref,
      transformation = "linear_to_log")))
  
  names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")
  
  # calculate differences between log means
  df_matched[, c("log_diff", "log_diff_se")] <- calculate_diff(
    df = dat_diff, 
    alt_mean = "mean_alt", alt_sd = "mean_se_alt",
    ref_mean = "mean_ref", ref_sd = "mean_se_ref" )
  
  # write to scratch location for testing, don't want to overwrite old results
  write.csv(df_matched, 
            paste0("FILEPATH/bundle_", 
                   as.character(bundle), ".csv"), row.names = FALSE)
  
}

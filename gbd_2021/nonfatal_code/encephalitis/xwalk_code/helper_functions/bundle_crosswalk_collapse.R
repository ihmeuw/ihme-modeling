# The intention for this function is to create a dataframe that is the collapsed
# and merged data that will be used in crosswalks. It subsets out the reference
# data in a bundle and the non-reference data. The user has the choice of how to merge
# the data depending on if there is within study information or not.
# It cuts these data depending on where the user wants them by age and year. The function
# returns a dataframe that is prepped for calculating the mean effect for the ratio
# in a crosswalk that can be used either in rma() or in MR-BRT.

# df is the dataframe (i.e. bundle data)
# covariate_name is the name of the binary column for the crosswalk (i.e. cv_)
# reference_name is the name of the binary column for the reference definition
# merge_type is how you want the data to be merged by study. The options are
# "within" means an exact match within a single study (merges by NID, age_start, age_end, year_start, year_end, sex)
# "between" means a close match by location, year (binned by year_cut argument), and age (binned by age_cut argument)
# Currently, if "within" is chosen as the merge_type, it merges data for exact matches so age_cut and year_cut only apply for "between" option
# location_match is at what level do you want your data merged by location, meaning how close geographically is a match?
# "exact" means it merges by location_id
# "country" means it merges by country (collapses subnationals into single value)
# "region" means it merges at GBD region (collapses all regional data into single value)
# "super" means it merges at GBD super region (collapses all super regional data into single value)
# age_cut is where you want the data to be merged by age (bins data at those values and aggregates). If you don't want this option, set to c(0,100)
# year_cut is where you want the data to be merged by year (bins data at thos values and aggregates). If you don't want this option, set to c(1980,2019)


bundle_crosswalk_collapse <- function(df,
                                      gbd_round_id,
                                      decomp_step,
                                      covariate_name,
                                      reference_name,
                                      age_cut=c(0,100),
                                      year_cut=c(1980,2019),
                                      merge_type="within",
                                      location_match="exact",
                                      include_logit = T){
  library(plyr)
  library(metafor)
  library(msm)

  locs <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step)

  # set cases (used in aggregation) to be the product of mean and sample_size.

  # get missing sample size before doing this
  source("filepath/get_row_populationV2.R")
  df <- add_population_cols(df, gbd_round = 7, decomp_step = decomp_step)

  df[is.na(cases), cases := mean*sample_size]

  # Subset outliers
  # df <- subset(df, year_end > min(year_cut) & is_outlier == 0)

  # Create a working dataframe
  wdf <- df[,c("location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
               reference_name,covariate_name), with = F]
  wdf <- join(wdf, locs[,c("location_id","ihme_loc_id","super_region_name","region_name")], by="location_id")

  # Create indicators for merging
  if(location_match=="exact"){
    wdf$location_match <- wdf$location_id
  } else if(location_match=="country"){
    wdf$location_match <- substr(wdf$ihme_loc_id,1,3)
  } else if(location_match=="region"){
    wdf$location_match <- wdf$region_name
  } else if(location_match=="super"){
    wdf$location_match <- wdf$super_region_name
  } else{
    print("The location_match argument must be [exact, country, region, super]")
  }

  wdf$age_median <- (wdf$age_end + wdf$age_start) / 2
  wdf$age_bin <- cut(wdf$age_median, age_cut)

  wdf$year_median <- (wdf$year_end + wdf$year_start) / 2
  wdf$year_bin <- cut(wdf$year_median, year_cut)


  ## Collapse to the desired merging ##  ## Create reference and non-reference data frames ##
  ref <- subset(wdf, get(reference_name) == 1)
  nref <- subset(wdf, get(covariate_name) == 1)
  setnames(nref, c("mean","standard_error","cases","sample_size"), c("n_mean","n_standard_error","n_cases","n_sample_size"))

  if(merge_type=="within"){
    # Use this to get exact demographic matches
    wmean <- merge(ref[,c("nid","cases","sample_size","age_start","age_end","year_start","year_end","age_bin","year_bin","location_match","sex")],
                   nref[,c("nid","n_cases","n_sample_size","age_start","age_end","year_start","year_end","age_bin","year_bin","location_match","sex")],
                   by=c("nid","age_start","age_end","year_start","year_end","age_bin","year_bin","location_match","sex"))
    wmean <- unique(wmean)

  } else if(merge_type=="between") {
    ## Aggregate by NID ##
    nref$n_nid <- nref$nid
    aref <- aggregate(cbind(sample_size, cases) ~ age_bin + year_bin + location_match + nid, data=ref, FUN=sum)
    anref <- aggregate(cbind(n_sample_size, n_cases) ~ age_bin + year_bin + location_match + n_nid, data=nref, FUN=sum)

    wmean <- merge(aref, anref, by=c("age_bin","location_match","year_bin"))
  } else {
    print("The merge_type argument must be either 'within' (within a single NID) or 'between' (matched on location, year, age)")
  }

  # Get the ratio
  wmean$mean <- wmean$cases/wmean$sample_size
  wmean$n_mean <- wmean$n_cases/wmean$n_sample_size
  wmean$standard_error <- sqrt(wmean$mean * (1-wmean$mean)/wmean$sample_size)
  wmean$n_standard_error <- sqrt(wmean$n_mean * (1-wmean$n_mean)/wmean$n_sample_size)

  # if zeros still exist after aggregation, drop them
  wmean <- as.data.table(wmean)
  wmean <- wmean[mean > 0 & n_mean > 0 & standard_error > 0 & n_standard_error > 0]

  # calculate the ratio with ALTERNATIVE as the NUMERATOR
  wmean$ratio <- (wmean$n_mean) / (wmean$mean)

  # Gives really high estimates of SE but is what is in guidance document
  wmean$se <- sqrt((wmean$mean^2 / wmean$n_mean^2) * (wmean$standard_error^2/wmean$mean^2 + wmean$n_standard_error^2/wmean$n_mean^2))

  # Lower, more plausible in my opinion
  #wmean$se <- sqrt(wmean$mean / wmean$n_mean * (wmean$standard_error^2/wmean$mean + wmean$n_standard_error^2/wmean$n_mean))

  # Use new crosswalk package to calculate ratios
  wmean[, c("logit_mean_alt", "se_logit_mean_alt") := data.table(delta_transform(mean = n_mean, sd = n_standard_error, transformation = "linear_to_logit"))]
  wmean[, c("logit_mean_ref", "se_logit_mean_ref") := data.table(delta_transform(mean = mean, sd = standard_error, transformation = "linear_to_logit"))]

  wmean[, logit_diff := logit_mean_alt - logit_mean_ref]
  wmean[, logit_diff_se := sqrt(se_logit_mean_alt^2 + se_logit_mean_ref^2)] # se for logit difference

  wmean[, c("log_mean_alt", "se_log_mean_alt") := data.table(delta_transform(mean = n_mean, sd = n_standard_error, transformation = "linear_to_log"))]
  wmean[, c("log_mean_ref", "se_log_mean_ref") := data.table(delta_transform(mean = mean, sd = standard_error, transformation = "linear_to_log"))]

  wmean[, c("log_diff", "log_diff_se")] <- calculate_diff(
    df = wmean,
    alt_mean = "log_mean_alt", alt_sd = "se_log_mean_alt",
    ref_mean = "log_mean_ref", ref_sd = "se_log_mean_ref" )

  # Pull out age_start, age_end, year_start, year_end
  # Using substr now
  if(merge_type == "between"){
    age_list <- data.frame(matrix(unlist(strsplit(as.character(wmean$age_bin),",")), ncol=2, byrow=TRUE))
    year_list <- data.frame(matrix(unlist(strsplit(as.character(wmean$year_bin),",")), ncol=2, byrow=TRUE))

    wmean$year_start <- as.numeric(substr(year_list$X1,2,5))
    wmean$year_end <- as.numeric(substr(year_list$X2,1,4))

    wmean$age_start <- as.numeric(substr(age_list$X1,2,4))
    wmean$age_end <- as.numeric(gsub(pattern = "]",replacement = "", age_list$X2))
  }

  # prepare formatting for code to follow
  if (location_match == "exact"){
    setnames(wmean, "location_match", "location_id")
    wmean <- merge(wmean, locs, by = "location_id")
    wmean[, ihme_loc_abv := substr(ihme_loc_id,1,3)]
  }
  wmean$alt_dorms <- covariate_name
  wmean$ref_dorms <- reference_name

  # Return the dataframe for further analysis
  return(wmean)
}

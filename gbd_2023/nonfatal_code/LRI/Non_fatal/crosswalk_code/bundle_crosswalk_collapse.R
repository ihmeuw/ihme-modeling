## This version of bundle_crosswalk_collapse is specifically for LRI

bundle_crosswalk_collapse <- function(df, covariate_name, age_cut=c(0,100), year_cut=c(1980,2019), merge_type="within", location_match="exact", include_logit = F, release){
  library(plyr)
  library(metafor)
  library(msm)
  source("FILEPATH")
  
  locs <- as.data.table(get_location_metadata(location_set_id = 35, release_id = release))
  
  # set cases (used in aggregation) to be the product of mean and sample_size.
  df$cases <- df$mean * df$sample_size
  
  # Don't include group_review data #
  df$group_review[is.na(df$group_review)] <- 1
  df <- subset(df, group_review != 0)
  
  # Subset outliers
  df <- subset(df, year_end > min(year_cut) & is_outlier == 0)
  
  # Create a working dataframe
  wdf <- df[,c("location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
               "is_reference",covariate_name)]
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
  
  wdf$ihme_loc_abv <- substr(wdf$ihme_loc_id,1,3)
  
  wdf$age_median <- (wdf$age_end + wdf$age_start) / 2
  wdf$age_bin <- cut(wdf$age_median, age_cut)
  
  wdf$year_median <- (wdf$year_end + wdf$year_start) / 2
  wdf$year_bin <- cut(wdf$year_median, year_cut)
  
  
  ## Collapse to the desired merging ##  ## Create reference and non-reference data frames ##
  wdf$cv <- wdf[,covariate_name]
  ref <- subset(wdf, is_reference == 1)
  nref <- subset(wdf, cv == 1)
  setnames(nref, c("mean","standard_error","cases","sample_size"), c("n_mean","n_standard_error","n_cases","n_sample_size"))
  
  if(merge_type=="within"){
    
    # Use this to get exact demographic matches
    wmean <- merge(ref[,c("nid","cases","sample_size","age_start","age_end","year_start","year_end","age_bin","year_bin","location_match","sex")],
                   nref[,c("nid","n_cases","n_sample_size","age_start","age_end","year_start","year_end","age_bin","year_bin","location_match","sex")],
                   by=c("nid","age_start","age_end","year_start","year_end","age_bin","year_bin","location_match","sex"))
    wmean <- unique(wmean)
    
  } else if(merge_type=="between") {
    
    ## Aggregate by NID (study)? ##
    if (covariate_name == "cv_inpatient"){
      
      nref <- as.data.table(nref)
      england <- nref[nid == 234762]
      nref <- nref[!(nid == 234762)]
      nref <- as.data.frame(nref)
      
      # Aggregate england alone
      england <- england[location_name %in% c("North East England", "North West England", "South East England", "South West England")]
      
      # aggregate england
      england <- as.data.frame(england)
      england$n_nid <- england$nid
      aengland <- aggregate(cbind(n_sample_size, n_cases) ~ age_bin + year_bin + location_match + n_nid, data=england, FUN=sum)
    }
    
    nref$n_nid <- nref$nid
    aref <- aggregate(cbind(sample_size, cases) ~ age_bin + year_bin + location_match + nid, data=ref, FUN=sum)
    anref <- aggregate(cbind(n_sample_size, n_cases) ~ age_bin + year_bin + location_match + n_nid, data=nref, FUN=sum)
    
    if (covariate_name == "cv_inpatient"){
      # rebind england to anref
      anref <- rbind(anref, aengland)
    }
    
    wmean <- merge(aref, anref, by=c("age_bin","location_match","year_bin"))
  } else {
    print("The merge_type argument must be either 'within' (within a single NID) or 'between' (matched on location, year, age)")
  }
  
  # Get the ratio
  wmean$mean <- wmean$cases/wmean$sample_size
  wmean$n_mean <- wmean$n_cases/wmean$n_sample_size
  wmean$standard_error <- sqrt(wmean$mean * (1-wmean$mean)/wmean$sample_size)
  wmean$n_standard_error <- sqrt(wmean$n_mean * (1-wmean$n_mean)/wmean$n_sample_size)
  
  wmean$ratio <- (wmean$mean) / (wmean$n_mean)
  wmean$se <- sqrt(wmean$mean^2 / wmean$n_mean^2 * (wmean$standard_error^2/wmean$mean^2 + wmean$n_standard_error^2/wmean$n_mean^2))
  wmean <- subset(wmean, se > 0)
  
  # Convert the ratio to log space, too
  wmean$log_ratio <- log(wmean$ratio)
  
  wmean$delta_log_se <- sapply(1:nrow(wmean), function(i) {
    ratio_i <- wmean[i, "ratio"]
    ratio_se_i <- wmean[i, "se"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })
  
  #calcuating standard error of dx1 in logit space
  if(include_logit == T){
    wmean$se_logit_mean_1 <- sapply(1:nrow(wmean), function(i) {
      ratio_i <- wmean[i, "mean"]
      ratio_se_i <- wmean[i, "standard_error"]
      deltamethod(~log(x1/(1-x1)), ratio_i, ratio_se_i^2)
    })
    
    #calcuating standard error of dx2 in logit space
    wmean$se_logit_mean_2 <- sapply(1:nrow(wmean), function(a) {
      ratio_a <- wmean[a, "n_mean"]
      ratio_se_a <- wmean[a, "n_standard_error"]
      deltamethod(~log(x1/(1-x1)), ratio_a, ratio_se_a^2)
    })
    
    wmean$logit_ratio <- logit(wmean$mean) - logit(wmean$n_mean)
    wmean$logit_ratio_se <- with(wmean, sqrt(se_logit_mean_1^2 + se_logit_mean_2^2))
  }
  
  # Pull out age_start, age_end, year_start, year_end
  if(merge_type == "between"){
    age_list <- data.frame(matrix(unlist(strsplit(as.character(wmean$age_bin),",")), ncol=2, byrow=TRUE))
    year_list <- data.frame(matrix(unlist(strsplit(as.character(wmean$year_bin),",")), ncol=2, byrow=TRUE))
    
    wmean$year_start <- as.numeric(substr(year_list$X1,2,5))
    wmean$year_end <- as.numeric(substr(year_list$X2,1,4))
    
    wmean$age_start <- as.numeric(substr(age_list$X1,2,4))
    wmean$age_end <- as.numeric(gsub(pattern = "]",replacement = "", age_list$X2))
  }
  # Return the dataframe for further analysis
  return(wmean)
}

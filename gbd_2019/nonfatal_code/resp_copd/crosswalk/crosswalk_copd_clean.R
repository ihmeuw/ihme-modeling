##################################################################################################################################### 
#The intention for this function is to create a dataframe that is the collapsed and merged data that will be used in crosswalks.
# It subsets out the reference data in a bundle and the non-reference data. It cuts these data depending on where the user wants
# them by age and year. The function returns a dataframe that is prepped for calculating the mean effect for the ratio in a crosswalk
# that can be used either in rma() or in MR-BRT.
# Written by USERNAME, March 2019 - modified by USERNAME
#
# 1. df is the dataframe (i.e. bundle data)
# 2. covariate_name is the name of the binary column for the crosswalk (i.e. cv_)
# 3. merge_type is how you want the data to be merged by study. The options are
#         "within" means an exact match within a single study (merges by NID, age_start, age_end, year_start, year_end, sex)
#         "between" means a close match by location, year, and age 
#          "market" means marketscan comparison so no matching by year
# 4. location_match is at what level do you want your data merged by location, meaning how close geographically is a match?
#         "exact" merges by location_id; "country" collapses subnationals into single value
#         "region" collapses regional data to single value; "super" collapses super regional data to single value
# 5. year_range is the number of years above or below year_start and year_end of the reference study that will be used for a comparison
######################################################################################################################################


library(dplyr)
library(msm, lib.loc = "FILEPATH")
library(readxl)
library(stringr)
library(metafor, lib.loc = "FILEPATH")

rm(list=ls())


locs <- read.csv("FILEPATH")

df <- as.data.table(read.csv("FILEPATH"))  

covariate_name <- "cv_ERS"    #set covariate of interest for crosswalk (alt)
alt_denom <- "cv_lln_post" #set if comparing to another non-reference covariate
location_match <- "exact"
cause_path <- "COPD/"  
cause_name <- "COPD_" 
year_range <- 0
age_range <- 0

# Load data
date <- Sys.Date() #returns current date
date <- gsub("-", "_", date) #date is year_month_day instead of year-month-day


#Needs an "is_reference" column and "is_alt" column
 df$is_reference <- df$cv_lln_post
 df$is_alt <- df$cv_ERS


# Create a working dataframe
wdf <- df[,c("location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
             "is_reference", "is_alt", "measure")]
covs <- select(df, matches("^cv_"))
wdf <- cbind(wdf, covs)
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
  
## Collapse to the desired merging ##
ref <- subset(wdf, is_reference == 1)
nref <- subset(wdf, is_alt == 1)

## Get pairs
setnames(nref, c("mean","standard_error","cases","sample_size"), c("n_mean","n_standard_error","n_cases","n_sample_size"))
    
wmean <- merge(ref, nref, by=c("nid","age_start","age_end","year_start","year_end", "location_match","sex", "measure")) 
wmean <- unique(wmean) 
    
    
## Get the ratio
# First get log ratio  
wmean$ratio <- wmean$n_mean/wmean$mean
wmean$se <- sqrt(wmean$n_mean^2 / wmean$mean^2 * (wmean$n_standard_error^2/wmean$n_mean^2 + wmean$standard_error^2/wmean$mean^2))
wmean <- subset(wmean, se > 0)

# Convert the ratio to log space
wmean$log_ratio <- log(wmean$ratio)

# Convert se to log space
wmean$delta_log_se <- sapply(1:nrow(wmean), function(i) {
  ratio_i <- wmean[i, "ratio"]
  ratio_se_i <- wmean[i, "se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

# Second get logit difference
setnames(wmean, "mean", "mean_ref")
setnames(wmean, "n_mean", "mean_alt")

wmean$se_logit_mean_alt <- sapply(1:nrow(wmean), function(i) {
  ratio_i <- wmean[i, mean_alt]
  ratio_se_i <- wmean[i, n_standard_error]
  deltamethod(~log(x1/(1-x1)), ratio_i, ratio_se_i^2)
})

wmean$se_logit_mean_ref <- sapply(1:nrow(wmean), function(a) {
  ratio_a <- wmean[a, mean_ref]
  ratio_se_a <- wmean[a, standard_error]
  deltamethod(~log(x1/(1-x1)), ratio_a, ratio_se_a^2)
})

wmean <- wmean %>% # creating the dataset for MR-BRT
  mutate(
    logit_mean_alt = logit(mean_alt),
    logit_mean_ref = logit(mean_ref),
    diff_logit = logit_mean_alt - logit_mean_ref,
    se_diff_logit = sqrt(se_logit_mean_alt^2 + se_logit_mean_ref^2)
  )

#Write output to csv 
write.csv(wmean, paste0("FILEPATH"), row.names = F)


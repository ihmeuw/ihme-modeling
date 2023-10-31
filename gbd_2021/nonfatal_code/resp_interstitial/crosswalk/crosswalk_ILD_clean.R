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

#try running with exact year and exact age

library(plyr)
library(msm, lib.loc = "FILEPATH")
library(readxl)
library(stringr)
library(metafor, lib.loc = "FILEPATH")
library(data.table)

rm(list=ls())

locs <- read.csv("FILEPATH")

df <- as.data.table(read.csv("FILEPATH"))  

covariate_name <- "cv_imaging"    #set covariate of interest for crosswalk (alt)
alt_denom <- "cv_IPF"
merge_type <- "between"
alt_to_alt <- FALSE
location_match <- "exact"
cause_path <- "ILD/"  
cause_name <- "ILD_" 
year_range <- 0
age_range <- 0

# Load data
date <- Sys.Date() 
date <- gsub("-", "_", date) 

#For ILD non-marketscan comparisons, remove hospital/claims data:
df<- df[df$clinical_data_type=="", ]

#The function needs an "is_reference" column and "is_alt" column
df$is_reference <- ifelse(df$cv_imaging==0, 1,0)
df$is_alt <- df$cv_imaging

#Remove outlier rows
df <- df[!is_outlier == 1 & measure %in% c("prevalence", "incidence")] 

z <- qnorm(0.975)
df[measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
df[measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
df[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]

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

#Exact age match
  wdf$age_start_match <- wdf$age_start
  wdf$age_end_match <- wdf$age_end
  
## Collapse to the desired merging
ref <- subset(wdf, is_reference == 1)
nref <- subset(wdf, is_alt == 1)


if(merge_type=="within"){

  setnames(nref, c("mean","standard_error","cases","sample_size"), c("n_mean","n_standard_error","n_cases","n_sample_size"))
    
  wmean <- merge(ref, nref, by=c("nid","age_start","age_end","year_start","year_end", "location_match","sex", "measure")) 
  wmean <- unique(wmean) 
    
} else if(merge_type=="between") {
  setnames(nref, c("mean","standard_error","cases","sample_size", "year_start", "year_end", "age_start", "age_end", "measure"), c("n_mean","n_standard_error","n_cases","n_sample_size", "n_year_start", "n_year_end", "n_age_start", "n_age_end", "n_measure"))
  
  wmean <- merge(ref, nref, by=c("location_match"), allow.cartesian = T)
  wmean[, c("ihme_loc_id.x", "ihme_loc_id.y", "super_region_name.x", "region_name.x", "ihme_loc_abv.x", "ihme_loc_abv.y", "location_name.x" , "location_id.x", "location_id.y")] <- NULL
  
  #Drop rows if the years are not within range 
  wmean$year_lower <- abs(wmean$year_start - wmean$n_year_start)
  wmean$year_upper <- abs(wmean$year_end - wmean$n_year_end)
  wmean <- wmean[wmean$year_lower <= year_range & wmean$year_upper <= year_range, ] 
  
  #Drop rows if the age are not within range 
  wmean$age_lower <- abs(wmean$age_start - wmean$n_age_start)
  wmean$age_upper <- abs(wmean$age_end - wmean$n_age_end)
  wmean <- wmean[wmean$age_lower <= age_range & wmean$age_upper <= age_range, ] 
  
  #Drop rows if sexes are not the same for sex and n_sex in a given row - then combine sex.x and sex.y into one sex column
  wmean <- wmean[wmean$sex.x==wmean$sex.y, ]
  wmean$sex <- wmean$sex.x
  wmean[, c("sex.x", "sex.y")] <- NULL
  
  #Drop if measure is not the same (incidence/prevalence)
  wmean <- wmean[wmean$measure==wmean$n_measure, ]
  wmean[, c("n_measure")] <- NULL
  
  #Drop duplicates
  wmean <- unique(wmean)
  
} else if(merge_type=="market") {
  setnames(nref, c("mean","standard_error","cases","sample_size", "year_start", "year_end"), c("n_mean","n_standard_error","n_cases","n_sample_size", "n_year_start", "n_year_end"))
  
  ref[, c("ihme_loc_id", "ihme_loc_abv", "location_id")] <- NULL
  nref[, c("ihme_loc_id", "ihme_loc_abv", "location_id")] <- NULL
  
  wmean <- merge(ref, nref, by=c("age_start_match", "age_end_match", "location_match", "sex"), allow.cartesian = T)
  
  #Drop duplicates
  wmean <- unique(wmean)
  
  
} else {
  print("The merge_type argument must be either 'within' (within a single NID) or 'between' (matched on location, year, age), or Marketscan")
}

# Get the ratio!
wmean$ratio <- wmean$n_mean/wmean$mean
wmean$se <- sqrt(wmean$n_mean^2 / wmean$mean^2 * (wmean$n_standard_error^2/wmean$n_mean^2 + wmean$standard_error^2/wmean$mean^2))

wmean <- subset(wmean, se > 0)

# Convert the ratio to log space, too
wmean$log_ratio <- log(wmean$ratio)

wmean$delta_log_se <- sapply(1:nrow(wmean), function(i) {
  ratio_i <- wmean[i, "ratio"]
  ratio_se_i <- wmean[i, "se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


#Write output to csv  
write.csv(wmean, paste0("FILEPATH"), row.names = F)


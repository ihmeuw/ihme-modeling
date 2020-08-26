#####################################################################################################################################
#The intention for this function is to create a dataframe that is the collapsed and merged data that will be used in crosswalks.
# It subsets out the reference data in a bundle and the non-reference data. It cuts these data depending on where the user wants
# them by age and year. The function returns a dataframe that is prepped for calculating the mean effect for the ratio in a crosswalk
# that can be used either in rma() or in MR-BRT.
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

## Set up --------------------------------------------------------------------------------------------------------------------------------------------------------
library(plyr)
library(msm, lib.loc = "FILEPATH")
library(readxl)
library(stringr)
library(metafor, lib.loc = "FILEPATH")

rm(list=ls())


locs <- read.csv("FILEPATH")

covariate_name <- "cv_best_corrected" #set covariate of interest for crosswalk (alt)
location_match <- "exact"
diagcode_p <- "p"
diagcode_b <- "b"
cause_path <- "FILEPATH"
cause_name <- "ID"

year_range <- 0
age_range <- 0

df <- as.data.table(read.csv(paste0("FILEPATH")))


## Set up alt and ref -------------------------------------------------------------------------------------------------------------------------------------------------------------
df <- df[df$is_outlier!=1, ]
df <- df[group_review==1 | is.na(group_review), ]
df <- df[check_duplicates=="", ]

df$is_reference <- ifelse((df$cv_best_corrected==0), 1, 0)
df$is_alt <- df$cv_best_corrected


# Create a working dataframe
wdf <- df[,c("nid", "location_id", "location_name", "sex", "year_start", "year_end", "age_start", "age_end", "mean", "lower", "upper",
             "standard_error", "urbanicity_type", "diagcode", "cv_diag_loss", "cv_best_corrected", "is_alt", "is_reference")]
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


## Get ratios --------------------------------------------------------------------------------------------------------------------------------------------
setnames(nref, c("mean","standard_error", "year_start", "year_end", "age_start", "age_end"), c("n_mean","n_standard_error", "n_year_start", "n_year_end", "n_age_start", "n_age_end"))
wmean <- merge(ref, nref, by=c("location_match", "sex"), allow.cartesian = T)
wmean[, c("ihme_loc_id.x", "ihme_loc_id.y", "super_region_name.x", "region_name.x", "ihme_loc_abv.x", "ihme_loc_abv.y", "location_name.x" ,"location_name.y", "location_id.x", "location_id.y")] <- NULL

#Drop rows if the years are not within range
wmean$year_lower <- abs(wmean$year_start - wmean$n_year_start)
wmean$year_upper <- abs(wmean$year_end - wmean$n_year_end)
wmean <- wmean[wmean$year_lower <= year_range & wmean$year_upper <= year_range, ]

#Drop rows if the ages are not within range
wmean$age_lower <- abs(wmean$age_start - wmean$n_age_start)
wmean$age_upper <- abs(wmean$age_end - wmean$n_age_end)
wmean <- wmean[wmean$age_lower <= age_range & wmean$age_upper <= age_range, ]

#Drop duplicates
wmean <- unique(wmean)

#Create a logical variable to signify if the ratio is within or between study and making rapid study or not rapid study a logical as well
wmean$cv_within <- ifelse(wmean$nid.x == wmean$nid.y, 1, 0)
wmean$cv_within <- as.logical(wmean$cv_within)

#Drop row if within-study RAAB is used for between-study comparison to another RAAB
wmean <- wmean[!(wmean$cv_diag_loss.x==1 & cv_diag_loss.y==1 & wmean$nid.x!=nid.y)]


# Get the ratio
wmean$ratio <- wmean$n_mean/wmean$mean
wmean$se <- sqrt(wmean$n_mean^2 / wmean$mean^2 * (wmean$n_standard_error^2/wmean$n_mean^2 + wmean$standard_error^2/wmean$mean^2))
wmean <- subset(wmean, se > 0)

# Get the logit mean and SE and logit difference
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


## Write results to csv file
write.csv(wmean, paste0("FILEPATH"), row.names = F)


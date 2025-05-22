# Prep data for crosswalking 
# Need to get and prep both USA data and MNG data

rm(list=ls())

library(ggplot2)
library(reticulate)
# Filepath to use shared code for IHME crosswalk package
reticulate::use_python("FILEPATH")
cw <- import("crosswalk")

# IHME shared functions
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# Set up for specific bundle & bundle_version
my_bundle_id <- X
my_bv_id <- X

# Get desired bundle version for self-harm
bundle_version <- get_bundle_version(my_bv_id)
ci_data <- bundle_version[!is.na(clinical_version_id)]
ci_data <- ci_data[,.(location_id, sex, year_start, year_end, age_start, age_end,
                      mean, standard_error, clinical_data_type, cases, sample_size)]

# Split into inpatient and outpatient, for the USA 
# (only location where we have both inpatient and outpatient for matching)
inpatient <- ci_data[location_id == 102 & clinical_data_type == "inpatient"]
outpatient <- ci_data[location_id == 102 & clinical_data_type == "outpatient"]

# Assign year_id
inpatient[year_start == 1988, year_id := 1]
inpatient[year_start == 1993, year_id := 2]
inpatient[year_start == 1998, year_id := 3]
inpatient[year_start == 2003, year_id := 4]
inpatient[year_start == 2008, year_id := 5]

# Assign year_id
outpatient[year_start %in% c(1988:1992), year_id := 1]
outpatient[year_start %in% c(1993:1997), year_id := 2]
outpatient[year_start %in% c(1998:2002), year_id := 3]
outpatient[year_start %in% c(2003:2007), year_id := 4]
outpatient[year_start %in% c(2008:2012), year_id := 5]

# Re-calculate outpatient incidence, across year_id bins
# USA outpatient data is in single-year bins, while inpatient is in 5-year
# bins - we want them to match
outpatient_agg <- outpatient[, .(cases = sum(cases), sample_size = sum(sample_size)), by = .(location_id, sex, year_id, age_start, age_end)]
outpatient_agg[, mean := cases/sample_size]

# Quick plot to check that re-calculated means fall in range of original data
ggplot() + 
  geom_point(data = outpatient, aes(x = age_start, y = mean), color = "green") +
  geom_point(data = outpatient_agg, aes(x = age_start, y = mean), color = "blue") 

# Re-calculate standard error
outpatient_agg[cases < 5, standard_error := ((5-mean*sample_size)/sample_size + mean*sample_size*sqrt(5/sample_size^2))]
outpatient_agg[cases >= 5, standard_error := sqrt(mean/sample_size)]

# Cleanup to prep dataset for matching
outpatient_agg <- outpatient_agg[!is.na(year_id)]
outpatient_agg[, clinical_data_type := "outpatient"]
outpatient_agg <- outpatient_agg[,.(location_id,sex,year_id,age_start,age_end,mean,standard_error,clinical_data_type)]

# Re-calculate inpatient incidence for elderly - outpatient data has info in 
# ages 85-124, whereas inpatient has 85-89, 90-94, 95-124 (again, need to match)
inpatient_85plus <- inpatient[age_start >= 85]
inpatient_85plus <- inpatient_85plus[, .(cases = sum(cases), sample_size = sum(sample_size)), 
                                     by = .(location_id, sex, year_id)]
inpatient_85plus[, age_start := 85]
inpatient_85plus[, age_end := 124]
inpatient_85plus[, mean := cases/sample_size]

# Quick plot to check that re-calculated means fall in range of original data
ggplot() + 
  geom_point(data = inpatient_85plus, aes(x = year_id, y = mean), color = "green", size = 3) +
  geom_point(data = inpatient[age_start>84], aes(x = year_id, y = mean), color = "blue")

# Recalculate standard error
inpatient_85plus[cases < 5, standard_error := ((5-mean*sample_size)/sample_size + mean*sample_size*sqrt(5/sample_size^2))]
inpatient_85plus[cases >= 5, standard_error := sqrt(mean/sample_size)]

# Cleanup to prep dataset for matching
inpatient_agg <- rbind(inpatient_85plus, inpatient[age_start < 85], fill = T)
inpatient_agg <- inpatient_agg[,.(location_id,sex,year_id,age_start,age_end,mean,standard_error,clinical_data_type)]
inpatient_agg[, clinical_data_type := "inpatient"]

# Change age_end to 99 instead of 124 for evenly spaced age_midpoints
inpatient_agg[age_end == 124, age_end := 99]
outpatient_agg[age_end == 124, age_end := 99]

# Calculate age_midpoint
inpatient_agg[, age_midpoint := (age_start + age_end)/2]
outpatient_agg[, age_midpoint := (age_start + age_end)/2]

# Only keep necessary columns
inpatient_agg <- inpatient_agg[,.(location_id,sex,year_id,age_midpoint,mean,standard_error,clinical_data_type)]
outpatient_agg <- outpatient_agg[,.(location_id,sex,year_id,age_midpoint,mean,standard_error,clinical_data_type)]

# Drop any zeros
inpatient_agg <- inpatient_agg[mean != 0]
outpatient_agg <- outpatient_agg[mean != 0]

# Set column names to prep for CWData
# Inpatient is reference (ref), outpatient is alternate (alt)
colnames(inpatient_agg) <- c("location_id","sex","year_id","age_midpoint","inc_ref","inc_se_ref","dorm_ref")
colnames(outpatient_agg) <- c("location_id","sex","year_id","age_midpoint","inc_alt","inc_se_alt","dorm_alt")

# Create matches!
usa_matches <- merge(inpatient_agg, outpatient_agg, by = c("location_id","sex","year_id","age_midpoint"))

# Delta transform means and standard errors to log space
usa_matches[, c("mean_alt", "mean_se_alt")] <- cw$utils$linear_to_log(mean = array(usa_matches$inc_alt), sd = array(usa_matches$inc_se_alt))
usa_matches[, c("mean_ref", "mean_se_ref")] <- cw$utils$linear_to_log(mean = array(usa_matches$inc_ref), sd = array(usa_matches$inc_se_ref))

# Calculate log difference and corresponding standard error
usa_matches <- usa_matches[, log_diff := mean_alt-mean_ref]
usa_matches <- usa_matches[, log_diff_se := sqrt(mean_se_alt^2 + mean_se_ref^2)]


# Get MNG data
mng <- bundle_version[location_id == 38]

# Only keep data from Injury Surveillance System (5 years, 5 NIDs)
mng <- mng[nid %in% c(472065,472067,472068,472069,472070)]

# Change age_end to 99 for evenly spaced age groups w/ age_midpoint
mng[age_end == 124, age_end := 99]

# Split into inpatient/outpatient
inpatient <- mng[source_type == "Facility - inpatient"]
outpatient <- mng[source_type == "Facility - outpatient"]

# Keep necessary columns
inpatient <- inpatient[,.(location_id,sex,year_start,year_end,age_start,age_end,mean,standard_error,cases,sample_size)]
outpatient <- outpatient[,.(location_id,sex,year_start,year_end,age_start,age_end,mean,standard_error,cases,sample_size)]

# Calculate age midpoint
inpatient[, age_midpoint := (age_start + age_end)/2]
outpatient[, age_midpoint := (age_start + age_end)/2]

# Add "dorm" columns for reference/alternate case def
inpatient[, dorm_ref := "inpatient"]
outpatient[, dorm_alt := "outpatient"]

# Use one year column only
inpatient[, year_id := year_start]
outpatient[, year_id := year_start]

# Only keep necessary columns
inpatient <- inpatient[,.(location_id,sex,year_id,age_midpoint,mean,standard_error,dorm_ref)]
outpatient <- outpatient[,.(location_id,sex,year_id,age_midpoint,mean,standard_error,dorm_alt)]

# Drop any zeros
inpatient <- inpatient[mean != 0]
outpatient <- outpatient[mean != 0]

# Rename
colnames(inpatient) <- c("location_id","sex","year_id","age_midpoint","inc_ref","inc_se_ref","dorm_ref")
colnames(outpatient) <- c("location_id","sex","year_id","age_midpoint","inc_alt","inc_se_alt","dorm_alt")

# Find matches!
mng_matches <- merge(inpatient, outpatient, by = c("location_id","sex","year_id","age_midpoint"))

# Delta transform means and standard errors to log space
mng_matches[, c("mean_alt", "mean_se_alt")] <- cw$utils$linear_to_log(mean = array(mng_matches$inc_alt), sd = array(mng_matches$inc_se_alt))
mng_matches[, c("mean_ref", "mean_se_ref")] <- cw$utils$linear_to_log(mean = array(mng_matches$inc_ref), sd = array(mng_matches$inc_se_ref))

# Calculate log difference and corresponding standard error
mng_matches <- mng_matches[, log_diff := mean_alt-mean_ref]
mng_matches <- mng_matches[, log_diff_se := sqrt(mean_se_alt^2 + mean_se_ref^2)]

# Combine USA and MNG matches into single dataset
all_matches <- rbind(usa_matches, mng_matches)

# Write to CSV to save!
fwrite(all_matches, paste0("FILEPATH/bundle_", my_bundle_id, "_", Sys.Date(), ".csv"))

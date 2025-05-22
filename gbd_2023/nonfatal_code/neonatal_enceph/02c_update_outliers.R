## ******************************************************************************
##
## Purpose: Replace existing outliers with outliers identified by new run of MR-BRT model
## Input:   bundle ID
## Output:
## Last Update: 3/23/2020
##
## ******************************************************************************

rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "PATHNAME"
  h <- "PATHNAME"
} else {
  j <- "PATHNAME"
  h <- "PATHNAME"
}

pacman::p_load(data.table, dplyr, ggplot2, stats, boot, msm)
source("PATHNAME/get_crosswalk_version.R")
source("PATHNAME/save_bulk_outlier.R")
source("PATHNAME/save_crosswalk_version.R")
source("PATHNAME/get_age_metadata.R")
source("PATHNAME/get_bundle_version.R")
source("PATHNAME/get_elmo_ids.R")
source("PATHNAME/get_location_metadata.R")

#Load existing xwalk data, merge on the trimmed data from MR-BRT, and update the outlier status
#mrbrt_outliers <- fread("PATHNAME")
mrbrt_outliers <- fread("PATHNAME")
mrbrt_outliers <- mrbrt_outliers[, .(seq, w)]

df <- data.table(read.xlsx("PATHNAME"))

update_xwalk_orig <- merge(df, mrbrt_outliers, by = 'seq', all.x = TRUE)
update_xwalk[, .N, by = .(is_outlier, w)]

update_xwalk[w == 0, is_outlier := 1]


#adding GBD2020 Iterative manual outliers
out_dir <- "PATHNAME"
bun_id <- 8066

update_xwalk <- data.table(read.xlsx("PATHNAME"))
update_xwalk[location_name == 'Democratic Republic of the Congo', is_outlier := 1]
update_xwalk[location_name == 'Nigeria' & year_id == 2003, is_outlier := 0]
update_xwalk[location_name == 'South Africa', is_outlier := 1]
update_xwalk[location_name == 'Uganda' & sex == 'Female', is_outlier := 0]
update_xwalk[location_name == 'Australia' & sex == 'Male' & year_id == 2004, is_outlier := 1]
update_xwalk[nid == 234750 & sex == 'Male', is_outlier := 1]
update_xwalk[location_name == 'Nigeria' & year_id == 2003, is_outlier := 1]

#outlier the U.S. low inpatient data
locs <- get_location_metadata(location_set_id = 22, gbd_round_id = 7)[, .(location_id, ihme_loc_id)]
usa_locs <- locs[grepl("USA", ihme_loc_id)]
update_xwalk[location_id %in% usa_locs$location_id & clinical_data_type == 'inpatient', is_outlier := 1]

#format for crosswalk version for st-gpr
update_xwalk$w <- NULL

update_xwalk[, measure := 'proportion']
update_xwalk[,year_id := floor((as.numeric(year_end)+as.numeric(year_start))/2)]
setnames(update_xwalk, 'mean', 'val')
update_xwalk$year_start <- NULL
update_xwalk$year_end <- NULL
update_xwalk[, variance := standard_error^2]

ages <- get_age_metadata(age_group_set_id = 19)[, .(age_group_id, age_group_years_start, age_group_years_end)]
setnames(ages, c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
update_xwalk <- merge(update_xwalk, ages, by = c('age_start', 'age_end'), all.x = TRUE)

update_xwalk$unit_value_as_published <- as.numeric(update_xwalk$unit_value_as_published)
update_xwalk$unit_value_as_published <- 1
update_xwalk <- update_xwalk[is.na(group_review) | group_review == 1]

update_xwalk$seq <- NA

write.csv(update_xwalk, file = "PATHNAME",
          row.names = FALSE)

# subset to just birth for the prevalence model
update_xwalk[age_start == 0 & age_end == 0, age_group_id := 164]

write.xlsx(update_xwalk, file = "PATHNAME", sheetName = 'extraction')

save_crosswalk_version(bundle_version_id = 29315, 
                       data_filepath = "PATHNAME",
                       description = 'data from bv29315 on bun 8066, birth prev clinical and lit with outliers, newly outliered usa inpatient')

test <- get_crosswalk_version(crosswalk_version_id = 27350)



# sex ratio by nid investigation -- didn't end up using
mean(update_xwalk[sex == 'Male', val]) / mean(update_xwalk[sex == 'Female', val])
mean(update_xwalk[sex == 'Male' & is_outlier == 0, val]) / mean(update_xwalk[sex == 'Female' & is_outlier == 0, val])
mean(update_xwalk[sex == 'Male' & is_outlier == 0 & clinical_data_type != '', val]) / mean(update_xwalk[sex == 'Female' & is_outlier == 0& clinical_data_type != '', val])
mean(update_xwalk[sex == 'Male' & clinical_data_type != '', val]) / mean(update_xwalk[sex == 'Female' & clinical_data_type != '', val])
mean(update_xwalk[sex == 'Male' & clinical_data_type == '', val]) / mean(update_xwalk[sex == 'Female' & clinical_data_type == '', val])


update_xwalk[, mean_by_nid_sex := mean(val), by = .(sex, location_name)]
dt20 <- update_xwalk[clinical_data_type != '']
dt20 <- unique(dt20[, .(sex, mean_by_nid_sex, location_name)])
dt20_wide <- dcast(dt20, value.var = 'mean_by_nid_sex', location_name ~ sex)
dt20_wide[Female == 0, Female := 1]
dt20_wide[, sex_ratio := Male / Female, by = .(location_name)]
mean(dt20_wide$sex_ratio)

ids <- get_elmo_ids(gbd_round_id = 6, decomp_step = 'step4', crosswalk_version_id = 10625)
dt <- get_crosswalk_version(crosswalk_version_id = 10625)
dt <- get_bundle_version(bundle_version_id = 12629, fetch = 'all')

dt <- dt[measure == 'prevalence']
mean(dt[sex == 'Male', mean]) / mean(dt[sex == 'Female', mean])
mean(dt[sex == 'Male' & is_outlier == 0, mean]) / mean(dt[sex == 'Female' & is_outlier == 0, mean])
mean(dt[sex == 'Male' & is_outlier == 0 & clinical_data_type != '', mean]) / mean(dt[sex == 'Female' & is_outlier == 0& clinical_data_type != '', mean])
mean(dt[sex == 'Male' & clinical_data_type != '', mean]) / mean(dt[sex == 'Female' & clinical_data_type != '', mean])
mean(dt[sex == 'Male' & clinical_data_type == '', mean]) / mean(dt[sex == 'Female' & clinical_data_type == '', mean])


mean(bv[sex == 'Male', mean]) / mean(bv[sex == 'Female', mean])
mean(bv[sex == 'Male' & is_outlier == 0, mean]) / mean(bv[sex == 'Female' & is_outlier == 0, mean])
mean(bv[sex == 'Male' & is_outlier == 0 & clinical_data_type != '', mean]) / mean(bv[sex == 'Female' & is_outlier == 0& clinical_data_type != '', mean])
mean(bv[sex == 'Male' & clinical_data_type != '', mean]) / mean(bv[sex == 'Female' & clinical_data_type != '', mean])
mean(bv[sex == 'Male' & clinical_data_type == '', mean]) / mean(bv[sex == 'Female' & clinical_data_type == '', mean])


dt[, mean_by_nid_sex := mean(mean), by = .(sex, location_name)]
dt19 <- dt[clinical_data_type != '']
dt19 <- unique(dt19[, .(sex, mean_by_nid_sex, location_name)])
dt19_wide <- dcast(dt19, value.var = 'mean_by_nid_sex', location_name ~ sex)
dt19_wide[Female == 0, Female := 1]
dt19_wide[, sex_ratio := Male / Female, by = .(location_name)]
mean(dt19_wide$sex_ratio)

compare <- merge(dt20_wide, dt19_wide, by = c('location_name'), all.x = TRUE)

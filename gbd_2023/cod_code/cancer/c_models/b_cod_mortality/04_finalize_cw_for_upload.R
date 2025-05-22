##################################################################################
# Name of Script: 04_finalize_cw_for_upload.R
# Date: 4/19/2024
# Description: Prepares GBD 2023 crosswalked data for upload
#
# Contributors: INDIVIDUAL_NAME
# Last Updated: 7/22/2024
###################################################################################

# Load required libraries
pacman::p_load(data.table, dplyr, tidyr, tidyverse, haven, readxl, reshape2, crayon)

# Source required shared functions
source("FILEPATH/get_location_metadata.R")

location_set = 35
release <- 16

#get locations
locs <- get_location_metadata(location_set_id=location_set, release_id = release)


star <- fread("FILEPATH")


cr_mort_rec <- fread("FILEPATH")

# OR read in CoD data that has been saved after most recent refresh
#gbd_2023_rec <- fread("FILEPATH")

# subset on is_estimate column
#gbd_2023_rec <- gbd_2023_rec[is_estimate == 1]
# subset on is_outlier column
#gbd_2023_rec <- gbd_2023_rec[is_outlier == 0]

# Read in CoD data that has been saved after most recent refresh
gbd_2023_part1 <- fread("FILEPATH")

gbd_2023_part2 <- fread("FILEPATH")

gbd_2023_rec <- rbind(gbd_2023_part1, gbd_2023_part2)

#pull in outliers
cod_outliers <- fread("FILEPATH")

# merge in is_outlier column
gbd_2023_rec <- merge(gbd_2023_rec, 
                      cod_outliers[, .(nid, source_id, location_id, year_id, age_group_id, sex_id, site_id, cause_id, is_outlier)], 
                      by = c("nid", "source_id" , "location_id", "year_id", "age_group_id", "sex_id", "site_id", "cause_id"), 
                      all.x = TRUE)

# merge in is_estimate column
gbd_2023_rec <- merge(gbd_2023_rec, locs[,.(location_id, is_estimate)], by = "location_id", all.x=T)


gbd_2023_rec <- gbd_2023_rec[is_estimate == 1]


gbd_2023_rec$data_type_name <- "VR"

#Subset to CoD columns needed
cod_dt23 <- gbd_2023_rec[,.(location_id, year_id, age_group_id, sex_id, cause_id, cf_raw, cf_final=cf, sample_size,
                            data_type_name, is_outlier)]

#Separate out VR data
cod_vr <- cod_dt23[ which(cod_dt23$data_type_name=='VR'), ]
#fill in missing is_outlier values with 0:
cod_vr[is.na(is_outlier), is_outlier := 0]
#Remove outliers from VR data because these should not be used as gold standard
cod_vr <- cod_vr[ which(cod_vr$is_outlier== 0), ]

#Subset to CR columns needed
cr_dt23 <- cr_mort_rec[,.(location_id, year_id, age_group_id, sex_id, cause_id, sample_size, national, cf_raw, cf_final)]

# Read in output from cw with spline
final_output <- read_csv("FILEPATH/spline_results.csv")
final_output <- final_output[,-1]

# Read in output from cw with NO spline
no_spline <- as.data.table(read_csv("FILEPATH/no_spline_results.csv"))
no_spline1 <- no_spline[,-1]
no_spline1$model <- "no_spline" 

no_spline1 <- no_spline1[is_cr == 1]

no_spline1 <- as.data.table(subset(no_spline1, !is.na(cf_adjusted)))
no_spline1 <- no_spline1[cf_type == "cr"]
no_spline1[, cf_no_spline := cf_adjusted]

final_combo1 <- subset(final_output, select = -c(adj_factor, adjustment))
final_combo1$model <- "spline"
final_combo1 <- as.data.table(subset(final_combo1, !is.na(cf_adjusted)))
final_combo <- final_combo1[cf_type == "cr"]
final_combo[, cf_final := cf_adjusted]

#remove unneeded columns
final_combo[, c("cf_adjusted", "cf_type", "cf_vr_pred", "index", "intercept", "is_cr", 
             "model", "offset", "trim_weights", "sample_size", "cf") := NULL]

#rename cf_final to cf_corr so we can retain pre-cw values to review in CoDViz
setnames(cr_dt23, old = "cf_final", new = "cf_corr")

# pull in cf_no_spline column
cr_cw_merge <- merge(final_combo, 
                     no_spline1[, .(location_id, year_id, age_group_id, sex_id, cause_id, cf_no_spline)], 
                      by = c("location_id", "year_id", "age_group_id", "sex_id", "cause_id"), 
                      all = TRUE)

#Fill in cf_final with cf_no_spline if there is no spline cw adjustment
cr_cw_merge$cf_final[is.na(cr_cw_merge$cf_final)] <- cr_cw_merge$cf_no_spline[is.na(cr_cw_merge$cf_final)]

# pull in cf_final column
cr_adj_merge <- merge(cr_dt23, 
                     cr_cw_merge[, .(location_id, year_id, age_group_id, sex_id, cause_id, cf_final)], 
                     by = c("location_id", "year_id", "age_group_id", "sex_id", "cause_id"), 
                     all.x = TRUE)

#Fill in cf_final with cf_corr if there is no cw adjustment
cr_adj_merge$cf_final[is.na(cr_adj_merge$cf_final)] <- cr_adj_merge$cf_corr[is.na(cr_adj_merge$cf_final)]

# pull in parent_id column
cr_adj_merge <- merge(cr_adj_merge, locs[,.(location_id, parent_id)], by = "location_id", all.x=T)

#Fill in cf_final with cf_corr if in India (parent_id == 4841 through 4875)
cr_adj_merge[parent_id >= 4841 & parent_id <= 4875, cf_final := cf_corr]


#add columns needed for the cod.cv_data table
cr_adj_merge[, `:=` (
  cf_rd = 0,
  cf_cov = 0, 
  cf_final_low_rd = 0,
  cf_final_high_rd = 0,
  cf_final_low_ss = 0,
  cf_final_high_ss = 0,
  cf_final_low_total = 0,
  cf_final_high_total = 0,
  variance_rd_logit_cf = 0,
  variance_rd_log_dr = 0,
  representative_id = national,
  site_id = 2
)]

# Define column order
order <- c('year_id', 'location_id', 'sex_id', 'age_group_id', 'cause_id', 
               'cf_final', 'cf_rd', 'cf_corr', 'cf_cov', 'cf_raw', 
               'cf_final_low_rd', 'cf_final_high_rd', 'cf_final_low_ss', 
               'cf_final_high_ss', 'cf_final_low_total', 'cf_final_high_total', 
               'variance_rd_logit_cf', 'variance_rd_log_dr', 'sample_size', 
               'representative_id', 'site_id')

# Reorder the columns
setcolorder(cr_adj_merge, order)

fwrite(cr_adj_merge, "FILEPATH/to_upload.csv")

# confirming inclusion of all needed columns in the correct order:
column_names <- names(cr_adj_merge)
print(column_names)



#rename cod_vr cf to cf_vr
setnames(cod_vr, old = "cf_final", new = "cf_vr")

#drop unneeded columns
cod_vr[, c("cod_source_label", "data_type_name", "is_outlier", "is_representative", 
             "sample_size", "cf_raw") := NULL]

#merge VR to CR data
vr_cr_merge <- merge(x = cr_adj_merge, y = cod_vr,
                     by = c("age_group_id", "location_id", "sex_id", "year_id", "cause_id"),
                     all.x = TRUE)

# prep star ratings from CoD
star_full <- star[time_window=="full_time_series", .(location_id, stars, time_window)]
# add the star rating for VR data 
vr_cr_star <- setDT(merge(vr_cr_merge, star_full[,.(location_id, avg_stars=stars)],by=c("location_id"), all.x=T))


vr_cr_star <- vr_cr_star[is.na(cf_final)|is.na(cf_vr), has_match := 0]
vr_cr_star <- vr_cr_star[!is.na(cf_final)&!is.na(cf_vr), has_match := 1]


qual_ext <- vr_cr_star[has_match == 0]

#drop unneeded columns
qual_ext[, c("cf_vr", "has_match", "avg_stars", "national", "is_estimate", "parent_id") := NULL]

# Reorder the columns
setcolorder(qual_ext, order)

fwrite(qual_ext, "FILEPATH/cr_extends.csv")

# confirming inclusion of all needed columns in the correct order:
column_names <- names(qual_ext)
print(column_names)
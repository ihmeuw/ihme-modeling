############################################################
# PURPOSE: OUTLIER DATA WHERE POPULATION HAS BEEN VACCINATED
############################################################

rm(list = ls())

library(openxlsx, plyr)

source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/save_bulk_outlier.R")

# Get the bundle version associated with the correct step 
dt_csmr_prev <- FILE_PATH

# Subset to only prevalence data to only get literature (not CSMR) and only cols for analysis 
dt <- dt_csmr_prev[measure == "prevalence"]
dt_csmr <- dt_csmr_prev[measure == "mtspecific", ]

# Read in the vaccine year of introduction data 
vac_intro <- readr::read_rds(FILE_PATH)
vac_intro <- vac_intro[me_name == "vacc_hepb3", ]
vac_intro <- unique(vac_intro[, .(ihme_loc_id, location_id, cv_intro)])


# Merge the data sets together 
dt_all <- merge(dt, vac_intro, by = "location_id")

# Create year of birth cols based on different permutations with year_start, year_end, age_start, and age_end 
dt_all[, `:=` (yob_ys_as = round(year_start - age_start, 0), 
               yob_ys_ae = round(year_start - age_end, 0), 
               yob_ye_as = round(year_end - age_start, 0), 
               yob_ye_ae = round(year_end - age_end, 0))]

dt_all[, `:=` (keep_ys_as = ifelse(yob_ys_as < cv_intro, 1, 0), 
               keep_ys_ae = ifelse(yob_ys_ae < cv_intro, 1, 0), 
               keep_ye_as = ifelse(yob_ye_as < cv_intro, 1, 0), 
               keep_ye_ae = ifelse(yob_ye_ae < cv_intro ,1, 0))]


keep_cols <- names(dt_all)[grepl("keep", names(dt_all))]
keep_cols
dt_all[, keep_total := rowSums(.SD), .SDcols = keep_cols]

# Outlier studies where all study participants were exposed to vaccination 
# Outlier studies where > 50% of the study population experienced vaccination 

dt_processing2 <- copy(dt_all)
dt_def_drop <- dt_processing2[keep_total == 0, ]
nrow(dt_def_drop)
dt_processing2 <- dt_processing2[keep_total == 1 | keep_total == 2 | keep_total == 3, ]
dt_processing2[, `:=` (mean = (yob_ys_ae + yob_ye_as) / 2, sd = (yob_ye_as - yob_ys_ae) / 6 )]
dt_processing2[, coverage := pnorm(cv_intro, mean = mean, sd = sd)]
dt_processing2[, drop := ifelse(coverage <= 0.5, 1, 0 )]
nrow(dt_processing2[drop == 1,]) # 41
dt_drop2 <- copy(dt_processing2[drop == 1, ])
dt_drop_both <- rbind(dt_def_drop, dt_drop2, fill = TRUE)

drop_seqs <- dt_drop_both[, unique(seq)]
upload_drop_seqs <- xw_df_step4[seq %in% drop_seqs, .(seq, is_outlier)]
upload_drop_seqs$is_outlier <- 1 
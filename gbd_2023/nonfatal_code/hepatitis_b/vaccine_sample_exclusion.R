###########################################################################################################
# Purpose: Find places where we have data that is in 30 years old or lower in years where vaccination would have been introduced 
# Goal: outlier seroprevalence data in age groups and years where vacination had been introduced; this is to create a counterfactual scenario of what seroprevalence of HBsAg would look like in a population with no vaccination efforts
###########################################################################################################

rm(list = ls())


library(openxlsx, plyr)

## Source central functions
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/save_bulk_outlier.R")
source("FILEPATH/upload_bundle_data.R")

# date
date <- Sys.Date()
date <- gsub("-", "_", date)


# Get the bundle version associated with the correct step 
b_id <- OBJECT
release_id <- OBJECT

active_data = get_bundle_data(bundle_id=b_id) #roundgostic
orig_dt1 <- copy(active_data)
table(orig_dt1$measure)

# Subset to only prevalence data to only get literature (not CSMR) and only cols for analysis 
dt_prev <- orig_dt1[measure == "prevalence"]
dt_csmr <- orig_dt1[measure == "mtspecific" , ]
dt_other <- orig_dt1[(measure != "mtspecific" & measure!="prevalence") , ]

#dt_prev <- dt_prev[nid %in% new_nid]


nrow(dt_prev)
length(unique(dt_prev$nid))
#dt_prev_old <- copy(dt_prev)
table(dt_prev$is_outlier)
#dt_prev$is_outlier <- 0

# Read in the vaccine year of introduction data (the dataset should be static - not changing between rounds)
vac_intro <- readr::read_rds("FILEPATH/vaccine_intro.rds")
vac_intro <- vac_intro[me_name == "vacc_hepb3", ]
vac_intro <- unique(vac_intro[, .(ihme_loc_id, location_id, cv_intro)])


# Merge the data sets together 
dt_all <- merge(dt_prev, vac_intro, by = "location_id")

# Create year of birth cols based on different permutations with year_start, year_end, age_start, and age_end 
dt_all[, `:=` (yob_ys_as = round(year_start - age_start, 0), 
               yob_ys_ae = round(year_start - age_end, 0), 
               yob_ye_as = round(year_end - age_start, 0), 
               yob_ye_ae = round(year_end - age_end, 0))]
head(dt_all)

dt_all[, `:=` (keep_ys_as = ifelse(yob_ys_as < cv_intro, 1, 0), 
               keep_ys_ae = ifelse(yob_ys_ae < cv_intro, 1, 0), 
               keep_ye_as = ifelse(yob_ye_as < cv_intro, 1, 0), 
               keep_ye_ae = ifelse(yob_ye_ae < cv_intro ,1, 0))]


keep_cols <- names(dt_all)[grepl("keep", names(dt_all))]
keep_cols
dt_all[, keep_total := rowSums(.SD), .SDcols = keep_cols]
head(dt_all)

# Determine which data points to keep
dt_processing2 <- copy(dt_all)
dt_def_drop <- dt_processing2[keep_total == 0, ] #exclude the ones that did not meet the permutation requirements from line 63
nrow(dt_def_drop)
dt_processing2 <- dt_processing2[keep_total == 1 | keep_total == 2 | keep_total == 3, ]
dt_processing2[, `:=` (mean = (yob_ys_ae + yob_ye_as) / 2, sd = (yob_ye_as - yob_ys_ae) / 6 )]
dt_processing2[, coverage := pnorm(cv_intro, mean = mean, sd = sd)] #normal distribution 
dt_processing2[, drop := ifelse(coverage <= 0.5, 1, 0 )] #exclude if coverage is less than 50%
nrow(dt_processing2[drop == 1,]) # 41
dt_drop2 <- copy(dt_processing2[drop == 1, ])
dt_drop_both <- rbind(dt_def_drop, dt_drop2, fill = TRUE)
nrow(dt_drop_both)

outlier_seqs <- dt_drop_both[, unique(seq)]
dt_prev[seq %in% outlier_seqs, is_outlier := 1]
table(dt_prev$is_outlier)
dt_prev[is_outlier == 1, note_modeler := paste0(note_modeler, " | outliered due to vaccine introduction in this age group and years") ]

nrow(dt_prev[is_outlier == 1, ])
#nrow(test[is_outlier == 1, ])
dt_prev[is_outlier == 1, length(unique(nid))]
#test[is_outlier == 1, length(unique(nid))]

dt_prev[(lower==upper), `:=` (lower=NA, upper=NA)]
dt_prev[(lower>=upper), `:=` (lower=NA, upper=NA)]
dt_prev[(is.na(lower) & !is.na(uncertainty_type_value)), `:=` (uncertainty_type_value=NA)]
dt_prev[(is.na(upper) & !is.na(uncertainty_type_value)), `:=` (lower=NA, upper=NA, uncertainty_type_value=NA)]

data_filepath <-  FILEPATH
write.xlsx(dt_prev, data_filepath, sheetName = "extraction")

description <- paste("outliered data for definitely drop cases and less than 50% (pnorm) study population not vaccinated")
result <- upload_bundle_data(bundle_id = b_id, filepath = data_filepath)
bundle_version <- save_bundle_version(bundle_id = b_id, 
                                      include_clinical=NULL)
print(sprintf('Bundle version ID: %s', bundle_version$bundle_version_id))

#--------------------------------------------------------------------------------------------------
# Stats--------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------


dt_rule[, id := .GRP, by = c("keep_ys_as", "keep_ys_ae", "keep_ye_as", "keep_ye_ae")]

dt_rule2 <- copy(dt_rule[, c("id", "keep_ys_as", "keep_ys_ae", "keep_ye_as", "keep_ye_ae")])
dt_rule2[, count := .N, by = "id"]


dt_all[is_outlier == 1, ]
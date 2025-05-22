#--------------------------------------------------------------
# Project: KD RR Evidence Score
# Purpose: prep KD data for RR curve/evidence score, save xwalk version
#--------------------------------------------------------------

# setup -------------------------------------------------------

# clear workspace
rm(list=ls())

# map drives
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
} else {
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}

user <- Sys.info()['user']

# source functions
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# set objects
bundle_version <- 46899 #most up to date bundle version for bundle id 8987 (kd rr)


# pull bundle version, manipulations -------------------------------------------------------

bun <- as.data.table(get_bundle_version(bundle_version))

# standardizing risk column
bun[risk == "stage1_2", risk := "albuminuria"]
bun[risk == "stage_3", risk := "stage3"]
bun[risk == "stage_4", risk := "stage4"]
bun[risk == "stage_5", risk := "stage5"]

table(bun$risk)

# removing gout datapoints where risk is not stage3-5
bun <- bun[!acause == "msk_gout" | risk == "stage3_5"]

# removing cvd datapoints where blood_pressure == 0 (these are not controlled for blood pressure)
bun <- bun[blood_pressure == 1 | is.na(blood_pressure), ]

# subsetting to cvd datapoints that are stage specific
bun <- bun[acause == "msk_gout" | (risk %in% c("albuminuria", "stage3", "stage4", "stage5"))]

# study_id - study name or NID if NA
bun[, study_id := study_name]
bun[is.na(study_id), study_id := nid]

# calculating age_mean
bun[is.na(age_mean), age_mean := (age_start+age_end)/2]

# column for cvd outcome (tag all stroke subtypes to stroke)
bun[acause == "cvd_ihd", cvd_mrbrt_outcome := "ihd"]
bun[acause == "cvd_pvd", cvd_mrbrt_outcome := "pad"]
bun[acause %like% c("cvd_stroke"), cvd_mrbrt_outcome := "stroke"]

# calculating log standard error
bun[effect_size_unit == "log" & !is.na(standard_error), ln_rr_se := standard_error]
bun[effect_size_unit == "log" & is.na(standard_error), ln_rr_se := (upper-lower)/3.92]
bun[!effect_size_unit == "log" & !is.na(standard_error), ln_rr_se := standard_error/mean]
bun[!effect_size_unit == "log" & is.na(standard_error), ln_rr_se := (log(upper)-log(lower))/3.92]

# calculating log mean 
bun[effect_size_unit == "log", ln_rr := mean]
bun[!effect_size_unit == "log", ln_rr := log(mean)]

# final manipulations
xwalk_upload <- copy(bun)

xwalk_upload[, ':=' (crosswalk_parent_seq = as.numeric(seq),
                     seq= "",
                     bundle_id = 8987,
                     bundle_version_id = bundle_version,
                     risk_type = "dichotomous")] # is actually 'mixed' for cvd outcomes but validations doesnt allow that

xwalk_upload <- subset(xwalk_upload, select = c(seq, crosswalk_parent_seq, rei, acause, nid, underlying_nid, bundle_id, bundle_version_id,
                                                location_id, location_name, sex, year_start, year_end, age_start, age_end, 
                                                design, is_outlier, risk_type, ln_rr, ln_rr_se, risk, study_id, age_mean, cvd_mrbrt_outcome))


# save crosswalk version -------------------------------------------------------
filename <- paste0("kd_rr_xwalk_gbd_2023.xlsx")

write.xlsx(xwalk_upload, paste0("FILEPATH",filename), sheetName = "extraction", rowNames = F)

save_crosswalk_version(bundle_version_id = bundle_version, 
                       description= 'GBD 2023 RR including systematic review extractions, drop gout where exp is not stage 3-5 & fix unit to linear',
                       data_filepath =paste0("FILEPATH",filename))



rm(list = ls())

# Set run parameters
RELEASE <- 16                # The release number for which we're running
PRIOR_RELEASE <- 9           # The release number for the previous GBD cycle
LEVEL_3 <-  'intest'         # The level-3 cause for which we're running; either 'ints' or 'intest'
PARAMETER <-  'incidence'    # The parameter for which we're running; either 'incidence' or 'proportion'
OUT_MODEL <- 'full'          # Do you want to prep data for the full model or the age-pattern model? ('full' or 'age_pattern')
MODEL_GEOG <- 'data_rich'    # The geographic scope of the model; either 'global' or 'data_rich'

SPLIT_GROUPS <- TRUE         # Whether to apply splits based on group-specific data
SPLIT_SEX <- TRUE            # Whether to apply sex splits

# Where locations have been split or combined, provide a mapping of prior location IDs to new location IDs
LOC_MAP <- data.table(location_id = c(60908, 95069, 94364), prior_loc_id = c(44858, 44858, 44858))

# Establish directories (these shouldn't need changing unless we change the directory structure or processes)
EXTRACTION_DIR <- file.path('FILEPATH/intest', paste0('release_', RELEASE), 'extractions')
INPUT_DIR <- file.path('FILEPATH', LEVEL_3, paste0('release_', RELEASE), 'inputs')
MRBRT_DIR <- file.path(INPUT_DIR, 'mrbrt')
SHARED_FUN_DIR <- 'FILEPATH'


# Load libraries
library(data.table)
library(xlsx)
library(openxlsx)

# Load shared functions
source(file.path(SHARED_FUN_DIR, 'get_bundle_data.R'))
source(file.path(SHARED_FUN_DIR, 'get_age_metadata.R'))
source(file.path(SHARED_FUN_DIR, 'get_location_metadata.R'))
source(file.path(SHARED_FUN_DIR, 'get_demographics.R'))
source(file.path(SHARED_FUN_DIR, 'get_draws.R'))
source(file.path(SHARED_FUN_DIR, 'get_ids.R'))
source(file.path(SHARED_FUN_DIR, 'get_model_results.R'))
source(file.path(SHARED_FUN_DIR, 'get_population.R'))
source(file.path(SHARED_FUN_DIR, 'save_crosswalk_version.R'))
source(file.path(SHARED_FUN_DIR, 'get_crosswalk_version.R'))
source(file.path(SHARED_FUN_DIR, 'upload_bundle_data.R'))
source(file.path(SHARED_FUN_DIR, 'save_bundle_version.R'))
source(file.path(SHARED_FUN_DIR, 'get_bundle_version.R'))
source(file.path(SHARED_FUN_DIR, 'get_elmo_ids.R'))


# Load custom functions
source('FILEPATH/sex_split_functions_simple.R')
source('FILEPATH/group_specificity_split.R')
source('FILEPATH/clear_bundle_data.R')
source('FILEPATH/apply_crosswalk.R')
source('FILEPATH/age_split_data.R')
source('FILEPATH/typhOnlyAdjust.R')

# Set conditions that vary by level 3 and parameter
if (LEVEL_3 == 'intest' & PARAMETER == 'incidence') {
  
  full_bundle_in <- 556
  age_bundle <- 6398
  vr_bundle <- 556
  age_pattern_meid <- 10140
  
  FIRST_VR_KEEP_YEAR <- 1990
  
  if (MODEL_GEOG == 'global') {
    full_bundle_out <- 556
    INCLUDE_VR <- FALSE           # Whether to include data derived from vital registration or not
  } else if (MODEL_GEOG == 'data_rich') {
    full_bundle_out <- 555
    INCLUDE_VR <- TRUE           # Whether to include data derived from vital registration or not
  } else {
    stop('MODEL_GEOG must be either "global" or "data_rich"')
  }
 
  SENSITIVITY_ADJ <- TRUE          # Whether or not to apply the diagnostic sensitivity adjustment
  PASSIVE_ADJ <- TRUE              # Whether or not to apply the passive case detection adjustment
  TYPH_ONLY_ADJ <- TRUE            # Whether or not to apply the typhoid-only adjustment
  AGE_SPLIT <- OUT_MODEL == 'full' # Apply the age split if the output model is the full model
  
  
  sex_ratio_draws <- fread(file.path(MRBRT_DIR, paste0('sex_split_draws_', full_bundle_in, '.csv')))
  sex_ratio_draws[, draw := (1:.N)-1]
  setnames(sex_ratio_draws, 'V1', 'log_ratio')

} else if (LEVEL_3 == 'intest' & PARAMETER == 'proportion') {
  full_bundle_in <- 18
  full_bundle_out <- 18
  age_bundle <- 6395
  age_pattern_meid <- 1252
  vr_bundle <- 18
  
  FIRST_VR_KEEP_YEAR <- 1990
  PR_OFFSET <- 0.01 * 0.5 # (1% of 0.5)
  
  INCLUDE_VR <- FALSE           # Whether to include data derived from vital registration or not
  SENSITIVITY_ADJ <- FALSE      # Whether or not to apply the diagnostic sensitivity adjustment
  PASSIVE_ADJ <- FALSE          # Whether or not to apply the passive case detection adjustment
  TYPH_ONLY_ADJ <- FALSE        # Whether or not to apply the typhoid-only adjustment
  AGE_SPLIT <- TRUE            # No age split for this proportion model as age patterns are pretty flat -- testing with age split
  
  sex_ratio_draws <- data.table(draw = 0:999, log_ratio = rep(0, 1000))

} else if (LEVEL_3 == 'ints' & PARAMETER == 'incidence') {
  full_bundle_in <- 3785
  age_bundle <- 3023
  vr_bundle <- 3023
  age_pattern_meid <- 20291
  
  FIRST_VR_KEEP_YEAR <- 1990
  
  if (MODEL_GEOG == 'global') {
    full_bundle_out <- 3785
    INCLUDE_VR <- FALSE           # Whether to include data derived from vital registration or not
  } else if (MODEL_GEOG == 'data_rich') {
    full_bundle_out <- 3788
    INCLUDE_VR <- TRUE           # Whether to include data derived from vital registration or not
  } else {
    stop('MODEL_GEOG must be either "global" or "data_rich"')
  }
  
  SENSITIVITY_ADJ <- FALSE       # Whether or not to apply the diagnostic sensitivity adjustment
  PASSIVE_ADJ <- FALSE           # Whether or not to apply the passive case detection adjustment
  TYPH_ONLY_ADJ <- FALSE         # Whether or not to apply the typhoid-only adjustment
  AGE_SPLIT <- OUT_MODEL=='full' # Apply the age split if the output model is the full model
  
  sex_ratio_draws <- fread(file.path(MRBRT_DIR, paste0('sex_split_draws_', full_bundle_in, '.csv')))
  sex_ratio_draws[, draw := (1:.N)-1]
  setnames(sex_ratio_draws, 'V1', 'log_ratio')
  
} else {
  stop('Invalid cause or parameter')
}




### PULL IN AGE AND LOCATION METADATA ###
age_meta <- get_age_metadata(age_group_set_id = 24, release_id = RELEASE)
loc_meta <- get_location_metadata(location_set_id = 35, release_id = RELEASE)
demog <- get_demographics('epi', release_id = RELEASE)




### GET THE FULL BUNDLE AND CLASSIFY POINTS AS AGE-SPECIFIC OR NOT ###
# Pull data from the full bundle
in_dt <- get_bundle_data(full_bundle_in, export = TRUE)
setDT(in_dt)


if (OUT_MODEL == 'full') {
  out_bundle <- full_bundle_out
  out_dt <- copy(in_dt)

} else if (OUT_MODEL == 'age_pattern') {
  out_bundle <- age_bundle
  
  # Classify data points as age-specific or not
  for (i in 1:nrow(age_meta)) {
    in_dt[, paste0('is_age_group', age_meta$age_group_id[i]) := (age_start < age_meta$age_group_years_end[i] & age_end > age_meta$age_group_years_start[i])]
  }
  
  in_dt$n_age_groups <- rowSums(in_dt[, .SD, .SDcols = grep('^is_age_group', names(in_dt), value = T)], na.rm = T)
  in_dt[, age_range := age_end - age_start]
  in_dt[, is_age_specific := (n_age_groups<5 | age_range<10 | age_end<=2*age_start | age_start>40)]
  
  in_dt %>% ggplot(aes(x = age_start, y = age_end, color = is_age_specific)) + geom_point() + 
     theme_minimal() + scale_color_manual(values = c('red', 'blue'))
  
  out_dt <- copy(in_dt)[is_age_specific==T, ]
  out_dt[, c(grep('^is_age', names(out_dt), value = T), 'age_range', 'n_age_groups') := NULL]
  
  out_dt[, seq := NA]
  
} else {
  stop('Invalid output model')
}


# Drop the VR data (we'll add back in below if INCLUDE_VR is TRUE)
out_dt <- out_dt[nid != 292827, ]

if (INCLUDE_VR) {
  vr <- fread(file.path(INPUT_DIR, 'vr_data_for_dismod_bundle.csv'))[bundle_id == vr_bundle, ]
  vr[, bundle_id := out_bundle]
  vr <- vr[!is.na(mean), ]
  
  vr <- vr[year_end >= FIRST_VR_KEEP_YEAR, ]

  out_dt <- rbind(out_dt, vr, fill = T)
} 


# Fill in any missing values for location_name (not necessary, but makes it easier to look at the data)
out_dt[is.na(location_name), location_name := loc_meta$location_name[match(location_id, loc_meta$location_id)]]
out_dt[is.na(ihme_loc_id), ihme_loc_id := loc_meta$ihme_loc_id[match(location_id, loc_meta$location_id)]]

out_dt[, seq := NA]
out_dt[, bundle_id := out_bundle]


### CLEAR OUT THE BUNDLE AND UPLOAD THE NEW DATA ###
date_stamp <- gsub('-', '', Sys.Date())
upload_fname <- file.path(EXTRACTION_DIR, paste0('combined_bundle_', out_bundle, 
                                                 '_to_upload_', date_stamp, '.xlsx'))

write.csv(out_dt, file = gsub('xlsx$', 'csv', upload_fname), row.names = FALSE)
write.xlsx(out_dt, file = upload_fname, sheetName = 'extraction', rowNames = FALSE)

# Clear out the bundle
clear_bundle_data(bundle_id = out_bundle, path = EXTRACTION_DIR, export = TRUE)


# Upload data to the bundle
result <- upload_bundle_data(bundle_id = out_bundle, filepath = upload_fname)


bkup <- copy(out_dt)


### PULL DATA FROM BUNDLE AND APPLY SPLITS AND ADJUSTMENTS ###

# Pull the age-specific data from the bundle
out_dt <- get_bundle_data(bundle_id = out_bundle, export = TRUE)
bkup <- copy(out_dt)



# Merge to prior locations where locations have been split or combined so we can 
# pull prior version models where necessary
out_dt <- merge(out_dt, LOC_MAP, by = 'location_id', all.x = T)
out_dt[is.na(prior_loc_id), prior_loc_id := location_id]


# Apply the group-specificity splits
if (SPLIT_GROUPS) {
  out_dt <- split_groups(out_dt)
}

# Adjust incidence data that included only typhoid to include paratyphoid
if (TYPH_ONLY_ADJ) {
  # filter observations to those that are typhoid-only data
  out_dt[is.na(typhoid_only), typhoid_only := 0]
  to_adj <- copy(out_dt)[typhoid_only==1, ]
  no_adj <- copy(out_dt)[typhoid_only!=1, ]
  
  
  # get gbd year closest to the study year mid-point
  years <- demog$year_id
  to_adj[, year_mid := (year_start + year_end)/2]
  to_adj$gbd_year <- sapply(to_adj$year_mid, function(x) {
    years[which.min(abs(years - x))]
    })
  
  
  adj_locs <- unique(to_adj$location_id)
  adj_years <- unique(to_adj$gbd_year)
  
  # get incidence, proportion, and population estimates
  inc <- get_model_results(gbd_team = "epi", gbd_id = 10140, sex_id = -1, 
                           age_group_id = -1, location_id = adj_locs, 
                           year_id = adj_years,  release_id = PRIOR_RELEASE)
  
  pops <- get_population(location_id = adj_locs, year_id = adj_years, sex_id = 1:3, 
                         age_group_id = age_meta$age_group_id, release_id = RELEASE)
  
  pops <- merge(pops, inc, all.y = TRUE,
                by = c("age_group_id", "location_id", "year_id", "sex_id"))
  pops <- pops[, population := population * mean
               ][, .(age_group_id, location_id, year_id, sex_id, population)]
  
  pr_para <- get_draws("modelable_entity_id", 1252, source = "epi", 
                       location_id = adj_locs, year_id = adj_years,  
                       sex_id = 1:2, age_group_id = age_meta$age_group_id, 
                       release_id = PRIOR_RELEASE)
  
  adjusted <- do.call(rbind, lapply(1:nrow(to_adj), function(x) {
    typh_only_adjust(to_adj, x, pr_para, pops)
    }))
  
  out_dt <- rbind(no_adj, adjusted, fill = T, use.names = T)
}


# Apply sex-splits
if (SPLIT_SEX) {
  out_dt <- sex_split_data(out_dt, sex_ratio_draws, RELEASE)
  out_dt <- out_dt[sex != 'Both', ]
  out_dt[group_review == 0, `:=` (group_review = NA, group = NA, specificity = NA)]
  out_dt[lower < 0, lower := 0]
}

# Apply diagnostic sensitivity adjustment
if (SENSITIVITY_ADJ) {
  
  # Pull the MR-BRT model for the sex split
  dx_draws <- fread(file.path(MRBRT_DIR, 'dx_sensitivity', 'model_coefs.csv'))
  dx_draws <- exp(rnorm(1000, dx_draws$beta_soln, sqrt(dx_draws$beta_var)))
  dx_draws <- (1 + dx_draws) / dx_draws
  
  # Get mean and SE of adjustment draws
  mean_adj <- mean(dx_draws)
  se_adj <- sd(dx_draws)
  
  # Split the data into those that need adjustment and those that don't
  out_dt[grepl('Vital registration', source_type), cv_sensitivity_adjusted := 1]
  out_dt[is.na(cv_sensitivity_adjusted), cv_sensitivity_adjusted := 0]
  to_adj <- copy(out_dt)[cv_sensitivity_adjusted == 0, ]
  no_adj <- copy(out_dt)[cv_sensitivity_adjusted != 0, ]
  
  # Create note to add to adjusted rows
  note = paste('muliplied by', round(mean_adj, digits = 2), 
               'to adjust for dx sensitivity (from mr-brt)')
  
  # Apply the adjustment
  adjusted <- apply_crosswalk(to_adj, mean_adj, se_adj, add_note = note)
  out_dt <- rbind(no_adj, adjusted, fill = T)
  
  # Clean up
  rm(to_adj, no_adj, dx_draws, mean_adj, se_adj, adjusted, note)
} 


# Apply passive surveillance adjustment
if (PASSIVE_ADJ) {
  # Pull the MR-BRT model for the passive adjustment
  preds <- readRDS('FILEPATH')
  adj_draws <- data.table(preds$model_draws)[, c('X_intercept', 'Z_intercept') := NULL]
  adj_draws <- 1 / exp(as.numeric(t(adj_draws)))
  
  # Get mean and SE of adjustment draws
  mean_adj <- mean(adj_draws)
  se_adj <- sd(adj_draws)
  
  # Split the data into those that need adjustment and those that don't
  out_dt[is.na(cv_passive), cv_passive := 0]
  to_adj <- copy(out_dt)[cv_passive==1, ]
  no_adj <- copy(out_dt)[cv_passive!=1, ]
  
  # Create note to add to adjusted rows
  note = paste('muliplied by', round(mean_adj, digits = 2), 'to adjust for passive surveillance')
  
  # Apply the adjustment
  out_dt <- rbind(no_adj, apply_crosswalk(to_adj, mean_adj, se_adj, add_note = note), fill = T)
  
  # Clean up
  rm(to_adj, no_adj, adj_draws, mean_adj, se_adj, note)
  
}




if (AGE_SPLIT) {
  
  to_split <- copy(out_dt)[(age_end - age_start) > 25, ]
  no_split <- copy(out_dt)[(age_end - age_start) <= 25, ]
  
  # convert year_start and year_end to year_id by determining the closest GBD 
  # year (epi) to the study mid-point
  years <- demog$year_id
  
  to_split[, year_mid := (year_start + year_end)/2 ]
  to_split$gbd_year <- sapply(to_split$year_mid, function(x) {
    years[which.min(abs(years - x))]
    })
  
  # Since we're pulling previous cycle models, need to use previous cycle locations
  to_split[, pattern_loc_id := prior_loc_id]
  
  split_years <- unique(to_split$gbd_year)
  

  inc <- get_model_results(gbd_team = "epi", gbd_id = age_pattern_meid, 
                           location_id = unique(to_split$pattern_loc_id),  
                           year_id = split_years, sex_id = -1, 
                           age_group_id = -1, release_id = PRIOR_RELEASE)
  
  pop <- get_population(location_id = unique(to_split$location_id), 
                        year_id = split_years,  sex_id = 1:2, age_group_id = -1, 
                        release_id = RELEASE)
  
  pop <- pop[, .(age_group_id, location_id, year_id, sex_id, population)]
  
  
  age_split <- rbindlist(lapply(1:nrow(to_split), function(x) {
    age_split_data(to_split, x, inc, pop, age_meta, 
                   description = "age split off DisMod", width = 5)
    }))
  

  out_dt <- rbind(age_split, no_split, fill = T)
  
  
}

out_dt[lower < 0 | mean == 0, lower := 0]
out_dt[upper > 1 | mean == 1, upper := 1]
out_dt[!is.na(lower) & !is.na(upper), uncertainty_type_value := 95]


# Set outliers
if (out_bundle == 556) {
  out_dt[nid == 554781 & location_id == 44540, is_outlier := 1]
  out_dt[nid %in% c(133199, 129181), is_outlier := 1]
} else if (out_bundle == 3785) {
  out_dt[location_id == 35654 | location_name == 'Siaya', is_outlier := 1]
}



# Apply continuous bi-directional offset to proportion data 
# (i.e. pull all points towards 0.5 to avoid logit transform problems)
if (out_bundle == 18) {
  out_dt[, mean := mean + PR_OFFSET * (0.5-mean)/0.5]
  out_dt[mean < 0.5 & upper < 0.5, upper := upper + PR_OFFSET * (0.5-upper)/0.5]
  out_dt[mean > 0.5 & lower > 0.5, lower := lower + PR_OFFSET * (0.5-lower)/0.5]
}

# Outlier extremely high typhoid incidence data in older groups from VR
if(out_bundle == 555) {
  out_dt[mean >= 0.001 & nid == 292827 & age_start > 50, is_outlier := 1]
}


# Check to ensure that everything looks right
summary(out_dt[, .(mean, lower, upper)])
table(out_dt$mean > out_dt$upper)
table(out_dt$mean < out_dt$lower)
table(out_dt$uncertainty_type_value, useNA = 'always')
table(out_dt$measure)

# Write the sex-split age-specific data to the bundle
date_stamp <- gsub('-', '', Sys.Date())
upload_fname <- file.path(EXTRACTION_DIR, paste0('adj_split_bundle_', out_bundle, 
                                                 '_to_upload_', date_stamp, '.xlsx'))

write.xlsx(out_dt, file = upload_fname, sheetName = 'extraction', rowNames = FALSE)

description <- 'Age/Sex split with updated GBD 2023 data; continuous offset'

SAVE_BUNDLE_VERSION <- TRUE
if (SAVE_BUNDLE_VERSION) {
  save_bundle_version_result <- save_bundle_version(bundle_id = out_bundle)
} else {
  save_bundle_version_result <- get_elmo_ids(bundle_id = out_bundle)[
    order(bundle_version_id, decreasing = TRUE), ][1, ]
}

result <- save_crosswalk_version(
  bundle_version_id = save_bundle_version_result$bundle_version_id,
  data_filepath = upload_fname,
  description = description)




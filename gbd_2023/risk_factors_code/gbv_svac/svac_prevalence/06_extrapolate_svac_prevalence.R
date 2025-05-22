###############################################################################################
#
# Purpose: Extrapolate SVAC prevalence over time
#
###############################################################################################
rm(list=ls())

#both sex xwalk
xw_version_id <- 'XW ID'

#timeframe of input data
min_year <- 1980
max_year <- 2022

#borderline outliers - do not extrapolate
fem_do_not_extrapolate <- c('NID LIST')
male_do_not_extrapolate <- c('NID LIST')

#se inflation factors
inflate <- 2


##### 0. SET UP ###############################################################################

#libraries + central functions
pacman::p_load(data.table, dplyr, openxlsx, ggplot2, stringr, lme4, splines, boot, parallel)
invisible(sapply(list.files("FILEPATH", full.names = T), source))
z <- qnorm(0.975)

#custom function to calculate standard error
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error), standard_error := sqrt(val*(1-val)/sample_size + z^2/(4*sample_size^2))]
  return(dt)
}

#custom function to calculate sample sizes
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(sample_size) & !is.na(val) & !is.na(standard_error), sample_size := val/(standard_error^2)] #from epi uploader
  return(dt)
}

#location and age metadata
locs <- get_location_metadata(location_set_id = 22, release_id = 16)
ages <- get_age_metadata(age_group_set_id=19, release_id = 9) %>%
  setnames(c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end')) %>%
  mutate(age_end = age_end - 1)


##### 1. GET + FORMAT DATA ####################################################################

#get crosswalk data; rename val to match edu code
csa_xw <- get_crosswalk_version(xw_version_id)

#sex information
csa_xw[sex == "Male", sex_id := 1]
csa_xw[sex == "Female", sex_id := 2]

#make sure all location metadata is in the crosswalk
csa_xw$ihme_loc_id <- NULL
csa_xw <- merge(csa_xw, locs[, .(location_id, ihme_loc_id, level)], by = 'location_id')

#calculate sample size if missing
csa_xw[is.na(sample_size) & !is.na(effective_sample_size), sample_size := effective_sample_size]
csa_xw <- get_se(csa_xw)
csa_xw <- get_cases_sample_size(csa_xw)

#extrapolate national non-outliers with sample sizes greater than 100 for people aged 20+ (15-19 year olds can still accrue cases of CSA before 18 under SDG def)
csa_subset <- csa_xw[level == 3 & sample_size > 100 & (is_outlier == 0 | is.na(is_outlier)) & age_start >= 20] 

#fix the "age split" points that can actually be extrapolated - aka, fix the mislabeled // misleading points --------------------------------
#by "misleading," we refer to the data points that were age split into a single value (ex: a 20-23-year-old data point looks "age split" because of the non-standard age bin)

#example 1: a 15-17-year-old extraction still shows up as age split bc it's not a standard age bin
#solution:  find the unique nid-location-years that only appear once (meaning there's only one age band represented)
csa_subset[!is.na(age_split_tag), split_count := length(val), by = .(nid, year_id, location_id, sex_id)]
csa_subset[split_count == 1, age_split_tag := NA]
csa_subset$split_count <- NULL

#example 3: a 10-19-year-old extraction is age split, but we only use the 15-19 to model - we assume it's okay to keep
#solution:  find the data points where the max age-split-start is 15
csa_subset[!is.na(age_split_tag), max_age := max(age_start), by = .(nid, year_id, location_id, sex_id)]
csa_subset[max_age == 20, age_split_tag := NA]
csa_subset$max_age <- NULL

#except for the studies that are on the cusp of being outliered
csa_subset[(sex_id == 1 & nid %in% male_do_not_extrapolate) |
             (sex_id == 2 & nid %in% fem_do_not_extrapolate), age_split_tag := 1]


#--------------------------------------------------------------------------------------------------------------------------------------------

#now we subset to those that were not age split (or that were manually fixed above)
csa_subset <- csa_subset[!grepl('agesplit', note_modeler) & !grepl('age split', note_modeler) | is.na(age_split_tag)]

#confirm extrap exclusions apply
csa_subset <- csa_subset[!(sex_id == 1 & nid %in% male_do_not_extrapolate)]
csa_subset <- csa_subset[!(sex_id == 2 & nid %in% fem_do_not_extrapolate)]

#fix zero/low means
mean_adj_val <- quantile(csa_subset[val!=0]$val, probs=0.025)
csa_subset[val < mean_adj_val, val := mean_adj_val]


##### 2. FORECAST AND BACKCAST DATA ###########################################################

#move data points backwards in time (copy 25-29 in 2005 and save as 20-24 in 2000)
back_cast <- function(dt,time) {
  timecast.dt <- dt[age_start >= 20 + time]
  timecast.dt[, age_orig := age_start]
  timecast.dt <- timecast.dt[, age_start := age_start - time]
  timecast.dt <- timecast.dt[, year_id := year_id - time]
  timecast.dt[, `:=` (year_start = year_id, year_end = year_id)]
  timecast.dt[, time_shift := -1*time]
  timecast.dt[, inflate_factor := time / 5]
  timecast.dt[, extrapolated := 1] #flag that it's extrapolated
  timecast.dt[, crosswalk_parent_seq := seq] #reset seq
  timecast.dt[, seq := NA]
  return(timecast.dt)
}

#move data points forwards in time (copy 20-24 in 2000 and save as 25-29 in 2005)
for_cast <- function(dt,time) {
  timecast.dt <- dt[age_start <= 95 - time & age_start >= 20]
  timecast.dt[, age_orig := age_start]
  timecast.dt <- timecast.dt[, age_start := age_start + time]
  timecast.dt <- timecast.dt[, year_id := year_id + time]
  timecast.dt[, `:=` (year_start = year_id, year_end = year_id)]
  timecast.dt[, time_shift := time]
  timecast.dt[, extrapolated := 1] #flag that it's extrapolated
  timecast.dt[, crosswalk_parent_seq := seq] #reset seq
  timecast.dt[, seq := NA]
  return(timecast.dt)
}

#create backcasts and forecasts
back.casts.list <- mclapply(X = seq(5, 65,5), FUN = back_cast, dt = csa_subset, mc.cores = 1) 
for.casts.list <- mclapply(X = seq(5, 80,5), FUN = for_cast, dt = csa_subset, mc.cores = 1)

#merge
predicted_vals <- rbind(rbindlist(back.casts.list), rbindlist(for.casts.list))

#reset age_end + age_group_id since we only changed age start
predicted_vals[, age_end := age_start + 4]
predicted_vals[age_start == 95, age_end := 124]
predicted_vals$age_group_id <- NULL
predicted_vals <- merge(predicted_vals, ages[, .(age_start, age_end, age_group_id)], all.x = T)

#restrict to min and max years
predicted_vals <- predicted_vals[year_id >= min_year & year_id <= max_year]

#inflate standard error; recalculate variance and UIs; trim UIs that go beyon 0 and 1
predicted_vals[, standard_error := standard_error*inflate]
predicted_vals[, variance := standard_error^2]
predicted_vals[, `:=` (lower = val - z*standard_error, 
                       upper = val + z*standard_error)]
predicted_vals[lower < 0, lower := 0]
predicted_vals[upper > 1, upper := 1]


##### 3. SAVE FOR ST-GPR MODEL ################################################################

#note: the predicted values do not have the original year_id - we can just bind everything!
stgpr_inputs <- rbind(csa_xw, predicted_vals, fill = T)

#confirm that it has the information needed for STGPR
stgpr_inputs[, measure_id := 18]
stgpr_inputs[is.na(seq), seq := .I]

#save root
save_root <- paste0('FILEPATH')

#save all together and by sex
write.csv(stgpr_inputs, paste0(save_root, 'all_xw-', xw_version_id, '_predicted_inputs_inflated_by_', inflate,'.csv'), row.names = F)
write.csv(stgpr_inputs[sex_id == 1], paste0(save_root, 'male_csa_xw-', xw_version_id, '_predicted_inputs_inflated_by_', inflate,'.csv'), row.names = F)
write.csv(stgpr_inputs[sex_id == 2], paste0(save_root, 'fem_csa_xw-', xw_version_id, '_predicted_inputs_inflated_by_', inflate,'.csv'), row.names = F)

library(msm)
library(data.table)

SHARED_FUN_DIR <- 'FILEPATH'
source(file.path(SHARED_FUN_DIR, 'get_population.R'))



get_row <- function(n, dt, pop_dt) {
  row <- copy(dt)[n]

  row[age_start >= 0.999, age_end := ceiling(age_end)]
  pops_sub <- pop_dt[location_id == row[, location_id] & 
                       as.integer(year_id) == row[, as.integer(midyear)] &
                       age_group_years_start >= row[, age_start]
                     ][age_group_years_end <= row[, age_end] | 
                         age_group_years_end == min(age_group_years_end), ]
  
  agg <- pops_sub[, .(pr_pop = sum(population, na.rm = T)), by = c('sex')]
  agg[, pr_pop := pr_pop / sum(pr_pop)]
  agg[, merge := 1]
  setnames(agg, 'sex', 'sex_new')
  row <- cbind(rbind(row, row), agg)
  return(row)
}


sex_split_data <- function(dt, ratio_draws, release_id) {
  tosplit_dt <- copy(dt)
  nosplit_dt <- tosplit_dt[sex %in% c('Male', 'Female') | 
                             (sex == 'Both' & group_review == 0)]
  tosplit_dt <- tosplit_dt[sex == 'Both' & 
                             (group_review == 1 | is.na(group_review))]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]
  
  # Measure columns that are all NAs are being read as logical, covert all to numeric
  measure_vars <- c('mean', 'lower', 'upper', 'standard_error', 'cases', 
                    'sample_size', 'uncertainty_type_value')
  tosplit_dt[, c(measure_vars) := lapply(.SD, as.numeric), 
             .SDcols = measure_vars]
  
  # Where cases or sample size is missing but we have values that allow direct
  # calculation, do that here
  tosplit_dt[is.na(sample_size) == T & is.na(cases) == F & is.na(mean) == F, 
             sample_size := cases / mean]
  tosplit_dt[is.na(mean) == T & is.na(cases) == F & is.na(sample_size) == F, 
             mean := cases / sample_size]
  

  
  
  # Get the population estimates that correspond to the data points
  message('Getting population data')
  pops <- get_population(location_id = tosplit_dt[, unique(location_id)],  
                         year_id = tosplit_dt[, unique(midyear)],  sex_id = 1:2,
                         age_group_id = 49:147, single_year_age = T, 
                         release_id = release_id)
  pops <- rbind(pops, get_population(location_id = tosplit_dt[, unique(location_id)],  
                                     year_id = tosplit_dt[, unique(midyear)], 
                                     sex_id = 1:2, age_group_id = 2:4, 
                                     release_id = release_id))
  
  dMeta <- merge(data.frame(age_group_id = c(2:4,49:147), 
                            age_group_years_start = c(0, 0.01917808, 0.07671233, 1:99), 
                            age_group_years_end = c(0.01917808, 0.07671233, 1:100)), 
                 data.frame(sex_id = 1:2, sex = c('Male', 'Female'), stringsAsFactors = F),
                 all = T)
  
  pops <- merge(pops, dMeta, by = c('age_group_id', 'sex_id'), all.x=T)
  setDT(pops)
  
  
  # Get the population sex proportions (use this adjust standard errors of split 
  # points relative to acutal sex ration in population)
  message('Getting population sex proportions')
  tosplit_dt <- rbindlist(lapply(1:nrow(tosplit_dt), function(x) {
    get_row(n = x, dt = tosplit_dt, pop_dt = pops)}))
  
  
  # Create a row index that we can use to merge back onto later
  tosplit_dt[, row_index := 1:.N]
  
  
  # Bring in sex-ratio draws from MR-BRT model
  split_dt <- merge(tosplit_dt, copy(ratio_draws)[, merge := 1], 
                    by = 'merge', allow.cartesian = T)
  

  message('Creating draws of mean')
  # Calculate SEs from UIs if needed
  split_dt[is.na(standard_error) & (measure == 'incidence' | mean < 0.5), 
           standard_error := (upper - mean) / qnorm(0.975)]
  split_dt[is.na(standard_error) & (measure != 'incidence' & mean >= 0.5), 
           standard_error := (mean - lower) / qnorm(0.975)]
  
  # Adjust SEs to account for splitting into 2 data points
  split_dt[, standard_error := sqrt(standard_error^2 / pr_pop)] 
  split_dt[, sample_size := sample_size * pr_pop]
  
  
  # Use method of moments to calculate parameters of beta distribution
  split_dt[, alpha := mean * (mean - mean^2 - standard_error^2) / standard_error^2] 
  split_dt[, beta := alpha * (1 - mean) / mean]
  
  # Pull draws from beta distribution where we don't have sample size, or from binomial where we do
  split_dt[is.na(sample_size) == T, 
           mean_draw := rbeta(.N, alpha, beta)]
  split_dt[is.na(sample_size) == F, 
           mean_draw := rbinom(.N, round(sample_size), mean) / round(sample_size)]
  
  
  # Apply sex-ratio
  # Calculate sex multipliers
  split_dt[sex_new == 'Female', 
           sex_mult := 1/(exp(log_ratio) * (1 - pr_pop) + pr_pop)]
  split_dt[sex_new == 'Male', 
           sex_mult := 1/(pr_pop + (1 / exp(log_ratio)) * (1 - pr_pop))]
  
  # Apply sex multiplier
  split_dt[, mean_draw := mean_draw * sex_mult]

  # Collapse draws to needed summary stats
  message('Collpsing draws')
  split_dt <- split_dt[, .(mean_ss = mean(mean_draw, na.rm = T),
                           lower_ss = quantile(mean_draw, 0.025, na.rm = T), 
                           upper_ss = quantile(mean_draw, 0.975, na.rm = T),
                           standard_error_ss = sd(mean_draw, na.rm = T),
                           sample_size_ss = mean(sample_size, na.rm = T),
                           sex_mult = mean(sex_mult, na.rm = T),
                           sex_mult_se = sd(sex_mult, na.rm = T)), 
                       by = row_index]
  
  
  # Merge draw-based estimates back into original data
  tosplit_dt <- merge(tosplit_dt, split_dt, by = 'row_index')
  
  
  tosplit_dt[, se_upper := ifelse(is.na(upper), standard_error, 
                                  (upper - mean)/qnorm(0.975))]
  tosplit_dt[, se_lower := ifelse(is.na(lower), standard_error, 
                                  (mean - lower)/qnorm(0.975))]
  
  # Recalculate sex-split mean (no uncertainty applied to mean calculation)
  message('Updating values to reflect splits')
  tosplit_dt[, mean := mean * sex_mult]
  tosplit_dt[!(mean %in% c(0, 1)) & !is.na(lower_ss), lower := lower_ss]
  tosplit_dt[!(mean %in% c(0, 1)) & !is.na(upper_ss), upper := upper_ss]


  # Where mean is zero and we have UIs, apply uncertainty to upper bound
  tosplit_dt[mean == 0 & is.na(standard_error), 
             standard_error := upper/qnorm(0.975)]
  tosplit_dt[mean == 0, upper := sqrt(standard_error^2 / pr_pop) * qnorm(0.975)]
  tosplit_dt[mean == 0, lower := 0]
  
  # Where mean is zero and we have sample size, use binomial CI
  tosplit_dt[mean == 0 & is.na(upper), 
             upper := BinomCI(cases, sample_size_ss, method = 'wilson')[, 3]]
  
  
  # Where mean is one and we have UIs, apply uncertainty to lower bound
  tosplit_dt[mean == 1 & is.na(standard_error), 
             standard_error := (1 - lower) / qnorm(0.975)]
  tosplit_dt[mean == 1, 
             lower := mean - sqrt(standard_error^2 / pr_pop) * qnorm(0.975)]
  tosplit_dt[mean == 1, 
             upper := 1]
  
  # Where mean is one and we have sample size, use binomial CI
  tosplit_dt[mean == 1 & is.na(lower), 
             lower := BinomCI(sample_size_ss, sample_size_ss, method = 'wilson')[, 2]]
  

  # Given extreme draws, we may end up with bounds that are not ordered. 
  # Also, where we have large SEs draws creation may return NA.
  # Deal with the edge-cases here
  tosplit_dt[upper < mean & !is.na(standard_error_ss), 
             upper := mean + standard_error_ss * qnorm(0.975)]
  tosplit_dt[is.na(upper_ss) & !(mean %in% c(0,1)), 
             upper := mean + sqrt((sex_mult^2 * se_upper^2) + 
                                    ((mean/sex_mult)^2 * sex_mult_se^2)) * 
               qnorm(0.975)]
  tosplit_dt[upper > 1, upper := 1]
  
  tosplit_dt[lower > mean & !is.na(standard_error_ss), 
             lower := mean - standard_error_ss * qnorm(0.975)]
  tosplit_dt[is.na(lower_ss) & !(mean %in% c(0,1)), 
             lower := mean - sqrt((sex_mult^2 * se_lower^2) + 
                                    ((mean/sex_mult)^2 * sex_mult_se^2)) * 
               qnorm(0.975)]
  tosplit_dt[lower < 0, lower := 0]
  
  # Clear out SE, SS, and cases to ensure that new uncertainty and mean are used
  tosplit_dt[, c('standard_error', 'sample_size', 'cases') := NA]
  tosplit_dt[, sex := sex_new]
  

  message('Prepping for upload')
  tosplit_dt[, crosswalk_parent_seq := seq]
  
  add_note = paste0('sex split with female/male ratio: ', 
                    round(mean(exp(ratio_draws$log_ratio)), 2), ' (', 
                    round(sd(exp(ratio_draws$log_ratio)), 2), ')')
  
  if (!('note_modeler' %in% names(tosplit_dt))) tosplit_dt[, note_modeler := '']
  tosplit_dt[, note_modeler := paste(note_modeler, add_note, sep = ' | ')]
  
  tosplit_dt[specificity != '', specificity := paste(specificity, 'sex', sep = ', ')]
  tosplit_dt[, seq := NA]
  
  tosplit_dt <- tosplit_dt[, c(names(dt), 'crosswalk_parent_seq'), with = F]
  
  tosplit_dt[, uncertainty_type_value := 95]
  return(rbind(tosplit_dt, nosplit_dt, fill = T))
}

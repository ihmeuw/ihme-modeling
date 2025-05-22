#' @author 
#' @date 2020/04/14
#' @description age_split
#' @function run_sex_split

# SOURCE FUNCTIONS --------------------------------------------------------
source("filepath/get_population.R")
source("filepath/get_model_results.R")
source("filepath/get_age_metadata.R")
source("filepath//bool_check_age_splits.R")

age_split_data <- function(xwalk_final_dt, cause, gbd_round_id, ds, model_version_id = NULL){
  dt <- copy(xwalk_final_dt)
  # pull gbd age group mapping
  age_dt <- get_age_metadata(19, gbd_round_id = gbd_round_id)
  age_dt[, age_group_weight_value:= NULL]
  setnames(age_dt, c("age_group_years_start", "age_group_years_end"), c("gbd_age_start", "gbd_age_end"))
  
  # pull global population
  pop_dt <- get_population(age_group_id = c(age_dt$age_group_id,21), # most granular and 80+
                           location_id = unique(dt$location_id), 
                           year_id = unique(c(dt$year_start,dt$year_end)),
                           sex_id = c(1,2), 
                           gbd_round_id = gbd_round_id, 
                           decomp_step = ds)
  
  # fill in cases, sample size, mean
  dt <- get_cases_sample_size(dt)
  get_se <- function(raw_dt){
    cat("\nCalling custom get_se function\n")
    dt <- copy(raw_dt)
    dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
    z <- qnorm(0.975)
    dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]  # NaNs....varicella
    dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
    dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    dt[is.na(standard_error) & measure == "cfr", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    return(dt)
  }
  
  ## GET CASES IF THEY ARE MISSING
  calculate_cases_fromse <- function(raw_dt){  # add "proportion" here?
    cat("\nCalling custom calculate_cases_fromse function\n")
    dt <- copy(raw_dt)
    dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
    dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
    ## add something for "proportion" or "cfr"?
    dt[is.na(cases), cases := mean * sample_size]
    return(dt)
  }
  
  # dt <- get_se(dt)
  # dt <- calculate_cases_fromse(dt)

  # age split rows with age range greater than 20 years but exclude 80+, and rows with no sample size and group review 0 rows
  split_rows <- dt[, .I[measure == "incidence"
                                    & (age_end - age_start > 20 & !(age_start >= 80))
                                    & !(is.na(sample_size) & is.na(effective_sample_size))
                                    & (group_review == 1 | is.na(group_review))]] 
  split_dt <- dt[split_rows]
  no_split_dt <- dt[!(split_rows)]
  if (nrow(split_dt) + nrow(no_split_dt) != nrow(dt)) {
    stop("Dataframe to be age-split and dt not to be split are not mutually exclusive and collectively exhauastive")
  }
  if (nrow(split_dt) == 0) {
    stop("there are no rows to age split.  confirm that this is correct.  if so, skip age split.")
  }
  
  # convert to age_start and age_end to closest gbd age groups
  split_dt[, gbd_age_start:= sapply(1:nrow(split_dt), function(i) {
    age <- split_dt[i, age_start]
    get_closest_age(start = T, age, age_dt)
  })]
  split_dt[, gbd_age_end:= sapply(1:nrow(split_dt), function(i) {
    age <- split_dt[i, age_end]
    get_closest_age(start = F, age, age_dt)
  })]
  
  # get starting and ending gbd age group ids
  split_dt <- get_gbd_age_group_id(split_dt, age_dt)
  age_dt[, most_detailed := NULL]
  age_dt$age_group_name <- NULL
  
  # "save a copy" of split_dt before splitting numerator and denominator
  pre_split_dt <- copy(split_dt)
  
  # split denominator using location-specific population distribution
  denominator_split_dt_list <- pblapply(1:nrow(split_dt), function(i) {
    # print(paste0(i, " out of ", nrow(split_dt), " denominators split"))
    split_denominator(split_dt[i], age_dt, pop_dt, most_detailed = T)
  }, cl = 100)
  split_dt <- rbindlist(denominator_split_dt_list)
  
  # split numerator using global age pattern from clinical data
  global_cases_dt <- get_clinical_age_pattern(gbd_round_id, cause)

  numerator_split_dt_list <- pblapply(1:nrow(split_dt), function(i) {
    # print(paste0(i, " out of ", nrow(split_dt), " numerators split"))
    split_numerator(split_dt[i], age_dt, global_cases_dt, most_detailed = T)
  }, cl = 100)
  split_dt <- rbindlist(numerator_split_dt_list)
  # check that age-split cases and sample sizes sum up to the original
  if (!bool_check_age_splits(split_dt)) {
    stop(print("Age-split sample sizes and cases do not sum up to the originals"))
  }
  
  # format for upload
  split_dt[, age_split_mean := age_split_cases / age_split_sample_size]
  split_dt[!is.na(specificity) & specificity != "", specificity := paste0(specificity, " | age")]
  split_dt[is.na(specificity) | specificity == "", `:=`(group = 1, group_review = 1, specificity = "age")]
  split_dt[, note_modeler:= paste0(note_modeler, "| multiplied by ", round(age_split_mean / mean, 2), " to age-split")]
  split_dt[, `:=`(mean = age_split_mean, cases = age_split_cases, sample_size = age_split_sample_size, crosswalk_parent_seq = seq, seq = NA,
                  effective_sample_size = age_split_sample_size, age_start = gbd_age_start, age_end = gbd_age_end)]
  cols.remove <- c("age_split_mean", "age_split_cases", "age_split_sample_size", "age_group_id", "age_group_id_start", 
                   "age_group_id_end", "gbd_age_start", "gbd_age_end", "year_match")
  split_dt[, (cols.remove) := NULL]
  final_age_split_dt <- rbind(no_split_dt, split_dt, fill = T)
  return(final_age_split_dt)
}
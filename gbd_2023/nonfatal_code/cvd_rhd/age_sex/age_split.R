
##
## Author: USERNAME
## Date: DATE
##
## Purpose: Age-split data using age patterns from a Dismod model.
##
##

os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILENAME"
  h<-"FILENAME"
} else {
  j<- "FILENAME"
  h<-paste0("FILENAME", Sys.info()[["user"]], "/")
  lib<-paste0("FILENAME", Sys.info()[["user"]])
}

date<-gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, openxlsx, gridExtra, roxygen2, dplyr)

central <- "FILEPATH"
code_root <- "FILEPATH"

# Central comp shared functions
for (func in paste0(central, list.files(central))) source(paste0(func))

# CVD helper functions
source(paste0(code_root, "FILEPATH/get_recent.R"))
source(paste0(code_root, "FILEPATH/data_tests.R"))
source(paste0(code_root, "FILEPATH/job_hold.R"))
source('FILEPATH/bundle_process.R')


#'
#' Age-split
#'
#' @details Age-split data points using the age pattern from a specified Dismod model. 
#'
#' @params df Data frame in epi database format. Requires columns age_start, age_end, mean, lower, upper, standard_error.
#' @params model_id Modelable Entity ID (MEID) of the Dismod model you want to use for the age pattern.
#' @params decomp_step Decomp step, defauts to iterative. Should be formatted "step2" or "iterative".
#' @params measure Measure of interest. Character string such as "prevalence" or "incidence". Make sure it fits what's in DisMod.
#'
#' @return age_split_data Data frame of age-split data.
#'


age_split <- function(df,
                      model_id,
                      model_version_id=NULL,
                      decomp_step = "iterative",
                      gbd_round_id = 7,
                      measure,
                      drop_group_review=F,
                      drop_age_groups = NULL,
                      global_age_pattern
                      ) {

  use_mvid <- F
  if (!(measure%in%c("prevalence", "incidence", "continuous", "cfr", "proportion"))) stop("Single parameter models can only be prevalence, incidence, continuous, proportion, or cfr")
  if (!missing(model_version_id)) {
    use_mvid <- T
    message("Model version ID is inputted so it will supercede decomp arguments.")
  }

  orig_names <- names(df)

  ages <- get_age_metadata(age_group_set_id = 19, gbd_round_id = gbd_round_id)
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  ages[age_start >= 1, age_end := age_end - 1]

  measures <- as.data.table(read.csv("FILEPATH/gbd2020_measure_ids.csv"))
  setnames(measures, "measure", "measure_abbr")
  measure_id <- measures[measure_abbr == measure, measure_id]

  drop_over_1 <- ifelse(measure %in% c("proportion", "prevalence", "incidence"), 1, 0)

  ################ 1. FORMAT DATA ####################

  message("Formatting data.")

  ## Format as data.table, get rid of group review and outliers, etc.
  data <- as.data.table(df)
  if (!(is.null(measure))) data <- data[measure %in% measure,]
  if (drop_group_review) data <- data[!(group_review==0),]
  data <- data[is_outlier == 0,]

  ## Sex ID and year ID variables
  data[, sex_id := ifelse(sex=="Male", 1, ifelse(sex=="Female", 2, 3))]
  data[, year_id := round((year_start + year_end)/2, 0)]

  if (3 %in% unique(data$sex_id)) stop("Sex-split before age-splitting.")

  ## Coerce these columns to numeric if they aren't already
  num_cols <- c("mean", "lower", "upper", "standard_error", "cases", "sample_size", "age_start", "age_end",
                "year_start", "year_end", "group_review", "group", "is_outlier", "recall_type_value", "uncertainty_type_value")
  lapply(X = num_cols, FUN = check_class, df=data, class="numeric", coerce=T)

  ## Fill in mean, cases, sample size if not inputted
  data[is.na(mean), mean:=cases/sample_size]
  data[is.na(cases) & !(is.na(sample_size)), cases := mean * sample_size]
  data[is.na(sample_size) & !(is.na(cases)), sample_size := cases / mean]

  ## Fill in standard error when not inputted, based on uploader formulas
  data[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  data[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  data[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  data[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  data[is.na(standard_error) & measure %in% c("cfr", "mtwith", "mtstandard", "proportion", "continuous"), standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]

  ## Fill in cases from sample size
  data[is.na(cases) & is.na(sample_size) & measure %in% c("prevalence", "cfr", "mtwith", "mtstandard", "proportion", "continuous"), sample_size := (mean*(1-mean)/standard_error^2)]
  data[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  data[is.na(cases), cases := mean * sample_size]

  ## Fill in upper, lower
  data[!is.na(mean) & !is.na(standard_error) & is.na(lower), lower := mean - (1.96 * standard_error)]
  data[!is.na(mean) & !is.na(standard_error) & is.na(upper), upper := mean + (1.96 * standard_error)]

  data[cases>sample_size, `:=` (cases=NA, sample_size=NA)]

  ## Make ID variable for splitting. One per row.
  data[, id := .GRP, by=names(data)]

  ## If SR-level age patten is being used and there is not already a column for SR ID, add it
  if (!global_age_pattern&!("super_region_id" %in% names(data))){
    locs = get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round_id)
    data = merge(data, locs[, c("location_id", "super_region_id")], by="location_id")
  }


  ################ 2. CREATE NEW AGE ROWS ####################

  message("Creating new age rows.")

  ## Pull out data you need to split, 'to_split'
  data[age_end > 99, age_end := 99]
  to_split <- copy(data[age_end - age_start > 25])

  ## Round age groups
  to_split[, age_start := age_start - age_start %%5]
  to_split[, age_end := age_end - age_end %%5 + 4]

  ## n_age is the number of rows the data point will be split into.
  to_split[, n_age := (age_end + 1 - age_start)/5]
  to_split[, age_start_floor := age_start]
  ids_small <- to_split[cases/n_age > 1, id]
  to_split <- to_split[cases/n_age > 1,] ## dropping rows where there would be fewer than one case in each split row

  ## Expand for age. Create n_age number of points for each ID (row).
  expanded <- data.table(id = rep(to_split$id, to_split$n_age))
  split_dt <- merge(expanded, to_split, by="id", all=T)
  split_dt[, age_rep := 1:.N - 1, by=.(id)]

  ## Use age_rep to re-assign age_start and age_end
  split_dt[, age_start := age_start + age_rep * 5]
  split_dt[, age_end := age_start + 4]

  split_dt <- merge(split_dt, ages, by=c("age_start", "age_end"), all.x=T)
  split_dt[age_start == 0 & age_end == 4, age_group_id := 1]
  split_dt[age_start == 95 & age_end == 99, age_group_id := 235]
  if (!is.null(drop_age_groups)) split_dt <- split_dt[!(age_group_id %in% drop_age_groups),]

  ################ 3. GET POPULATION STRUCTURE ####################

  message("Getting population structure.")

  ## Pull the locations and years you need to estimate for
  if (global_age_pattern){
    pop_locs = 1
  } else {
    pop_locs = unique(split_dt$super_region_id)
  }
  #pop_locs <- unique(split_dt$location_id)
  pop_years <- unique(split_dt$year_id)
  pop_ages <- unique(split_dt$age_group_id)

  ## Pull population structure
  populations <- get_population(location_id = pop_locs, year_id = pop_years, decomp_step = decomp_step,
                                sex_id = c(1, 2, 3), age_group_id = pop_ages, gbd_round_id = gbd_round_id)

  ## Collapse populations under 1
  age_1 <- copy(populations)[age_group_id %in% c(2, 3, 388, 389)]
  age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
  age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
  age_1[, age_group_id := 1]

  populations <- rbind(populations[!(age_group_id %in% c(2, 3, 388, 389))], age_1)


  ################ 4. GET AGE PATTERN FROM DISMOD ####################

  message("Getting age pattern from Dismod draws.")

  # Round year to nearest dismod year (1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022)
  split_dt[ , dismod_year := ifelse(year_id<2017, 5*round(year_id/5), ifelse(year_id%in%c(2017, 2018), 2019, year_id))]
  split_dt[ , dismod_year := ifelse(dismod_year<1990, 1990, dismod_year)]
  split_dt[ , dismod_year := ifelse(dismod_year>2022, 2022, dismod_year)]

  ## Age pattern for the locations and years you need to estimate for
  if (use_mvid) {
    age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = model_id, version_id = model_version_id,
                             measure_id = measure_id, location_id = pop_locs, source = "epi",
                             sex = c(1, 2), gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                             age_group_id = pop_ages, year_id = unique(split_dt$dismod_year))

  } else {
    age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = model_id,
                             measure_id = measure_id, location_id = pop_locs, source = "epi",
                             status = "best", sex = c(1, 2), gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                             age_group_id = pop_ages, year_id = unique(split_dt$dismod_year))
  }

  ## Get means from draws
  draws <- names(age_pattern)[grepl("draw", names(age_pattern))]
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis, year_id)]

  ## Calculate cases and sample of the Dismod pattern
  age_pattern[measure_id %in% c(5, 9, 13, 18, 19, 17), sample_size_dis := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[measure_id == 6, sample_size_dis := rate_dis/se_dismod^2]
  age_pattern[, cases_dis := sample_size_dis * rate_dis]
  age_pattern[is.nan(sample_size_dis), sample_size_dis := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
  age_pattern[is.nan(cases_dis), cases_dis := 0]

  age_pattern <- age_pattern[ , .(age_group_id, year_id, sex_id, measure_id, cases_dis, sample_size_dis, rate_dis, se_dismod, location_id)]
  setnames(age_pattern, old="year_id", new="dismod_year")

  ################ 5. USE AGE STRUCTURE TO CREATE NEW AGE-SPECIFIC POINTS ####################

  message("Using Dismod pattern to age-split data points.")

  ## Merge in on age and population pattern
  if (global_age_pattern){
    split_dt <- merge(split_dt, populations[, .(sex_id, age_group_id, year_id, population)], by=c("sex_id", "age_group_id", "year_id"))
    split_dt <- merge(split_dt, age_pattern[, .(sex_id, age_group_id, dismod_year, cases_dis, sample_size_dis, rate_dis, se_dismod)], by=c("sex_id", "age_group_id", "dismod_year"))
  } else{
    # set location id to SR id so it can be merged back on data
    setnames(populations, old="location_id", new="super_region_id")
    setnames(age_pattern, old="location_id", new="super_region_id")

    split_dt <- merge(split_dt, populations, by=c("sex_id", "age_group_id", "super_region_id", "year_id"))
    split_dt <- merge(split_dt, age_pattern, by=c("sex_id", "age_group_id", "super_region_id", "dismod_year"))
  }
  #split_dt <- merge(split_dt, populations, by=c("sex_id", "age_group_id", "location_id", "year_id"))
  #split_dt <- merge(split_dt, age_pattern, by=c("sex_id", "age_group_id", "location_id"))
  split_dt$dismod_year= NULL

  ## Split out
  split_dt[, total_pop := sum(population), by="id"]
  split_dt[, sample_size := (population/total_pop)*sample_size]
  split_dt[, cases_dis := sample_size * rate_dis]
  split_dt[, total_cases_dis := sum(cases_dis), by="id"]
  split_dt[, total_sample_size := sum(sample_size), by="id"]
  split_dt[, all_age_rate := total_cases_dis/total_sample_size]
  split_dt[, ratio := mean / all_age_rate]
  split_dt[, mean := ratio * rate_dis]
  split_dt[, cases := mean * sample_size]

  ## Format for uploader
  split_dt[, group := 1]
  split_dt[, specificity := "age, sex"]
  split_dt[, group_review := 1]
  split_dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
  split_dt[, (blank_vars) := NA]

  ## Don't age split points that went over 1 if inc/prev/proportion
  if (drop_over_1) {
    ids_over1 <- split_dt[mean>1, unique(id)]
    split_dt <- split_dt[!(id %in% ids_over1)]
  }


  ## Figure out standard error
  z <- qnorm(0.975)
  split_dt[measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  split_dt[measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  split_dt[measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  split_dt[measure %in% c("cfr", "mtwith", "mtstandard", "proportion", "continuous"), standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  split_dt[, c("upper", "lower") := NA]
  split_dt[, note_modeler := paste0(note_modeler, " | age-split using Dismod age pattern")]

  ## Format to return
  names <- names(data)
  split_dt <- split_dt[, ..names]
  split_ids <- unique(split_dt$id)
  good_data <- copy(data[!(id %in% split_ids)])
  good_data[id %in% ids_small, note_modeler := paste0(note_modeler, " | not age split because cases < 1")]


  if (drop_over_1) {
    good_data[id %in% ids_over1, note_modeler := paste0(note_modeler, " | not age split because age split went over 1")]
  }

  dt <- rbind(good_data, split_dt)

  ## Get rid of extra columns
  dt <- dt[, ..orig_names]

  return(dt)

}


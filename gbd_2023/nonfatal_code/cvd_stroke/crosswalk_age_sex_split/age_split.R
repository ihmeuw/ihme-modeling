
##
## Author: USERNAME
## Date: DATE
##
## Purpose: Age-split data using age patterns from a Dismod model.
##
##
##          Updating DATE to give the option of larger age-bins, option of dropping instances where cases go below 1
##

os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
  h<-"FILEPATH"
} else {
  j<- "FILEPATH"
  h<-paste0("FILEPATH", Sys.info()[["user"]], "/")
  lib<-paste0("FILEPATH", Sys.info()[["user"]])
}

date<-gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, RMySQL)

central <- "FILEPATH"

# Central comp shared functions
for (func in paste0(central, list.files(central))) source(paste0(func))

# CVD helper functions
source("FILEPATH/data_tests.R")
source("FILEPATH/mapping_gbd_release_and_round.R")

#'
#' Age-split
#'
#' @details Age-split data points using the age pattern from a specified single-parameter Dismod model. Currently, only single-parameter Dismod models are used to age-split in order to ensure the measure of interest is not impacted by other measures and to avoid circularity. 
#'
#' @params df Data frame in epi database format. Requires columns age_start, age_end, mean, lower, upper, standard_error. Data should be already subset to the measure of interest.
#' @params model_id Modelable Entity ID (MEID) of the Dismod model you want to use for the age pattern. The best model version will be pulled if model_version_id=NULL.
#' @params model_version_id Model version ID of the single-parameter Dismod model you want to use for the age pattern. Must match the specified model_id and release_id. If NULL, the best model will be pulled in. Defaults to NULL.
#' @params release_id GBD release ID.
#' @params measure Measure of interest. Character string such as "prevalence", "incidence", or "continuous". Make sure it fits what's in DisMod (measures like 'emr' should be split as 'continuous' to match single-parameter dismod model measure).
#' @params split_size Threshold size of age bin to be split (difference between age_end and age_start). Defaults to 25.
#' @params drop_group_review Whether drop data where group_review==0. Defaults to TRUE.
#' @params drop_under_1_case Whether to not age-split rows where there would be fewer than one case in each split row. Defaults to TRUE.
#' @params drop_age_groups GBD age group IDs to drop from that data being split. Defaults to NULL.
#' @params gbd_age_groups Whether to use standard 5-year GBD age bins for data being split. Default to TRUE.
#' @params age_bin_size If not using standard 5-year GBD age bins for splitting data (gbd_age_groups=F), size of age bins to use. Defaults to NULL.
#' @params global_age_pattern Whether to use the global age pattern (TRUE) or super region age pattern (FALSE)
#'
#' @return age_split_data Data frame of age-split data.
#'


age_split <- function(df,
                      model_id,
                      model_version_id=NULL,
                      release_id,
                      measure,
                      split_size = 25,
                      drop_group_review = T,
                      drop_under_1_case = T,
                      drop_age_groups = NULL,
                      gbd_age_groups = T,
                      age_bin_size = NULL,
                      global_age_pattern
) {
  
  if (!(measure%in%c("prevalence", "incidence", "continuous", "proportion"))) stop("Single parameter models can only be prevalence, incidence, continuous, or proportion")
  
  if(length(unique(df$measure))>1){
    stop("There should only be one unique measure in the data being split")
  }
  
  if(unique(df$measure)!=measure){
    stop("Measure of interest does not match the measure in the data being split")
  }
  
  if (!missing(model_version_id)) {
    use_mvid <- T
    message("Model version ID is inputted so it will be used instead of pulling in the best model version. This should be a single parameter model version.")
  } else {
    use_mvid <- F
    message(paste0("The best model version from ME ID ", model_id, " will be used to age-split. This should be a single parameter model verion."))
  }
  
  if (!gbd_age_groups & (missing(age_bin_size) | is.null(age_bin_size))) {
    stop("Provide age bin sizes or toggle gbd_age_groups=T")
  }
  
  ## If using GBD age groups you'll split into 5-year age groups
  age_bin_size <- ifelse(gbd_age_groups, 5, age_bin_size)
  if (!(age_bin_size%%5==0)) stop("Age bin size must be divisible by 5")
  
  # save original column names
  orig_names <- names(df)
  
  # pull in age metadata
  mapping <- map_release_and_round(release=release_id, type='epi')
  ages <- get_age_metadata(release_id = release_id)
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  ages[age_start >= 1, age_end := age_end - 1]
  under_5_ids <- ages[age_start<5&age_end<5, age_group_id]
  
  # pull in measure metadata
  db_con <- read.csv("FILEPATH/database_info.csv",
                     stringsAsFactors = FALSE)
  con <- dbConnect(dbDriver("MySQL"),
                   username = db_con$user,
                   password = db_con$password,
                   host = "ADDRESS")
  measures <- dbGetQuery(con, paste0('select measure_id, measure, measure_name, measure_name_short
                                     from shared.measure'))
  dbDisconnect(con)
  setnames(measures, "measure", "measure_abbr")
  measures <- as.data.table(measures)
  measure_id <- measures[measure_abbr == measure, measure_id]
  
  # data with age split points that went over 1 should not be kept if measure is inc/prev/proportion
  drop_over_1 <- ifelse(measure %in% c("proportion", "prevalence", "incidence"), TRUE, FALSE)
  
  ################ 1. FORMAT DATA ####################
  
  message("Formatting data.")
  
  ## Format as data.table, get rid of group review and outliers, etc.
  data <- as.data.table(df)
  if (drop_group_review) data <- data[is.na(group_review) | !(group_review==0),]
  data <- data[is_outlier == 0,]
  
  ## Sex ID and year ID variables
  data[, sex_id := ifelse(sex=="Male", 1, ifelse(sex=="Female", 2, 3))]
  data[, year_id := round((year_start + year_end)/2, 0)]
  
  if (3 %in% unique(data$sex_id)) stop("Sex-split before age-splitting.")
  
  ## Coerce these columns to numeric if they aren't already
  num_cols <- c("mean", "lower", "upper", "standard_error", "cases", "sample_size", "age_start", "age_end",
                "year_start", "year_end", "group_review", "group", "is_outlier", "recall_type_value", "uncertainty_type_value")
  lapply(X = num_cols, FUN = check_class, df=data, class="numeric", coerce=T)
  
  if('specificity'%in%names(data)){
    data[, specificity := as.character(specificity)]
  } else {
    data[, specificity := as.character(NA)]
  }
  
  ## Make ID variable for splitting. One per row.
  data[, id := .GRP, by=names(data)]
  
  ## If SR-level age pattern is being used and there is not already a column for SR ID, add it
  if (!global_age_pattern&!("super_region_id" %in% names(data))){
    locs <- get_location_metadata(location_set_id = 35, release_id = release_id)
    data <- merge(data, locs[, c("location_id", "super_region_id")], by="location_id")
  }
  
  
  ################ 2. CREATE NEW AGE ROWS ####################
  
  message("Creating new age rows.")
  
  ## Pull out data you need to split, 'to_split'
  data[age_end > 99, age_end := 99]
  to_split <- copy(data[age_end - age_start > split_size])
  
  
  ## Fill in mean, cases, sample size if not inputted
  to_split[is.na(mean), mean:=cases/sample_size]
  to_split[is.na(cases) & !(is.na(sample_size)), cases := mean * sample_size]
  to_split[is.na(sample_size) & !(is.na(cases)), sample_size := cases / mean]
  
  ## Fill in standard error when not inputted, based on uploader formulas
  z <- qnorm(0.975)
  to_split[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/(2*z)]
  to_split[is.na(standard_error) & measure %in% c("incidence", "continuous") & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  to_split[is.na(standard_error) & measure %in% c("incidence", "continuous") & cases >= 5, standard_error := sqrt(mean/sample_size)]
  to_split[is.na(standard_error) & measure %in% c("prevalence", "proportion"), standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  
  ## Fill in cases from sample size
  to_split[is.na(cases) & is.na(sample_size) & measure %in% c("prevalence", "proportion"), sample_size := (mean*(1-mean)/standard_error^2)]
  to_split[is.na(cases) & is.na(sample_size) & measure %in% c("incidence", "continuous"), sample_size := mean/standard_error^2]
  to_split[is.na(cases), cases := mean * sample_size]
  
  ## Fill in upper, lower
  to_split[!is.na(mean) & !is.na(standard_error) & is.na(lower), lower := mean - (z * standard_error)]
  to_split[lower < 0, lower := 0]
  to_split[!is.na(mean) & !is.na(standard_error) & is.na(upper), upper := mean + (z * standard_error)]
  if(drop_over_1){
    to_split[upper > 1, upper := 1]
  }
  
  to_split[cases>sample_size, `:=` (cases=NA, sample_size=NA)]
  
  
  ## Round age groups based on age bin size
  to_split[, age_start := age_start - age_start %%age_bin_size]
  to_split[, age_end := age_end - age_end %%age_bin_size + (age_bin_size-1)]
  
  ## n_age is the number of rows the data point will be split into.
  to_split[, n_age := (age_end + 1 - age_start)/age_bin_size]
  to_split[, age_start_floor := age_start]
  
  if (drop_under_1_case) {
    ids_small <- to_split[cases/n_age <= 1, id]
    to_split <- to_split[cases/n_age > 1,] ##dropping rows where there would be fewer than one case in each split row
  }
  
  ## Expand for age. Create n_age number of points for each ID (row).
  expanded <- data.table(id = rep(to_split$id, to_split$n_age))
  split_dt <- merge(expanded, to_split, by="id", all=T)
  split_dt[, age_rep := 1:.N - 1, by=.(id)]
  
  ## Use age_rep to re-assign age_start and age_end
  split_dt[, age_start := age_start + age_rep * age_bin_size]
  split_dt[, age_end := age_start + (age_bin_size - 1)]
  
  # Add in age_group_id
  if (gbd_age_groups) {
    if('age_group_id'%in%names(split_dt)){
      split_dt[, age_group_id := NULL]
    }
    split_dt <- merge(split_dt, ages, by=c("age_start", "age_end"), all.x=T)
    split_dt[age_start == 0 & age_end == 4, age_group_id := 1]
    split_dt[age_start == 95 & age_end == 99, age_group_id := 235]
  } else {
    split_dt[, age_group_id := paste0(age_start, 0, age_end)]
  }
  
  if (!is.null(drop_age_groups)) split_dt <- split_dt[!(age_group_id %in% drop_age_groups),]
  
  ################ 3. GET POPULATION STRUCTURE ####################
  
  message("Getting population structure.")
  
  ## Pull the locations and years you need to estimate for
  if (global_age_pattern){
    pop_locs <- 1
  } else {
    pop_locs <- unique(split_dt$super_region_id)
  }
  pop_years <- unique(split_dt$year_id)
  
  if (gbd_age_groups) {
    
    pop_ages <- unique(split_dt$age_group_id)
    
    ## Pull population structure
    populations <- tryCatch({
      get_population(location_id = pop_locs, year_id = pop_years, release_id = release_id,
                     sex_id = c(1, 2), age_group_id = pop_ages)
    }, error = function(e) {
      previous_release <- mapping$previous_release_id
      print(paste0('No population data available for release_id ', release_id, ', switching to previous release_id (release_id ', previous_release, ') population'))
      return(get_population(location_id = pop_locs, year_id = pop_years, release_id = previous_release,
                            sex_id = c(1, 2), age_group_id = pop_ages))
    })
    
    if(1 %in% pop_ages & !1 %in% unique(populations$age_group_id)){
      ## If aggregate <5 data unavailable from get_population(), collapse populations under 5 to match 5-year age bins in data to split
      under_5 <- tryCatch({
        get_population(location_id = pop_locs, year_id = pop_years, release_id = release_id,
                       sex_id = c(1, 2), age_group_id = under_5_ids)
      }, error = function(e) {
        previous_release <- mapping$previous_release_id
        print(paste0('No population data available for release_id ', release_id, ', switching to previous release_id (release_id ', previous_release, ') population'))
        return(get_population(location_id = pop_locs, year_id = pop_years, release_id = previous_release,
                              sex_id = c(1, 2), age_group_id = under_5_ids))
      })
      under_5[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
      under_5 <- unique(under_5, by = c("location_id", "year_id", "sex_id"))
      under_5[, age_group_id := 1]
      
      populations <- rbind(populations, under_5)
    }
    
    
  } else {
    
    ## Pull population structure
    populations <- tryCatch({
      get_population(location_id = pop_locs, year_id = pop_years, release_id = release_id,
                     sex_id = c(1, 2), age_group_id = "all", single_year_age = T)
    }, error = function(e) {
      previous_release <- mapping$previous_release_id
      print(paste0('No population data available for release_id ', release_id, ', switching to previous release_id (release_id ', previous_release, ') population'))
      return(get_population(location_id = pop_locs, year_id = pop_years, release_id = previous_release,
                            sex_id = c(1, 2), age_group_id = "all", single_year_age = T))
    })
    
    ages <- get_ids("age_group")
    ages[age_group_id==28, age_group_name := '0']
    ages[age_group_id==238, age_group_name := '1']
    ages[age_group_id==235, age_group_name := '95']
    suppressWarnings(ages[, age_group_name := as.numeric(age_group_name)])
    ages <- ages[!is.na(age_group_name)]
    populations <- merge(populations, ages, by="age_group_id")
    
    ## Collapse to the correct ages
    populations[, c("age_group_id", "run_id") := NULL]
    populations[, bin := age_group_name - age_group_name%%age_bin_size]
    populations[, population := sum(population), by=c("location_id", "year_id", "sex_id", "bin")]
    populations <- unique(populations[, .(location_id, year_id, sex_id, bin, population)])
    populations[, age_start := bin]
    populations[, age_end := age_start + age_bin_size - 1]
    populations[, age_group_id := paste0(age_start, 0, age_end)]
    populations$bin <- NULL
    
  }
  
  
  ################ 4. GET AGE PATTERN FROM DISMOD ####################
  
  message("Getting age pattern from Dismod draws.")
  
  # Round year to nearest dismod year
  dismod_year_list <- rev(unlist(get_demographics('epi', release_id = release_id)['year_id']))
  find_closest <- function(value){
    return(dismod_year_list[which.min(abs(dismod_year_list - value))])
  }
  split_dt[, dismod_year := sapply(year_id, find_closest)]
  
  ages_round <- get_age_metadata(release_id = release_id)
  if (!gbd_age_groups) pop_ages <- ages_round$age_group_id
  
  ## Age pattern for the locations and years you need to estimate for
  if (use_mvid) {
    age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = model_id, version_id = model_version_id,
                             measure_id = measure_id, location_id = pop_locs, source = "epi",
                             sex = c(1, 2), release_id = release_id,
                             age_group_id = pop_ages, year_id = unique(split_dt$dismod_year))
    
    if(1 %in% pop_ages & !1 %in% unique(age_pattern$age_group_id)){
      ## If aggregate <5 data unavailable from get_draws(), add in individual standard GBD age groups from under 5
      under_5 <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = model_id, version_id = model_version_id,
                           measure_id = measure_id, location_id = pop_locs, source = "epi",
                           sex = c(1, 2), release_id = release_id,
                           age_group_id = under_5_ids, year_id = unique(split_dt$dismod_year))
      
      age_pattern <- rbind(age_pattern, under_5)
    }
    
  } else {
    age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = model_id,
                             measure_id = measure_id, location_id = pop_locs, source = "epi",
                             status = "best", sex = c(1, 2), release_id = release_id,
                             age_group_id = pop_ages, year_id = unique(split_dt$dismod_year))
    
    if(1 %in% pop_ages & !1 %in% unique(age_pattern$age_group_id)){
      ## If aggregate <5 data unavailable from get_draws(), add in individual standard GBD age groups from under 5
      under_5 <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = model_id,
                           measure_id = measure_id, location_id = pop_locs, source = "epi",
                           status = "best", sex = c(1, 2), release_id = release_id,
                           age_group_id = under_5_ids, year_id = unique(split_dt$dismod_year))
      
      age_pattern <- rbind(age_pattern, under_5)
    }
  }
  
  ## Get means from draws
  draws <- names(age_pattern)[grepl("draw", names(age_pattern))]
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis, year_id)]
  
  ## Calculate cases and sample of the Dismod pattern
  age_pattern[measure_id %in% c(5, 18), sample_size_dis := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[measure_id %in% c(6, 19), sample_size_dis := rate_dis/se_dismod^2]
  age_pattern[, cases_dis := sample_size_dis * rate_dis]
  age_pattern[is.nan(sample_size_dis), sample_size_dis := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0
  age_pattern[is.nan(cases_dis), cases_dis := 0]
  age_pattern <- age_pattern[cases_dis > 0 & sample_size_dis > 0]
  
  if (!gbd_age_groups) {
    ## Collapse if you don't need GBD age groups
    age_pattern <- merge(age_pattern, ages_round, by="age_group_id")
    age_pattern[, bin := age_group_years_start - age_group_years_start%%age_bin_size]
    age_pattern[, `:=` (cases_dis=sum(cases_dis), sample_size_dis=sum(sample_size_dis)), by=c("sex_id", "measure_id", "location_id", "year_id", "bin")]
    age_pattern <- unique(age_pattern[, .(cases_dis, sample_size_dis, sex_id, measure_id, location_id, year_id, bin)])
    age_pattern[, age_start := bin]
    age_pattern[, age_end := age_start + age_bin_size - 1]
    age_pattern[, rate_dis := cases_dis/sample_size_dis]
    age_pattern[measure_id %in% c(6, 19) & cases_dis < 5, se_dismod := ((5-rate_dis*sample_size_dis)/sample_size_dis+rate_dis*sample_size_dis*sqrt(5/sample_size_dis^2))/5]
    age_pattern[measure_id %in% c(6, 19) & cases_dis >= 5, se_dismod := sqrt(rate_dis/sample_size_dis)]
    age_pattern[measure_id %in% c(5, 18), se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_dis + z^2/(4*sample_size_dis^2))]
    age_pattern[, age_group_id := paste0(age_start,"0",age_end)]
    
  } else {
    ## If aggregate <5 data was unavailable from get_draws(), collapse under 5 to match 5-year age bins in data to split
    if(any(under_5_ids%in%unique(age_pattern$age_group_id))){
      
      # sum cases and sample sizes for under 5
      under_5 <- age_pattern[age_group_id%in%under_5_ids]
      under_5 <- under_5[, .(cases_dis=sum(cases_dis), sample_size_dis=sum(sample_size_dis)), by=c("sex_id", "measure_id", "location_id", "year_id")]
      under_5[, age_group_id := 1]
      # recalculate rate and se
      under_5[, rate_dis := cases_dis/sample_size_dis]
      under_5[measure_id %in% c(6, 19) & cases_dis < 5, se_dismod := ((5-rate_dis*sample_size_dis)/sample_size_dis+rate_dis*sample_size_dis*sqrt(5/sample_size_dis^2))/5]
      under_5[measure_id %in% c(6, 19) & cases_dis >= 5, se_dismod := sqrt(rate_dis/sample_size_dis)]
      under_5[measure_id %in% c(5, 18), se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_dis + z^2/(4*sample_size_dis^2))]
      
      age_pattern <- rbind(age_pattern[!(age_group_id %in% under_5_ids)], under_5)
      
    }
  }
  
  age_pattern <- age_pattern[ , .(age_group_id, year_id, sex_id, measure_id, cases_dis, sample_size_dis, rate_dis, se_dismod, location_id)]
  setnames(age_pattern, old="year_id", new="dismod_year")
  
  missing_ages <- unique(split_dt$age_group_id)[!unique(split_dt$age_group_id)%in%unique(age_pattern$age_group_id)]
  if(length(missing_ages)>0){
    message(paste0("The following age_group_ids do not have any cases according to the dismod age pattern and so these rows will be removed from the to-split data: ", 
                   paste0(missing_ages, collapse=', ')))
    split_dt <- split_dt[!age_group_id%in%missing_ages]
  }
  
  ################ 5. USE AGE STRUCTURE TO CREATE NEW AGE-SPECIFIC POINTS ####################
  
  message("Using Dismod pattern to age-split data points.")
  
  ## Merge in on age and population pattern
  row_check <- nrow(split_dt)
  if (global_age_pattern){
    split_dt <- merge(split_dt, populations[, .(sex_id, age_group_id, year_id, population)], by=c("sex_id", "age_group_id", "year_id"))
    split_dt <- merge(split_dt, age_pattern[, .(sex_id, age_group_id, dismod_year, cases_dis, sample_size_dis, rate_dis, se_dismod)], by=c("sex_id", "age_group_id", "dismod_year"))
  } else{
    # set location id to SR id so it can be merged back on data
    setnames(populations, old="location_id", new="super_region_id")
    setnames(age_pattern, old="location_id", new="super_region_id")
    
    split_dt <- merge(split_dt, populations[, .(sex_id, age_group_id, year_id, super_region_id, population)], by=c("sex_id", "age_group_id", "super_region_id", "year_id"))
    split_dt <- merge(split_dt, age_pattern[, .(sex_id, age_group_id, dismod_year, cases_dis, sample_size_dis, rate_dis, se_dismod, super_region_id)], by=c("sex_id", "age_group_id", "super_region_id", "dismod_year"))
  }
  split_dt$dismod_year= NULL
  message(paste0(row_check-nrow(split_dt), " out of ", row_check, " rows of the to-split data lost when merging in population and dismod age-pattern"))
  
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
  
  ## standard error
  z <- qnorm(0.975)
  split_dt[measure %in% c("incidence", "continuous") & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  split_dt[measure %in% c("incidence", "continuous") & cases >= 5, standard_error := sqrt(mean/sample_size)]
  split_dt[measure %in% c("prevalence", "proportion"), standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  split_dt[, c("upper", "lower") := NA]
  split_dt[, note_modeler := paste0(note_modeler, " | age-split using Dismod age pattern")]
  
  ## Format to return
  names <- names(data)
  split_dt <- split_dt[, ..names]
  split_ids <- unique(split_dt$id)
  
  good_data <- copy(data[!(id %in% split_ids)])
  
  if (drop_under_1_case) good_data[id %in% ids_small, note_modeler := paste0(note_modeler, " | not age split because cases < 1")]
  
  if (drop_over_1) {
    good_data[id %in% ids_over1, note_modeler := paste0(note_modeler, " | not age split because age split went over 1")]
    if(length(ids_over1)>0){
      message(paste0("Data points from the following NIDs were not age split because the age split went over 1: ", paste0(unique(good_data[id %in% ids_over1, nid]), collapse=', ')))
    }
  }
  
  dt <- rbind(good_data, split_dt)
  
  ## Get rid of extra columns
  dt <- dt[, ..orig_names]
  
  return(dt)
  
}


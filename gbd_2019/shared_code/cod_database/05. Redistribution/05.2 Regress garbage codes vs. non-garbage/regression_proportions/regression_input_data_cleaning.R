library("ihme", lib.loc = "FILEPATH")

ihme::source_functions(get_cause_metadata = T, get_covariate_estimates = T, get_population = T)
source("FILEPATH/get_age_metadata.R")  # temp fix, not in source_functions atm


# Basic getters -----------------------------------------------------------

# Print out with newlines at end, salute to java's print_ln()
cat_ln <- function(...) {
  cat(...); cat("\n")
}

# Grab the most recent regression launch set from the regression launch set id table
get_regression_launch_set <- function(id = NULL) {
  table <- fread(paste0(REGDIR, "FILEPATH/regression_launch_set_id_table.csv"))
  if (is.null(id)) return(table[nrow(table), ])
  else return(table[regression_launch_set_id == id, ])
}

# Print regression launch set, for text file output
print_launch_set <- function(regression_launch_set) {
  cat("REGRESSION LAUNCH SET:\n")
  for (col in names(regression_launch_set)) {
    cat(paste0("    ", col, " : ", regression_launch_set[, get(col)], "\n"))
  }
}

# Returns TRUE iff the regression formula has random effects
# Uses '|' in the formula to determine this.
has_random_effects <- function(formula) {
  return(grepl("\\|", formula))
}

# Returns the name of the year_column used
# options: year_id (year-specific regression),
# year_window (cancers use this)
get_year_setting <- function(formula) {
  garbage_col_name <- gsub("(^.*~ ?logit_)|( ?\\+.*)", "", formula)
  if (grepl("year_window", garbage_col_name)) {
    return("year_window")
  } else {
    return("year_id")
  }
}


# Reading in/basic col set up ---------------------------------------------

logit <- function(x) {
  log(x / (1 - x))
}


invlogit <- function(x) {
  exp(x) / (1 + exp(x))
}


bind_column_by_offsets <- function(dt, col) {
  dt <- copy(dt)
  dt[get(col) < LOWER_OFFSET, eval(col) := LOWER_OFFSET]
  dt[get(col) > UPPER_OFFSET, eval(col) := UPPER_OFFSET]
  return(dt)
}

attach_stars <- function(dt) {
  # GBD 2017 stars version used for regressions
  stars <- fread(paste0(j, "FILEPATH/stars_by_iso3_time_window.csv"))
  stars <- stars[, c("location_id", "stars", "time_window")]
  
  # not real proud of readability here but doing fancy string manipulation to convert a time window
  # in the form of "1990_1994" to the number of years covered
  stars[, year_span := mapply(function(x) { eval(parse(text=paste0("-(", gsub("_", "-", x), ")"))) }, time_window)]
  stars <- stars[year_span == max(year_span)] # grab star rating for largest range of available years
  dt <- merge(dt, stars[, c("location_id", "stars")], by.x = "country", by.y = "location_id", all.x = TRUE)
  return(dt)
}

limit_stars <- function(dt, stars_lower_limit) {
  dt <- attach_stars(dt)
  
  cat_ln("Limiting to high-quality-VR to >=", stars_lower_limit, "stars")
  dt <- dt[stars >= stars_lower_limit]
}

#' Sets the lower bound for the logit transformation. Keeps
#' the defaults lower bound for stroke and diabetes.
#' For cancers, replaces it with a lower bound of the 10th
#' percentile of prop_target_window_scaled after all the 0s are dropped
#' 
#' @mutate LOWER_OFFSET global variable
set_lower_bound <- function(launch_set, input_dt) {
  year_setting = get_year_setting(launch_set$formula)
  
  if (year_setting == "year_id") {
    cat_ln(paste0("Keeping lower offset for logit transform as is: ", LOWER_OFFSET))
  } else if (year_setting == "year_window") {
    LOWER_OFFSET <<- quantile(input_dt[prop_target_year_window_scaled != 0]$prop_target_year_window_scaled,
                              na.rm = T, probs = .1)
    cat_ln(paste0("Setting lower offset for logit transform to ", LOWER_OFFSET, " (prop target 10th percentile)."))
  } else {
    stop(paste0("Unknown year setting: ", year_setting), ". Cannot set lower bound.")
  }
  
}

# Separating this from prep_input_dt() so
# that for region/super aggregation I can 
# use the full input data, regardless of stars
read_input_dt <- function(file) {
  dt <- fread(file)
  dt$age <- as.factor(dt$age)
  return(dt)
}

prep_input_dt <- function(indir, data_id) {
  input_dt <- read_input_dt(paste0(indir, "/data/_input_data_id_", data_id, ".csv"))
  input_dt[, prop_target_scaled := prop_target / sum(prop_target), by=c("country", "year_id", "age_group_id", "sex_id")]
  input_dt[, prop_target_all_years_scaled := prop_target_all_years / sum(prop_target_all_years), by=c("country", "age_group_id", "sex_id")]
  input_dt[, prop_target_year_window_scaled := prop_target_year_window / sum(prop_target_year_window), by=c("country", "age_group_id", "sex_id")]
  

  set_lower_bound(launch_set, input_dt = input_dt)
  
  for (col in grep("prop_", names(input_dt), value = T)) {
    input_dt <- bind_column_by_offsets(input_dt, col)
    input_dt[, paste0("logit_", col) := logit(get(col))]
  }
  
  # restrict to maximum value of pct garbage
  #cat_ln(paste0("MAX_PROP_GARBAGE: ", MAX_PROP_GARBAGE, " (but currently unused)"))
  #input_dt[prop_garbage <= MAX_PROP_GARBAGE, ] # FIXME: THIS IS WRONG BUT WAS USED
  
  # tack on region and super region
  locs <- loc_data[, c("location_id", "location_name", "region_name", "super_region_name")]
  input_dt <- merge(input_dt, locs, by.x = "country", by.y = "location_id")
  
  return(input_dt)
}

# Cross a dt with another
cross <- function(data1, data2) {
  df1_prep <- copy(as.data.table(data1))
  df2_prep <- copy(as.data.table(data2))
  
  df1_prep[, merge_var := 1]
  df2_prep[, merge_var := 1]
  
  crossed <- merge(df1_prep, df2_prep, by = "merge_var", allow.cartesian=TRUE)
  crossed[, merge_var := NULL]
  return(crossed[])
}

#' @param year_setting is how to handle years. For cancers we're doing age_windows. year_id for year-specific aggregation
#' `Not currently supporting all years``
aggregate_prop_garbage <- function(file, year_setting = get_year_setting(launch_set$formula)) {
  input_dt <- read_input_dt(file)
  input_deaths <- input_dt[, c("country", "region", "super_region", "global", "year_id", "age_group_id", "age", "sex_id", 
                               "cause_id", "deaths_target", "deaths_garbage")]
  
  base_cols <- c("country", "region", "super_region", "global", "year_id", "age", "sex_id", 
                 "cause_id")
  # collapse by age group
  input_deaths <- input_deaths[, list(deaths_target = sum(deaths_target), 
                                      deaths_garbage = sum(deaths_garbage)),
                               by = base_cols]
  
  # need to deal with year-specificity as it changes how aggregation works
  prediction <- gsub(" ?~.*", "", launch_set$formula)
  if (grepl("all_years", prediction)) { # not year specific
    # collapse on year
    base_cols <- base_cols[!base_cols %in% "year_id"]
    input_deaths <- input_deaths[, list(deaths_target = sum(deaths_target), 
                                        deaths_garbage = sum(deaths_garbage)),
                                 by = base_cols]
  } else if (grepl("year_window", prediction)) {
    # create and collapse on time window
    base_cols <- base_cols[!base_cols %in% "year_id"]
    base_cols <- c(base_cols, "year_window")
    
    time_window_cut <- 2004
    start <- min(input_deaths$year_id)
    end <- max(input_deaths$year_id)
    
    input_deaths[, year_window := ifelse(year_id <= time_window_cut, 
                                         paste0(start, " - ", time_window_cut),
                                         paste0(time_window_cut + 1, " - ", end))]
    
    input_deaths <- input_deaths[, list(deaths_target = sum(deaths_target), 
                                        deaths_garbage = sum(deaths_garbage)),
                                 by = base_cols]
  } # else IS year-specific and we can move forward w/o special case
  
  region_cols <- base_cols[!base_cols %in% "country"]
  super_cols <- region_cols[!region_cols %in% "region"]
  global_cols <- super_cols[!super_cols %in% "super_region"]
  
  region_deaths <- input_deaths[, list(deaths_target_region = sum(deaths_target),
                                       deaths_garbage_region = sum(deaths_garbage)),
                                by = region_cols]
  super_deaths <- input_deaths[, list(deaths_target_super = sum(deaths_target),
                                      deaths_garbage_super = sum(deaths_garbage)),
                               by = super_cols]
  global_deaths <- input_deaths[, list(deaths_target_global = sum(deaths_target),
                                       deaths_garbage_global = sum(deaths_garbage)),
                                by = global_cols]
  
  input_deaths <- merge(input_deaths, region_deaths, by = region_cols)
  input_deaths <- merge(input_deaths, super_deaths, by = super_cols)
  input_deaths <- merge(input_deaths, global_deaths, by = global_cols)
  
  return(input_deaths)
}


prep_square_dt <- function(indir, data_id, year_setting = "year_window") {
  square_dt <- fread(paste0(indir, "/data/_square_data_id_", data_id, ".csv"))
  square_dt$age <- as.factor(square_dt$age)
  
  # for ALL we deal with region/super aggregation AFTER regression, testing rn 2/15/19
  if (TRUE) { # (year_setting != "year_window") {
    for (col in grep("prop_", names(square_dt), value = T)) {
      square_dt <- bind_column_by_offsets(square_dt, col)
      square_dt[, paste0("logit_", col) := logit(get(col))]
    }
    
    # tack on region and super region
    locs <- loc_data[, c("location_id", "location_name", "region_name", "super_region_name")]
    square_dt <- merge(square_dt, locs, by.x = "country", by.y = "location_id")
    return(square_dt)
  }
  
  # create and tack on rows for region, super region, and global!
  # Since # of garbage deaths are shared across causes,
  # we only need the unique value for GC deaths while we
  # have to sum for target deaths (collapse by cause)
  input_dt <- aggregate_prop_garbage(paste0(indir, "/data/_input_data_id_", data_id, ".csv"))
  global_rows <- input_dt[, list(deaths_target_global = sum(deaths_target_global), deaths_garbage_global = unique(deaths_garbage_global)),
                          by = c("age", "sex_id", eval(year_setting), "global")]
  super_rows <- input_dt[, list(deaths_target_super = sum(deaths_target_super), deaths_garbage_super = unique(deaths_garbage_super)),
                         by = c("age", "sex_id", eval(year_setting), "super_region")]
  region_rows <- input_dt[, list(deaths_target_region = sum(deaths_target_region), deaths_garbage_region = unique(deaths_garbage_region)),
                          by = c("age", "sex_id", eval(year_setting), "region")]
  
  # REG EX: extract column name for prop garbage based on what it is in formula
  # this grabs first word after ~, drops all else
  garbage_col_name <- gsub("(^.*~ ?logit_)|( ?\\+.*)", "", launch_set$formula)
  
  global_rows[, eval(garbage_col_name) := deaths_garbage_global / (deaths_garbage_global + deaths_target_global)]
  global_rows[, location_id := global]
  super_rows[, eval(garbage_col_name) := deaths_garbage_super / (deaths_garbage_super + deaths_target_super)]
  super_rows[, location_id := super_region]
  region_rows[, eval(garbage_col_name) := deaths_garbage_region / (deaths_garbage_region + deaths_target_region)]
  region_rows[, location_id := region]
  
  aggregate_rows <- rbind(global_rows, super_rows, region_rows, fill = T)
  aggregate_rows <- aggregate_rows[, c("age", "sex_id", year_setting, "location_id", garbage_col_name), with = F]
  
  # create square df for regions, supers, and global to merge prop garbage onto
  age <- unique(square_dt$age)
  sex_id <- unique(square_dt$sex_id)
  years <- unique(square_dt[, get(year_setting)])
  location_id <- loc_data[level < 3, c("location_id", "super_region_id")]
  cause_id <- unique(square_dt$cause_id)
  
  square_aggregate_df <- cross(as.data.table(age), as.data.table(sex_id))
  square_aggregate_df <- cross(square_aggregate_df, as.data.table(years))
  square_aggregate_df <- cross(square_aggregate_df, as.data.table(location_id))
  square_aggregate_df <- setNames(square_aggregate_df, c("age", "sex_id", year_setting, "location_id", "super_region"))
  
  # merge on where we actually have data. If we have no data, replace with super region or global
  aggregate_rows <- merge(aggregate_rows, square_aggregate_df, all = T)
  
  fill_prop_garbage_na <- function(age_group, sex, year, super_region_id) {
    # this is deffos not a great idea but we're doing it
    super_region_prop <- aggregate_rows[age == age_group & sex_id == sex & year_window == year & location_id == super_region_id, get(garbage_col_name)]
    if (is.na(super_region_prop) & super_region_id != 1) { # insurance we cant get caught in infinite loop if any global val is NA
      return(fill_prop_garbage_na(age_group, sex, year, 1)) # return GLOBAL VALUE
    } else {
      return(super_region_prop)
    }
  }
  
  aggregate_rows[is.na(get(garbage_col_name)), prop_garbage_year_window := mapply(fill_prop_garbage_na, age, sex_id, get(year_setting), super_region)]
  aggregate_rows <- as.data.table(cross(aggregate_rows, as.data.table(cause_id)))
  aggregate_rows[, cause_id := as.character(cause_id)]
  aggregate_rows[, model_group := paste0(cause_id, "_", sex_id)]
  
  # MERGE ON LOC INFO FOR AGG ROWS, APPEND TO SQUARE DT
  locs <- loc_data[, c("location_id", "region_id")]
  locs <- setNames(locs, c("location_id", "region")) # yes this is hacky, just trying to fit into older system
  aggregate_rows <- merge(aggregate_rows, locs, by = "location_id")
  aggregate_rows <- setNames(aggregate_rows, c("country", names(aggregate_rows)[2:length(aggregate_rows)])) # sorry :/
  
  square_dt <- rbind(square_dt, aggregate_rows, fill = T)
  square_dt[, global := 1]
  
  for (col in grep("prop_", names(square_dt), value = T)) {
    square_dt <- bind_column_by_offsets(square_dt, col)
    square_dt[, paste0("logit_", col) := logit(get(col))]
  }
  
  # tack on region and super region
  locs <- loc_data[, c("location_id", "location_name", "region_name", "super_region_name")]
  square_dt <- merge(square_dt, locs, by.x = "country", by.y = "location_id")
  
  
  return(square_dt)
}


# Cause restrictions application ------------------------------------------


#' Given a list of cause_ids and a GBD round id to grab the cause
#' heirarchy from, downloads the age and sex restrictions for each cause.
#' Depends on shared functions `get_cause_metadata()` and `get_age_metadata()`
#' 
#' @param cause_ids list of cause_ids to look up cause restrictions on
#' @param gdb_round_id round id to grab proper heirarchy. Defaults to 6 (GBD 2019)
#' 
#' @return data.table of age and sex restrictions with a column for each cause_id passed in
get_cause_restrictions <- function(cause_ids, gbd_round_id = 6) {
  gbd_estimation_set <- 4
  most_detailed_age_group_set <- 12
  
  cause_metadata <- get_cause_metadata(cause_set_id = gbd_estimation_set, 
                                       gbd_round_id = gbd_round_id)
  age_metadata <- get_age_metadata(age_group_set_id = most_detailed_age_group_set,
                                   gbd_round_id = gbd_round_id)
  
  
  cause_metadata <- cause_metadata[cause_id %in% cause_ids]
  if (nrow(cause_metadata[most_detailed == 0] > 0)) {
    warning(paste0("List contains causes that are NOT the most detailed level: ",
                   paste(unique(cause_metadata[most_detailed == 0]$cause_name), collapse = ", "),
                   ". Are you sure this is correct?"))
  }
  
  cause_metadata <- cause_metadata[, c("cause_set_id", "cause_set_name", "cause_id", "acause", "cause_name",
                                       "male", "female", "yll_age_start", "yll_age_end")]
  age_metadata <- age_metadata[, -c("age_group_weight_value", "age_group_years_end")]
  
  # Merge on AGE GROUP start and end as  what we have here is the first age in the age group start/ends
  # slightly dangerous merge on int vs double
  restrictions_dt <- merge(cause_metadata, age_metadata, by.x = "yll_age_start", by.y = "age_group_years_start")
  setnames(restrictions_dt, "age_group_id", "age_group_id_start")
  
  restrictions_dt <- merge(restrictions_dt, age_metadata, by.x = "yll_age_end", by.y = "age_group_years_start")
  setnames(restrictions_dt, "age_group_id", "age_group_id_end")
  
  restrictions_dt <- restrictions_dt[, -c("yll_age_start", "yll_age_end")]
  
  return(restrictions_dt)
}

apply_cause_restrictions <- function(input_dt, square_dt, shared_package_id = NA) {
  # CUSTOM RESTRICTIONS FOR SITE UNSPECIFIED C
  if (!is.na(shared_package_id) && shared_package_id == 9) {
    cat_ln("  Special handling for C80 (site unspecified) restrictions")
    restrictions <- fread(paste0(REGDIR, "FILEPATH/C80_site_unspecified_cancer_GBD_2019_restrictions.csv"))
  } else {
    restrictions <- get_cause_restrictions(unique(input_dt$cause_id))
  }
  
  restrictions <- restrictions[, c("cause_id", "male", "female", "age_group_id_start", "age_group_id_end")]
  
  # seeing NAs on sex restrictions in new GBD 2019 causes, assumping no sex restrictions
  restrictions[, male := ifelse(is.na(male), 1, male)]
  restrictions[, female := ifelse(is.na(female), 1, female)]
  
  input_dt <- merge(input_dt, restrictions, by = "cause_id")
  square_dt <- merge(square_dt, restrictions, by = "cause_id")
  
  # drop restricted data from input, square data set and add to forced_predictions
  input_dt <- input_dt[(sex_id == 1 & male != 0) | (sex_id == 2 & female != 0)]
  input_dt <- input_dt[age_group_id >= age_group_id_start & age_group_id <= age_group_id_end]
  
  # saying here that where sex restricted, forced to 0. For ages, we have to hack because
  # we're predicting on larger age groups:
  #   - if the cancer is restricted to end in under 15s and the age col is not 0 -> set to 0
  #   - if cancer restricted to start after age 15 and age col is 0 -> set to 0
  forced_predictions <- square_dt[(sex_id == 1 & male == 0) | (sex_id == 2 & female == 0) |
                                    (age != 0 & age_group_id_end < 8) |
                                    (age == 0 & age_group_id_start >= 8)]
  
  forced_predictions[, prop_target := 0]
  forced_predictions[, prop_target_scaled := 0]
  
  square_dt <- square_dt[(sex_id == 1 & male != 0) | (sex_id == 2 & female != 0)]
  square_dt <- square_dt[!(age != 0 & age_group_id_end < 8) & !(age == 0 & age_group_id_start >= 8)]
  
  # clean up
  input_dt <- input_dt[, -c("male", "female", "age_group_id_start", "age_group_id_end")]
  square_dt <- square_dt[, -c("male", "female", "age_group_id_start", "age_group_id_end")]
  forced_predictions <- forced_predictions[, -c("male", "female", "age_group_id_start", "age_group_id_end")]
  
  return(list("input_dt" = input_dt, "square_dt" = square_dt, "forced_predictions" = forced_predictions))
}


# Input, square cleaning and manipulation ---------------------------------


#' Function that takes the regression version, the input_dt and the square_dt and 
#' cleans/modifies/sets up as necessary. Returns a list containing "input_dt", "square_dt"
#' and "forced_predictions"
clean_reg_input_data <- function(regression_launch_set_id, input_dt, square_dt) {
  launch_set <- get_regression_launch_set(regression_launch_set_id)
  forced_predictions <- NULL

  if (launch_set$shared_package_id == 15) { # Stroke, unspecified

    cat_ln("Restricting input and output to 1990 and on. Dropping", nrow(input_dt[year_id < 1990]), "rows.")
    input_dt <- input_dt[year_id >= 1990]
    square_dt <- square_dt[year_id >= 1990]
    
    # mark locs w/ 1, 2, 3 stars to be replaced by the region
    cat_ln("Marking locations with 1 - 3 stars for replacement by region after running")
    square_dt <- attach_stars(square_dt)
    square_dt[stars <= 3, replaced_by := "region"]
    square_dt[, stars := NULL]
    
  } else if (launch_set$shared_package_id == 2614) { # diabetes
    # remove under 15 because it should all go to type 1
    input_dt <- input_dt[age != 0, ]
    cat_ln("Dropping age under 15")
    
    # remove Russia (it is disaggregated stuff) (GBD 2017 decision)
    input_dt <- input_dt[country != 62, ]
    cat_ln("Dropping Russia")
    
    # mark locs w/ 1, 2, 3 stars to be replaced by the region
    cat_ln("Marking locations with 1 - 3 stars for replacement by region after running")
    square_dt <- attach_stars(square_dt)
    square_dt[stars <= 3, replaced_by := "region"]
    square_dt[, stars := NULL]
    
    # Set all diabetes deaths under 15 to type 1
    forced_predictions <- square_dt[age == 0, ]
    square_dt <- square_dt[age != 0, ]
    forced_predictions[age == 0 & cause_id == 975, prop_target := 1]
    forced_predictions[age == 0 & cause_id == 975, prop_target_scaled := 1]
    forced_predictions[age == 0 & cause_id == 976, prop_target := 0]
    forced_predictions[age == 0 & cause_id == 976, prop_target_scaled := 0]

    # tack on regression formula
    forced_predictions[, formula := launch_set$formula]
    forced_predictions[, `:=`(location_id = country, level = 3, level_name = "Country")]

    # these data tables still think the level '0' exists unless we re-level them
    input_dt$age <- factor(input_dt$age)
    square_dt$age <- factor(square_dt$age)
  } else if(launch_set$shared_package_id == 4) { # uterus cancer
    # dropping where essentially we have no data: target deaths and garbage deaths are
    # both 0. Doing this in hopes model will converge
    rows_dropped <- nrow(input_dt[(prop_target_year_window == LOWER_OFFSET & prop_garbage_year_window == LOWER_OFFSET), ])
    
    cat_ln("Droping data points where both prop target and garbage are 0 (", rows_dropped, ")", sep = "")
    input_dt <- input_dt[!(prop_target_year_window == LOWER_OFFSET & prop_garbage_year_window == LOWER_OFFSET), ]
    
    # KEEP IN MIND WE DONT REALLY CARE ABOUT MALE RESULT,
    # as males can't get uterus cancer due to lack of a uterus.
    # These deaths are miscoded are redistrbuted elsewhere
    input_dt <- input_dt[sex_id != 1]
    cat_ln("Dropping all male data")
    
    # set all male props to other cancer
    forced_predictions <- square_dt[sex_id == 1, ]
    square_dt <- square_dt[sex_id != 1, ]
    forced_predictions[sex_id == 1 & cause_id == 489, `:=`(prop_target = 1, prop_target_scaled = 1)]
    forced_predictions[sex_id == 1 & cause_id %in% c(432, 435), `:=`(prop_target = 0, prop_target_scaled = 0)]
    
    # remove under age 15 as it should all go to other cancer
    input_dt <- input_dt[age != 0, ]
    cat_ln("Dropping age under 15")
    
    forced_predictions <- rbind(forced_predictions, square_dt[age == 0, ], fill = T)
    square_dt <- square_dt[age != 0, ]
    forced_predictions[age == 0 & cause_id == 489, prop_target := 1]
    forced_predictions[age == 0 & cause_id == 489, prop_target_scaled := 1]
    forced_predictions[age == 0 & cause_id %in% c(432, 435), prop_target := 0]
    forced_predictions[age == 0 & cause_id %in% c(432, 435), prop_target_scaled := 0]
    
    # tack on regression formula
    forced_predictions[, formula := launch_set$formula]
    #forced_predictions[, `:=`(location_id = country, level = 3, level_name = "Country")]
    
    # > 15 we only want two target groups: uterine and cervix
    cat_ln("Dropping other cancer data from input and square data")
    input_dt <- input_dt[cause_id != 489, ]
    square_dt <- square_dt[cause_id != 489, ]
    
    # these data tables still think the level '0' exists unless we re-level them
    input_dt$age <- factor(input_dt$age)
    square_dt$age <- factor(square_dt$age)
  } else if(launch_set$shared_package_id == 2) { # GI cancer
    # dropping where essentially we have no data: target deaths and garbage deaths are
    # both 0. Doing this in hopes model will converge
    rows_dropped <- nrow(input_dt[(prop_target_year_window == LOWER_OFFSET & prop_garbage_year_window == LOWER_OFFSET), ])
    
    cat_ln("Droping data points where both prop target and garbage are 0 (", rows_dropped, ")", sep = "")
    input_dt <- input_dt[!(prop_target_year_window == LOWER_OFFSET & prop_garbage_year_window == LOWER_OFFSET), ]
    
    # drop all deaths under age 15 if they aren't coded to liver cancer or other
    input_dt <- input_dt[!(age == 0 & !(cause_id %in% c(417, 489)))]
    cat_ln("Dropping all data under age 15 that isn't coded to liver cancer or other cancer")
    
    # set all male props to other cancer
    forced_predictions <- square_dt[age == 0 & !(cause_id %in% c(417, 489)), ]
    square_dt <- square_dt[!(age == 0 & !(cause_id %in% c(417, 489))), ]
    forced_predictions[age == 0 & !(cause_id %in% c(417, 489)), `:=`(prop_target = 0, prop_target_scaled = 0)]
  } else if (launch_set$shared_package_id == 9) {  # C80, site unspecified cancer
    cat_ln("Dropping cause id 625 'Other endocrine, metabolic, blood, and immune disorders' that's somehow snuck into data set.")
    input_dt <- input_dt[cause_id != 625]
    square_dt <- square_dt[cause_id != 625]
    
    # both 0. Doing this in hopes model will converge
    rows_dropped <- nrow(input_dt[(prop_target_year_window == LOWER_OFFSET & prop_garbage_year_window == LOWER_OFFSET), ])
    
    cat_ln("Droping data points where both prop target and garbage are 0 (", rows_dropped, ")", sep = "")
    input_dt <- input_dt[!(prop_target_year_window == LOWER_OFFSET & prop_garbage_year_window == LOWER_OFFSET), ]
    
    # Apply sex and age restrictions by cause
    cat_ln("Applying sex and age restrictions by cause")
    results <- apply_cause_restrictions(input_dt, square_dt, shared_package_id = launch_set$shared_package_id)
    
    input_dt <- results[["input_dt"]]
    square_dt <- results[["square_dt"]]
    forced_predictions <- results[["forced_predictions"]]
    
    # THIS IS AN ISSUE. for some reason these are NA for a few age 0s,
    # prob bc there's no deaths at all in a Y/A/S/L
    target_col_name <- gsub(" *~.*$", "", launch_set$formula)
    
    cat_ln(paste0("Dropping ", nrow(input_dt[is.na(get(target_col_name))]), " rows where ", target_col_name,
                  " is NA"))
    input_dt <- input_dt[!is.na(prop_target_year_window_scaled)]
  }
  
  return(list("input_dt" = input_dt, "square_dt" = square_dt, "forced_predictions" = forced_predictions))
}



# HACKS FOR SPEED ---------------------------------------------------------

#' Downloads (or reads from disk) population estimates for every country, 
#' aggregating to age groups specified via the age group map.
#' 
#' @param age_map a dataframe containing two columns: age and age_group_id to map one to the other
#' @param decomp_step the vdecomp step to pull populations with
#' @param download_new set ot TRUE to download a fresh version
prep_population <- function(age_map, decomp_step = DECOMP_STEP, download_new = FALSE) {
  base_path <- paste0(REGDIR, "ICD_cod/flat_inputs/")
  age_groups_ids <- c(2:20, 30:32, 235)
  sex_ids <- 1:2
  if (download_new) {
    population <- get_population(age_group = age_groups_ids, location_id = "all", year_id = "all", sex_id = sex_ids,
                                 gbd_round_id = 6, decomp_step = DECOMP_STEP)
    readr::write_csv(population, paste0(base_path, "population_", gsub("-", "_", Sys.Date()), ".csv"))
  } else { # read in most recent version from /flat_inputs/
    pop_version <- sort(list.files(paste0(REGDIR, "ICD_cod/flat_inputs/"), pattern = "population"), decreasing = TRUE)[1]
    warning(paste("Reading in population flat file:", pop_version))
    
    population <- fread(paste0(base_path, pop_version))
  }
  
  # clean up, clean up, everybody do your share :music_note:
  # merge on ages and collapse on small age_group_ids
  population <- population[year_id >= 1980]
  population <- merge(population, age_map, by = "age_group_id")
  population <- population[, population_big_age := sum(population), by = c("location_id", "age", "sex_id", "year_id")]
  
  population <- population[, c("location_id", "year_id", "sex_id", "age_group_id", "age", "population", "population_big_age")]
  setnames(population, "location_id", "country")
}

#' Merge on new covariates for input data, square data
#' when this is literally all you have to change about downloaded
#' data. Saves time
#' 
#' @param covariate_id id of the covariate to tack on to input_dt, square_dt
#' @param regression_launch_set_id RLSID to pull shared package, data version
#' 
add_covariate_to_reg_data <- function(covariate_id, covariate_name, regression_launch_set_id, download_new_pop = FALSE) {
  cat("Adding covariate:", covariate_name, "-", covariate_id, "\n")
  
  launch_set <- get_regression_launch_set(regression_launch_set_id)
  
  #' @indir Directory of shared_package_id, aka diabetes unspecified
  #' @outdir where outputs are written fora given regression_launch_set_id
  indir  <- paste0(REGDIR, "ICD_cod/", launch_set$shared_package_id)
  outdir <- paste0(indir, "/", launch_set$regression_launch_set_id)
  
  input_dt  <- fread(paste0(indir, "/data/_input_data_id_", launch_set$data_id, ".csv"))
  square_dt <- fread(paste0(indir, "/data/_square_data_id_", launch_set$data_id, ".csv"))
  
  # get covariate
  cov_dt <- get_covariate_estimates(covariate_id, decomp_step = DECOMP_STEP)
  setnames(cov_dt, c("location_id"), c("country"))
  
  by_sex <- 1 %in% unique(cov_dt$sex_id) # will this work? The exact numbers used here
  by_age <- 5 %in% unique(cov_dt$age_group_id) # don't matter, but their prescence indicates detail
  
  cat("  by sex:", by_sex, ", by age:", by_age, "\n")
  
  merge_cols <- c("country", "year_id")
  if (by_sex) {
    merge_cols <- c(merge_cols, "sex_id")
  }
  
  
  if (by_age) {
    # we're gonna need to aggregate to our age groups using population (VALIDITY: some?)
    population <- prep_population(age_map = unique(input_dt[, c("age", "age_group_id")]), decomp_step = DECOMP_STEP,
                                  download_new = download_new_pop)
    
    cov_dt <- merge(cov_dt, population, by = c(merge_cols, "age_group_id"))
    cov_dt[, pop_weight := population / population_big_age]
    
    # aggregate covar value to big age groups
    merge_cols <- c(merge_cols, "age")
    cov_dt <- cov_dt[, list(mean_value = sum(mean_value * pop_weight)), by = merge_cols]
  }
  
  # clean up
  setnames(cov_dt, c("mean_value"), c(covariate_name))
  cov_dt <- cov_dt[, c(merge_cols, covariate_name), with = F]
  
  # merge and return
  input_dt <- merge(input_dt, cov_dt, by = merge_cols)
  square_dt <- merge(square_dt, cov_dt, by = merge_cols)
  
  readr::write_csv(input_dt, paste0(indir, "/data/_input_data_id_", launch_set$data_id, ".csv"))
  readr::write_csv(square_dt, paste0(indir, "/data/_square_data_id_", launch_set$data_id, ".csv"))
}

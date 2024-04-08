
# Meta --------------------------------------------------------------------

# Description: Fits poisson model for expected mortality in the absence of
#   COVID-19 pandemic, and generates predictions
# Steps:
#   1. Download config and processed all-cause and population inputs
#   2. Fits model separately by sex and model type
#   3. Predict model: generate draws from variance covariance matrix
#   4. Save draws


# Load libraries ----------------------------------------------------------

message(Sys.time(), " | Setup")

library(argparse)
library(arrow)
library(assertable)
library(data.table)
library(demInternal)
library(fs)
library(demUtils)
library(hierarchyUtils)


# Command line arguments --------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir", type = "character", required = !interactive(),
  help = "base directory for this version run"
)
parser$add_argument(
  "--loc_id", type = "integer", required = !interactive(),
  help = "location ID to fit and predict model for"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)


# Setup -------------------------------------------------------------------

# get config
config <- config::get(
  file = paste0(main_dir, "/covid_em_detailed.yml"),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

# set seed
set.seed(123)

# set the global cpu thread pool as specified in job submission
arrow::set_cpu_count(5)

# convert strings to arrow schema format
draws_schema <- mapply(function(col) eval(parse(text=col)), draws_schema)
draws_schema <- do.call(arrow::schema, draws_schema)

draw_id_cols <- unique(c(partitioning_cols, id_cols, draw_col))


# Inputs ------------------------------------------------------------------

message(Sys.time(), " | Inputs")

# get mappings
process_locations <- fread(paste0(main_dir, "/inputs/process_locations.csv"))
process_sexes <- fread(paste0(main_dir, "/inputs/process_sexes.csv"))
process_ages <- fread(paste0(main_dir, "/inputs/process_ages.csv"))

# get ihme_loc_id for current location
ihme_loc <- process_locations[location_id == loc_id, ihme_loc_id]

# processed data (all-cause and population)
data <- fread(paste0(main_dir, "/inputs/data_all_cause/", ihme_loc, ".csv"))

if (loc_id == 434) data[age_name != "0 to 125", deaths := NA]

prepped_data <- data[
  ((year_start < year_cutoff) |
     (year_start == year_cutoff & time_start < time_cutoff)) &
    !is.na(population) & !is.na(deaths)
]

# don't need to predict for years before our data
# also don't predict for places where we don't have observed deaths
data <- data[year_start >= min(prepped_data$year_start) &
               !is.na(population) & !is.na(deaths)]


# Model function ------------------------------------------------------------

#' @title Function to run model
#' @description Fit and predict for one sex, model type, and age type
#' @param model_data \[`data.table()`\]\cr
#'   Input data to fit on.
#' @param model_type \[`character(1)`\]\cr
#'   Model type. Options: 'poisson', 'neg_binom', 'logistic'.
#' @param ss \[`character(1)`\]\cr
#'   Sex to subset to. Options: "male", "female", or "all".
#' @param predict_data \[`data.table()`\]\cr
#'   Data to generate prediction on.
#' @param all_age \[`logical(1)`\]\cr
#'   T/F for whether the inputs are only all-age or are age-specific.
run_model <- function(model_data, model_type, ss, predict_data, all_age) {

  message(paste0(Sys.time(), " | Fit ", model_type, " model for sex ", ss))

  # subset on age and sex
  model_data <- copy(model_data)
  predict_data <- copy(predict_data)
  model_data <- model_data[sex == ss]
  predict_data <- predict_data[sex == ss]
  if (all_age) {
    model_data <- model_data[age_name == "0 to 125"]
    predict_data <- predict_data[age_name == "0 to 125"]
    one_age_group <- T
  } else {
    model_data <- model_data[age_name != "0 to 125"]
    predict_data <- predict_data[age_name != "0 to 125"]
    # check if more than 1 detailed age group
    one_age_group <- length(unique(model_data$age_name)) == 1
  }

  # create formula. only use age effect if age-specific
  if (model_type == "logistic") {
    model_data[, death_rate := deaths / population]
    form <- paste0("death_rate ~ factor(time_start) + factor(year_start) + ",
                   ifelse(one_age_group, "", " factor(age_name)"))
  } else {
    form <- paste0("deaths ~ factor(time_start) + factor(year_start) + ",
                   ifelse(one_age_group, "", " factor(age_name) + "),
                   "offset(log(population))")
  }
  message(paste0("Using formula: ", form))
  form <- as.formula(form)

  set.seed(123)

  # fit Poisson model
  if (model_type == "poisson") {
    fit <- stats::glm(
      formula = form,
      family = poisson(link = log),
      data = model_data
    )
  } else if (model_type == "neg_binom") {
    fit <- MASS::glm.nb(
      formula = form,
      link = log,
      data = model_data
    )
  } else if (model_type == "logistic") {
    fit <- stats::glm(
      formula = form,
      data = model_data,
      family = "binomial"
    )
  } else {
    stop(paste0("'", model_type, "' is an unsupported model type."))
  }

  # get fixed effects draws from variance covariance matrix
  fe <- stats::coef(fit)
  fe_vcov <- stats::vcov(fit)

  fe <- round(fe, 10)
  fe_vcov <- round(fe_vcov, 10)

  fe_draws <- MASS::mvrnorm(n = n_draws, mu = fe, Sigma = fe_vcov)
  fe_draws <- as.data.table(fe_draws)
  fe_draws$draw <- c(1:n_draws)

  # reshape fixed effects draws
  fe_draws <- melt(fe_draws, id.vars = "draw")
  fe_draws[, type := tstrsplit(variable, "\\(|\\)", keep = 2)]
  fe_draws[type != "Intercept", group := tstrsplit(variable, "\\(|\\)", keep = 3)]
  fe_draws_intercept <- fe_draws[
    type == "Intercept",
    list(draw, fe_intercept = value)
  ]
  fe_draws_time <- fe_draws[
    type == "time_start",
    list(draw, time_start = as.numeric(group), fe_time = value)
  ]
  fe_draws_year <- fe_draws[
    type == "year_start",
    list(draw, year_start = as.numeric(group), fe_year = value)
  ]
  fe_draws_age <- fe_draws[
    type == "age_name",
    list(draw, age_name = group, fe_age = value)
  ]

  # fill in reference groups w/ fixed effects zeros
  ref_time <- setdiff(unique(model_data$time_start), unique(fe_draws_time$time_start))
  ref_yr <- setdiff(unique(model_data$year_start), unique(fe_draws_year$year_start))
  ref_age <- setdiff(unique(model_data$age_name), unique(fe_draws_age$age_name))
  fe_draws_time <- rbind(
    fe_draws_time,
    data.table::CJ(draw = 1:n_draws, time_start = ref_time, fe_time = 0)
  )
  fe_draws_year <- rbind(
    fe_draws_year,
    data.table::CJ(draw = 1:n_draws, year_start = ref_yr, fe_year = 0)
  )
  fe_draws_age <- rbind(
    fe_draws_age,
    data.table::CJ(draw = 1:n_draws, age_name = ref_age, fe_age = 0)
  )

  # extrapolate year effect to any predict years not in model fit dataset
  fill_yrs <- setdiff(
    unique(predict_data$year_start),
    unique(fe_draws_year$year_start)
  )
  if (length(fill_yrs) > 0) {
    if (max(fill_yrs) > max(fe_draws_year$year_start)) {
      fe_draws_year <- demUtils::extrapolate(
        fe_draws_year,
        id_cols = c("draw", "year_start"),
        extrapolate_col = "year_start",
        value_col = "fe_year",
        extrapolate_vals = unique(predict_data$year_start),
        method = "uniform",
        n_groups_fit = 2
      )
    } else {
      fe_draws_year <- demUtils::interpolate(
        fe_draws_year,
        id_cols = c("draw", "year_start"),
        interpolate_col = "year_start",
        value_col = "fe_year",
        interpolate_vals = unique(predict_data$year_start)
      )
    }
  }

  # merge fixed effects onto predict data
  d <- merge(predict_data, fe_draws_time, by = "time_start", allow.cartesian = T)
  d <- merge(d, fe_draws_year, by = c("year_start", "draw"))
  d <- merge(d, fe_draws_age, by = c("age_name", "draw"))
  d <- merge(d, fe_draws_intercept, by = c("draw"))

  # predict expected deaths from regression formula
  if (model_type == "logistic") {
    d[, logit_death_rate := fe_intercept + fe_time + fe_year + fe_age]
    d[, death_rate := demUtils::invlogit(logit_death_rate)]
    d[, deaths_expected := death_rate * population]
    d[, c("death_rate", "logit_death_rate") := NULL]
  } else {
    d[,
      deaths_expected := exp(fe_intercept + fe_time + fe_year + fe_age + log(population))
    ]
  }

  # value = excess mortality rate
  d[, value := (deaths - deaths_expected) / population]

  d[, model_type := model_type]

  return(d)
}


# Run all-age and age-specific models -------------------------------------

# Loop over sex and model type (excluding 'regmod' model type)
run_model_types <- c("poisson")

draws <- rbindlist(lapply(run_model_types, function(model_type) {
  draws_mt <- rbindlist(lapply(unique(prepped_data$sex), function(ss) {

    model_data <- prepped_data[(sex == ss)]
    predict_data <- data[(sex == ss)]

    only_all_age <- length(unique(model_data$age_name)) == 1

    # all-age model
    d_all_age <- run_model(
      model_data = model_data,
      model_type = model_type,
      ss = ss,
      predict_data = predict_data,
      all_age = T
    )

    # age-specific model
    if (!only_all_age) {
      d_age_specific <- run_model(
        model_data = model_data,
        model_type = model_type,
        ss = ss,
        predict_data = predict_data,
        all_age = F
      )
      d <- rbind(d_all_age, d_age_specific)
    } else {
      d <- d_all_age
    }
    return(d)
  }))
  return(draws_mt)
}))


# Format ------------------------------------------------------------------

message(Sys.time(), " | Format")

draws[, process_stage := "final"]
draws[, groupings := "estimated"]

# check
assertable::assert_values(
  draws, colnames = c(draw_id_cols, draw_value_cols), test = "not_na"
)
assertable::assert_values(draws, "deaths_expected", test = "gte", test_val = 0)

# subset and sort columns
draws <- draws[, .SD, .SDcols = c(draw_id_cols, draw_value_cols)]
setkeyv(draws, draw_id_cols)


# Save draws --------------------------------------------------------------

message(Sys.time(), " | Save")

# type conversions to match schema
draws[, location_id := as.integer(location_id)]
draws[, year_start := as.integer(year_start)]
draws[, time_start := as.integer(time_start)]
draws[, age_start := as.numeric(age_start)]
draws[, age_end := as.numeric(age_end)]
draws[, draw := as.integer(draw)]
draws[, value := as.numeric(value)]

for (yy in unique(draws$year_start)) {
  arrow::write_dataset(
    dataset = draws[year_start == yy],
    path = fs::path(main_dir, "outputs/draws/"),
    format = draws_format,
    partitioning = partitioning_cols,
    basename_template = paste0(yy, "-{i}.", draws_format)
  )
}

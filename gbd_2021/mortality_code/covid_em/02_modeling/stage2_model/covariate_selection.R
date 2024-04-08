
# Meta --------------------------------------------------------------------

# Perform model selection

# Start with LASSO regression, then iteratively test each covariate for
# significance and direction.


# Load packages -----------------------------------------------------------

library(data.table)
library(glmnet)


# Set parameters ----------------------------------------------------------

current_date <- Sys.Date()

dir_covid_plots <- "FILEPATH"
base_dir <- "FILEPATH"
dir_code <- here::here()
path_model_specifications <- fs::path(dir_code, "stage2_model/specify_models.R")


# Load data ---------------------------------------------------------------

dt_input <- copy(dt_list_prep$fit_draw)

std_error <- function(x) sd(x) / sqrt(length(x))

tmp_std_error <- dt_input[
  j = .(
    std_err_death_rate_excess = std_error(death_rate_excess),
    std_err_log_death_rate_excess = std_error(log(death_rate_excess))
  ),
  by = .(location_id, ihme_loc_id, year_start)
]

tmp_std_error[std_err_death_rate_excess == 0, std_err_death_rate_excess := NA]
tmp_std_error[std_err_log_death_rate_excess == 0, std_err_log_death_rate_excess := NA]

mean_se <- mean(tmp_std_error$std_err_death_rate_excess, na.rm = TRUE)
mean_se_log <- mean(tmp_std_error$std_err_log_death_rate_excess, na.rm = TRUE)

tmp_std_error[is.na(std_err_death_rate_excess), std_err_death_rate_excess := mean_se]
tmp_std_error[is.na(std_err_log_death_rate_excess), std_err_log_death_rate_excess := mean_se_log]

readr::write_csv(tmp_std_error, fs::path(dir_covid_plots, current_date, "FILEPATH"))

# Specify models ----------------------------------------------------------

source(path_model_specifications)

formula_list <- specify_models("all_covs.prep")
mf <- formula_list$all_covs.prep


# Prep model selection ----------------------------------------------------

direction <- c(
  "mobility_lagged" = -1,
  "idr_lagged" = -1,
  "log(cumulative_infections_lagged)" = 1,
  "stars_bin_high" = -1,
  "prop_pop_75plus" = 1,
  "avg_abs_latitude" = 1,
  "universal_health_coverage" = -1,
  "HAQI" = -1,
  "gbd_inpatient_admis" = 1,
  "hypertension_prevalence" = 1,
  "smoking_prevalence" = 1,
  "gbd_obesity" = 1,
  "log(death_rate_covid)" = 1,
  "log(crude_death_rate)" = 1,
  "crude_death_rate_sd_1990" = 1,
  "gbd_diabetes" = 1,
  "log(diabetes_death_rate)" = 1,
  "log(cong_downs_death_rate)" = 1,
  "log(cvd_death_rate)" = 1,
  "log(endo_death_rate)" = 1,
  "log(hemog_sickle_death_rate)" = 1,
  "log(hiv_death_rate)" = 1,
  "log(ncd_death_rate)" = 1,
  "log(neo_death_rate)" = 1,
  "log(neuro_death_rate)" = 1,
  "log(ckd_death_rate)" = 1,
  "log(resp_asthma_death_rate)" = 1,
  "log(resp_copd_death_rate)" = 1,
  "log(subs_death_rate)" = 1
)

dt_direction <- data.table(
  covariate = names(direction),
  direction = direction
)


# Get order of covariates -------------------------------------------------

set.seed(9876)

setdiff(labels(terms(mf)), names(direction))
setdiff(names(direction), labels(terms(mf)))

X_input <- model.matrix(mf, data = dt_input)

## Basic LASSO ----

fit_lasso <- glmnet::glmnet(
  x = X_input[, colnames(X_input) %in% names(direction)],
  y = log(dt_input$death_rate_excess),
  alpha = 1
)

fit_cov_order <- setDT(broom::tidy(fit_lasso))
fit_cov_order <- fit_cov_order[order(step), .SD[1], by = "term"]

## CV LASSO ----

fit_lasso_cv <- glmnet::cv.glmnet(
  x = X_input[, colnames(X_input) %in% names(direction)],
  y = log(dt_input$death_rate_excess),
  nfolds = 10,
  alpha = 1
)

## ROVER list ----

covs_rover <- list(
  set1 = c(
    "log(cumulative_infections_lagged)", "prop_pop_75plus",
    "log(death_rate_covid)", "log(crude_death_rate)", "gbd_diabetes",
    "idr_lagged", "universal_health_coverage", "smoking_prevalence", "stars_bin_high"
  ),
  set2 = c(
    "log(cvd_death_rate)", "log(ckd_death_rate)", "mobility_lagged"
  )
)


# Model selection ---------------------------------------------------------

## Basic LASSO ----

select_covariates <- function(candidates, data, expected_direction) {

  included <- c()
  insignificant <- c()
  reverse <- c()

  for (cov in fit_cov_order[-1, term]) {

    form_rhs <- paste0(c(included, cov), collapse = " + ")
    form <- as.formula(paste0("log(death_rate_excess) ~ ", form_rhs))

    m <- lm(formula = form, data)
    mc <- setDT(broom::tidy(m))

    cov_est <- mc[like(term, cov, fixed = TRUE), estimate]
    cov_pv <- mc[like(term, cov, fixed = TRUE), p.value]

    if (sign(cov_est) != expected_direction[cov]) {
      reverse <- c(reverse, cov)
    } else if (cov_pv > 0.5) {
      insignificant <- c(insignificant, cov)
    } else {
      included <- c(included, cov)
    }

  }

  list(
    included = included,
    insignificant = insignificant,
    reverse = reverse
  )

}

cov_selections <- select_covariates(
  candidates = fit_cov_order[-1, term],
  data = dt_input,
  expected_direction = direction
)

## CV LASSO ----

fit_lasso_cv$lambda.min
fit_lasso_cv$lambda.1se

dt_cv_covs <-
  c("lambda.min") |>
  purrr::set_names(~gsub(".", "_", .x, fixed = TRUE)) |>
  purrr::map_dfr(
    \(x) coef(fit_lasso_cv, s = x) |> as.matrix() |> as.data.table(keep.rownames = "covariate"),
    .id = "lambda"
  ) |>
  setDT() |>
  setnames("s1", "coef") |>
  merge(dt_direction, by = "covariate") |>
  setnames("direction", "expected_direction") |>
  (\(dt) dt[coef != 0])() |>
  (\(dt) dt[, correct_direction := sign(coef) == sign(expected_direction)])() |>
  setorderv(c("lambda", "correct_direction", "coef"), order = -1L)

## ROVER ----

frm_rover <- as.formula(paste0("log(death_rate_excess) ~ ", paste0(covs_rover$set1, collapse = " + ")))
all.vars(frm_rover[-2])

mm_input_scaled <- frm_rover |>
  update.formula(~ . + 0) |>
  model.matrix(dt_input) |>
  scale()

lm_rover <- lm(frm_rover, data = dt_input)
lm_rover_standardized <- lm(log(dt_input$death_rate_excess) ~ mm_input_scaled)

lm_rover_coef <- setDT(broom::tidy(lm_rover)) |> setnames("term", "covariate")
lm_rover_coef[dt_direction, correct_direction := sign(estimate) == i.direction, on = "covariate"]


# Build model -------------------------------------------------------------

frm_lambda_min <- as.formula(
  paste0("log(death_rate_excess) ~ ", paste0(dt_cv_covs[lambda == "lambda_min" & (correct_direction), covariate], collapse = " + "))
)

lm_lambda_min <- lm(frm_lambda_min, data = dt_input)
lm_labda_min_coef <- setDT(broom::tidy(lm_lambda_min))

dt_cv_covs_final <- dt_cv_covs[, -"lambda"][lm_labda_min_coef, lm_est := i.estimate, on = .(covariate = term)]

dt_cv_covs_final[, lm_correct_direction := sign(lm_est) == expected_direction]


# Save results ------------------------------------------------------------

yaml::write_yaml(
  cov_selections,
  fs::path("FILEPATH")
)

purrr::iwalk(
  split(dt_cv_covs, by = "lambda", keep.by = FALSE),
  ~readr::write_csv(
    .x,
    fs::path("FILEPATH")
  )
)

readr::write_csv(
  dt_cv_covs_final,
  fs::path("FILEPATH")
)

readr::write_csv(
  lm_rover_coef,
  fs::path("FILEPATH")
)


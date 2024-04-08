
# Meta --------------------------------------------------------------------

# Perform out-of-sample validation


# Load packages -----------------------------------------------------------

library(data.table)
library(furrr)


# Set parameters ----------------------------------------------------------

plan("multisession")

current_date <- Sys.Date()
dir_covid_plots <- "FILEPATH"
dir_out <- fs::path(dir_covid_plots, current_date, "model_validation")

region_cols <- c("region_name", "super_region_name")

trim_residuals_quantiles <- c(0.05, 0.95)


# Load maps ---------------------------------------------------------------

map_locs_gbd <- demInternal::get_locations(gbd_year = 2021)

map_locs <- rbind(
  map_locs_gbd,
  data.table(
    location_id = 99999,
    ihme_loc_id = "ITA_99999",
    location_name = "Provincia autonoma di Bolzano + Provincia autonoma di Trento",
    region_name = "Western Europe",
    super_region_name = "High-income",
    level = 4,
    parent_id = 86,
    is_estimate = 1,
    path_to_top_parent = "1,64,73,86,99999"
  ),
  fill = TRUE
)

map_locs[
  location_id %in% c(35498, 35499),
  `:=`(
    parent_id = 99999,
    level = 5,
    path_to_top_parent = paste0("1,64,73,86,99999,", location_id)
  )
]


# Load data ---------------------------------------------------------------

dt_list_prep <- readRDS(fs::path("FILEPATH"))
dt_coef_compare <- fread(fs::path("FILEPATH"))


# Prep model formulas -----------------------------------------------------

frm_base <- formula_base <- formula(
  log(death_rate_excess) ~
    log(cumulative_infections_lagged) +
    prop_pop_75plus +
    log(death_rate_covid) +
    log(crude_death_rate) +
    log(diabetes_death_rate) +
    idr_lagged +
    smoking_prevalence +
    gbd_inpatient_admis
)

pub_covs <- dt_coef_compare[!is.na(`coef-published`) & term != "(Intercept)", term] |>
  gsub("stars_binlow", "stars_bin_high", x = _)
prev_covs <- dt_coef_compare[!is.na(`coef-10_28`) & term != "(Intercept)", term]

list_formulas <- list(
  "published" = formula(paste0("log(death_rate_excess) ~ ", paste0(pub_covs, collapse = " + "))),
  "12_16-hiv" = update(frm_base, ~ .  + log(hiv_death_rate)),
  "12_16-haqi" = update(frm_base, ~ .  + HAQI),
  "12_16-hiv-haqi" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI),
  "12_16-hiv-haqi-endo" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + log(endo_death_rate)),
  "12_16-hiv-haqi-endo-hyper" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + log(endo_death_rate) + hypertension_prevalence),
  "12_16-haqi-hyper" = update(frm_base, ~ .  + HAQI + hypertension_prevalence),
  "12_16-hiv-haqi-hyper" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + hypertension_prevalence),
  "12_16-hiv-haqi-mobility" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + mobility_lagged),
  "12_16-hiv-haqi-lat" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + avg_abs_latitude),
  "12_16-hiv-haqi-mobility-lat" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + mobility_lagged + avg_abs_latitude),
  "12_16-hiv-haqi-endo-mobility" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + log(endo_death_rate) + mobility_lagged),
  "12_16-hiv-haqi-endo-lat" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + log(endo_death_rate) + avg_abs_latitude),
  "12_16-hiv-haqi-endo-mobility-lat" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + log(endo_death_rate) + mobility_lagged + avg_abs_latitude),
  "12_16-hiv-haqi-endo-mobility-stars" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + log(endo_death_rate) + mobility_lagged + stars_bin_high),
  "12_16-haqi-stars" = update(frm_base, ~ .  + HAQI + stars_bin_high),
  "12_16-hiv-haqi-stars" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + stars_bin_high),
  "12_16-hiv-haqi-mobility-stars" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + mobility_lagged + stars_bin_high),
  "12_16-haqi-gbd_diabetes" = update(frm_base, ~ .  + HAQI - log(diabetes_death_rate) + gbd_diabetes),
  "12_16-hiv-haqi-gbd_diabetes" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI - log(diabetes_death_rate) + gbd_diabetes),
  "12_16-hiv-haqi-endo-gbd_diabetes" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + log(endo_death_rate) - log(diabetes_death_rate) + gbd_diabetes),
  "12_16-hiv-haqi-endo-mobility-gbd_diabetes" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + log(endo_death_rate) + mobility_lagged - log(diabetes_death_rate) + gbd_diabetes),
  "12_16-hiv-haqi-endo-mobility-lat-gbd_diabetes" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + log(endo_death_rate) + mobility_lagged + avg_abs_latitude - log(diabetes_death_rate) + gbd_diabetes),
  "12_16-hiv-haqi-endo-lat-gbd_diabetes" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + log(endo_death_rate) + avg_abs_latitude - log(diabetes_death_rate) + gbd_diabetes),
  "12_16-hiv-haqi-mobility-gbd_diabetes" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + mobility_lagged - log(diabetes_death_rate) + gbd_diabetes),
  "12_16-hiv-haqi-mobility-lat-gbd_diabetes" = update(frm_base, ~ .  + log(hiv_death_rate) + HAQI + mobility_lagged + avg_abs_latitude - log(diabetes_death_rate) + gbd_diabetes)
)

frm_all <- list_formulas |>
  lapply(\(f) labels(terms(f))) |>
  purrr::reduce(union) |>
  paste0(collapse = " + ") |>
  (\(x) as.formula(paste("log(death_rate_excess) ~", x)))()

frm_prep <- update(frm_all, NULL ~ location_id + log(death_rate_excess) + person_years + death_rate_person_years + . + 0)


# Prep data ---------------------------------------------------------------

## Model fitting data ----

dt_fit <- model.matrix(frm_prep, dt_list_prep$fit_draw) |>
  as.data.table() |>
  (\(dt) dt[, lapply(.SD, mean), by = "location_id"])()

log_covs <- grep("^log\\(.+\\)$", names(dt_fit), value = TRUE)

dt_fit[, (log_covs) := lapply(.SD, exp), .SDcols = log_covs]
setnames(dt_fit, log_covs, gsub("^log\\(|\\)$", "", log_covs))

dt_fit[
  map_locs,
  (region_cols) := .(i.region_name, i.super_region_name),
  on = "location_id"
]
setcolorder(dt_fit, c("location_id", region_cols))

# Model prediction data ----

dt_pred <- rbindlist(
  list(
    "2020" = dt_list_prep$fit_2020_mean[
      j = .SD,
      .SDcols = c(region_cols, all.vars(frm_prep))
    ],
    "2021" = dt_list_prep$fit_2021_mean[
      j = .SD,
      .SDcols = c(region_cols, all.vars(frm_prep))
    ],
    "all" = dt_fit
  ),
  idcol = "time_frame",
  use.names = TRUE
)


# Fit/predict functions ---------------------------------------------------

calc_region_residuals <- function(dt_resid, region_map) {

  dt_region_resid <- dt_resid[
    j = .(resid_region = mean(.resid, na.rm = TRUE)),
    by = "region_name"
  ]

  dt_region_resid <- dt_region_resid[
    region_map,
    on = "region_name"
  ]

  dt_super_region_resid <- dt_resid[
    j = .(resid_super_region = mean(.resid, na.rm = TRUE)),
    by = "super_region_name"
  ]

  dt_region_resid[
    dt_super_region_resid,
    resid_super_region := i.resid_super_region,
    on = "super_region_name"
  ]

  dt_region_resid <- dt_region_resid[j = .(
    super_region_name,
    region_name,
    residual = fifelse(is.na(resid_region), resid_super_region, resid_region)
  )]

  # Fill completely missing super regions with global residual
  dt_region_resid[is.na(residual), residual := mean(dt_resid$residual)]

  return(dt_region_resid)

}

model_predict_resid <- function(m, dt_resid, dt_pred) {

  dt_result <- setDT(broom::augment(m, newdata = dt_pred))
  dt_result[
    dt_resid,
    .(
      location_id,
      time_frame,
      scale = "linear",
      person_years,
      actual = death_rate_excess,
      predicted = exp(.fitted + i.residual)
    ),
    on = "region_name",
    nomatch = NULL
  ]

}

fit_trimmed_residuals <- function(f, data, residual_trim) {

  dt_m1 <- lm(f, data) |>
    broom::augment(newdata = data) |>
    as.data.table()

  resid_trim <- quantile(dt_m1[[".resid"]], residual_trim)

  dt_trim <- dt_m1[`.resid` %between% resid_trim, -c(".fitted", ".resid")]

  lm(f, data = dt_trim)

}

get_predicted_actual <- function(dt_fit, dt_pred, list_frm, residual_trim, region_cols) {

  list_model <- lapply(
    list_frm,
    \(f) fit_trimmed_residuals(f, dt_fit, residual_trim)
  )

  list_resid <- list_model |>
    lapply(\(m) setDT(broom::augment(m, newdata = dt_fit))[, c(..region_cols, ".resid")]) |>
    lapply(\(dt, loc_map) calc_region_residuals(dt, map_locs[level == 2, c(..region_cols)]))

  purrr::map2(
    list_model, list_resid,
    \(m, dt_resid) model_predict_resid(m, dt_resid, dt_pred = dt_pred)
  ) |> rbindlist(idcol = "model")

}


# Get coefficients --------------------------------------------------------

dt_coefs <- list_formulas |>
  lapply(\(f) f |>
    fit_trimmed_residuals(data = dt_fit, residual_trim = trim_residuals_quantiles) |>
      broom::tidy() |>
      setDT()
  ) |>
  rbindlist(idcol = "model") |>
  setnames(c("estimate", "std.error"), c("coef", "se"))

dt_coefs[, model := factor(model, levels = names(list_formulas), ordered = TRUE)]

dt_coefs_wide <- dcast(dt_coefs, term ~ model, value.var = c("coef", "se"), sep = "-")

dt_coefs[term == "idr_lagged"][order(-coef), -c("statistic", "p.value")]


# Validation --------------------------------------------------------------

dt_pred_actual <- unique(dt_fit$location_id) |>
  future_map(\(loc) get_predicted_actual(
    dt_fit[location_id != loc],
    dt_pred[location_id == loc],
    list_formulas,
    residual_trim = trim_residuals_quantiles,
    region_cols = region_cols
  )) |>
  rbindlist()

dt_validation <- melt(dt_pred_actual, measure.vars = c("actual", "predicted"))
dt_validation[, value := person_years * value]
dt_validation <- dcast(dt_validation[, -"person_years"], ...~time_frame, value.var = "value")
dt_validation[variable == "predicted", `2021` := all - `2020`]

dt_validation <- dt_validation |>
  melt(measure.vars = c("2020", "2021", "all"), variable.name = "time_frame") |>
  dcast(...~scale, value.var = "value")

dt_validation[
  dt_pred_actual,
  linear := linear / i.person_years,
  on = .(model, location_id, time_frame)
]

dt_validation[, `log` := log(linear)]

dt_validation <- dt_validation |>
  melt(measure.vars = c("linear", "log"), variable.name = "scale") |>
  dcast(...~variable, value.var = "value")

dt_validation[, relative_error := (predicted - actual) / actual]

dt_validation[!complete.cases(dt_validation)]

dt_validation_summary <- dt_validation[
  complete.cases(dt_validation),
  j = .(
    rmse = Metrics::rmse(actual, predicted),
    mae = Metrics::mae(actual, predicted),
    mrae = mean(abs(relative_error)),
    mre = mean(relative_error)
  ),
  by = .(model, time_frame, scale)
]

dt_validation_summary_2020_2021 <- dt_validation[
  complete.cases(dt_validation) & time_frame != "all",
  j = .(
    time_frame = "2020_2021",
    rmse = Metrics::rmse(actual, predicted),
    mae = Metrics::mae(actual, predicted),
    mrae = mean(abs(relative_error)),
    mre = mean(relative_error)
  ),
  by = .(model, scale)
]

dt_validation_summary <- rbind(
  dt_validation_summary,
  dt_validation_summary_2020_2021,
  use.names = TRUE
)

setorder(dt_validation_summary, scale, time_frame, rmse)
setcolorder(dt_validation_summary, c("time_frame", "scale", "model"))

dt_validation_summary[, .SD[1], by = .(time_frame, scale)]

dt_validation_summary_wide <- dcast(
  dt_validation_summary,
  ...~ scale + time_frame,
  value.var = c("rmse", "mae", "mrae", "mre"),
  sep = "-"
)


# Save --------------------------------------------------------------------

if (!fs::dir_exists(dir_out)) {
  fs::dir_create(dir_out, mode = "775")
}

stopifnot(fs::dir_exists(dir_out))

readr::write_csv(dt_validation, fs::path(dir_out, "validation_predicted_actual.csv"))
readr::write_csv(dt_validation_summary, fs::path(dir_out, "validation_summary.csv"))
readr::write_csv(dt_validation_summary_wide, fs::path(dir_out, "validation_summary-wide.csv"))
readr::write_csv(
  dt_validation_summary[, .SD[1], by = .(time_frame, scale)],
  fs::path(dir_out, "validation_summary-best.csv")
)

readr::write_csv(dt_coefs, fs::path(dir_out, "compare_coefs.csv"))
readr::write_csv(dt_coefs_wide, fs::path(dir_out, "compare_coefs-wide.csv"))
readr::write_csv(
  dt_coefs[term == "idr_lagged"][order(-coef), -c("statistic", "p.value")],
  fs::path(dir_out, "compare_coefs-idr.csv")
)

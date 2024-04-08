
# Load Packages -----------------------------------------------------------

## DESCRIPTION
#  Bring attention to the frequently used packages within this script. Other
#  packages are used, but will necessarily be prefixed with the package name.

library(data.table)
library(broom)
library(purrr)


# Set parameters ----------------------------------------------------------

## DESCRIPTION
#  Define the variables that affect how the script will run. Mainly sets
#  input/output paths and overall run version.

current_date <- Sys.Date()
timestamp_id <- format(Sys.time(), "%Y-%m-%d-%H-%M")
run_id_s3 <- paste0("s3-", timestamp_id)

set.seed(4567)
n_draws <- 100

dir_covid_plots <- fs::path("FILEPATH")
base_dir <- fs::path("FILEPATH")
out_dir <- fs::path(base_dir, run_id_s3, "outputs")

dir_code <- here::here()
path_model_specifications <- fs::path(dir_code, "stage2_model/specify_models.R")

input_date <- "DATE"

path_dt_list <- list(
  fit_mean = fs::path("FILEPATH"),
  fit_draw = fs::path("FILEPATH"),
  fit_2020_mean = fs::path("FILEPATH"),
  fit_2021_mean = fs::path("FILEPATH"),
  pred_2020_draw = fs::path("FILEPATH"),
  pred_2021_draw = fs::path("FILEPATH"),
  pred_all_draw = fs::path("FILEPATH")
)

flag_use_person_years_threshold <- TRUE
flag_floor_IDR <- FALSE
flag_use_resid <- TRUE
flag_use_region_tcs <- FALSE
flag_trim_residuals <- TRUE
flag_set_star_bin_high <- FALSE

person_years_threshold <- 1e6
idr_lagged_floor <- 0.01
trim_residuals_quantiles <- c(0.05, 0.95)


# Load maps ---------------------------------------------------------------

## DESCRIPTION
#  Define the relationships between internal IHME IDs and common names. The
#  location map additional defines the hierarchy of locations from subnational
#  units to global for use in aggregation and scaling.

map_locs_gbd <- demInternal::get_locations(gbd_year = 2020)
map_locs_covid <- demInternal::get_locations(gbd_year = 2020, location_set_name = "COVID-19 modeling")

map_locs_covid <- rbind(
  map_locs_covid,
  map_locs_gbd[ihme_loc_id %like% "ZAF_"],
  data.table(
    location_id = 99999,
    ihme_loc_id = "ITA_99999",
    location_name = "Provincia autonoma di Bolzano + Provincia autonoma di Trento",
    region_id = 73,
    region_name = "Western Europe",
    super_region_id = 64,
    super_region_name = "High-income",
    level = 4,
    parent_id = 86,
    path_to_top_parent = "1,64,73,86,99999"
  ),
  fill = TRUE
)

map_locs_covid[
  location_id %in% c(35498, 35499),
  `:=`(
    parent_id = 99999,
    level = 5,
    path_to_top_parent = paste0("1,64,73,86,99999,", location_id)
  )
]

map_locs_gbd <- rbind(
  map_locs_gbd[!ihme_loc_id %like% "ITA"],
  map_locs_covid[ihme_loc_id %like% "ITA"]
)

map_locs <- copy(map_locs_gbd)

map_locs[, path_ttp := lapply(strsplit(path_to_top_parent, ","), as.integer)]

map_data_frame <- data.table(
  data_frame = c("pred_2020_draw", "pred_2021_draw", "pred_all_draw"),
  parent = c("pred_all_draw", "pred_all_draw", NA),
  year_start = c(2020, 2021, 2020),
  year_end = c(2021, 2022, 2022),
  year_id = c(2020, 2021, NA)
)


# Load data ---------------------------------------------------------------

## DESCRIPTION
#  Load and collect all unmodified covariate data frames into a single list.

dt_list_raw <- purrr::map(path_dt_list, fread)


# Get special location groups ---------------------------------------------

## DESCRIPTION
#  Define different groups of locations that require non-standard treatment
#  during estimation.

# Get locations to outlier from model fitting
locs_outlier_model_fit <- map_locs[
  ihme_loc_id %in% c("CHN_354", "GTM", "GBR") |
    ihme_loc_id %like% "PHL_|POL_|JPN_|NZL_|SWE_" |
    (ihme_loc_id %like% "GBR" & level == 6),
  location_id
]

# Prep locations to exclude from prediction
locs_nonestimate <- map_locs[
  ihme_loc_id %in% c("TJK", "TKM", "TZA", "NIC", "PRK", "VEN"),
  location_id
]

# Get island nation locations where there is no infection data
locs_islands <- map_locs[
  ihme_loc_id %in% c("KIR", "FSM", "TON", "ASM", "COK", "NRU", "NIU", "PLW", "TKL", "TUV"),
  location_id
]

# Get locations where a zero COVID death rate is believable
locs_keep_zero <- purrr::map_dfr(
  dt_list_raw[!names(dt_list_raw) %like% "fit"],
  ~.x[death_rate_covid == 0, .(location_id = unique(location_id))],
  .id = "data_frame"
)

# GBD non-estimate locations
locs_gbd_nonestimate <- map_locs[
  ihme_loc_id %like% "GBR" & level %in% c(3, 5),
  location_id
]

# Get Locations where the ratio should be 1
loc_ids_ratio1 <- map_locs[ihme_loc_id %like% "RUS", location_id]

# Get locations where negative input excess should be ignored
locs_ignore_neg_excess <- map_locs[
  ihme_loc_id %in% c("GTM"),
  location_id
]

# Set location-draws with negative stage 1 predicted excess to 1
dt_locs_s1_neg_excess <-
  dt_list_raw[!names(dt_list_raw) %like% "fit"] |>
  purrr::map( \(d) d[j = .(mdre = mean(death_rate_excess)), by = "location_id"][
    mdre < 0 & !location_id %in% c(locs_ignore_neg_excess, 53536, 53580),
    .(location_id)
  ]) |>
  rbindlist(idcol = "data_frame")

# Get locations with stage 1 excess
locs_s1_excess <- purrr::map(
  dt_list_raw[!names(dt_list_raw) %like% "fit"],
  ~.x[
    !is.na(death_rate_excess) & !location_id %in% locs_outlier_model_fit,
    unique(location_id)
  ]
)

# Get locations that require scaling (where input excess is available at the
# national level but not lower level)
locs_scale_manual <- c(
  map_locs[ihme_loc_id %in% c("RUS", "PHL", "NOR"), location_id],
  4749
)
locs_nonscale_manual <- c(
  99999,
  map_locs[ihme_loc_id %in% c("UKR"), location_id],
  44533,
  map_locs[ihme_loc_id %like% "KEN" & level ==  4, location_id]
)

locs_scale_list <- purrr::map(locs_s1_excess, function(x) {

  map_locs[
    level > 3,
    .(n_lower_with_excess = sum(location_id %in% x)),
    by = .(parent_id)
  ][
    n_lower_with_excess == 0 & !parent_id %in% locs_nonscale_manual,
    unique(as.integer(c(parent_id, locs_scale_manual)))
  ]

})

# Get national locations where subnationals should use the national excess
# death rate
locs_use_national_rate <- map_locs[ihme_loc_id == "PHL", location_id]


# Prep data ---------------------------------------------------------------

## DESCRIPTION
#  Apply additional preparation and cleaning to the input data frames. This
#  ensures that only viable locations are used in model fitting and prediction
#  and that all data frames have the needed set of covariates.

# Fit and Prediction covariate prep

prep_cov_general <- function(data) {

  data_prep <- data[location_id %in% map_locs$location_id]

  data_prep[
    map_locs,
    `:=`(
      ihme_loc_id = i.ihme_loc_id,
      region_name = i.region_name,
      super_region_name = i.super_region_name
    ),
    on = .(location_id)
  ]

  if (flag_floor_IDR) {
    data_prep[idr_lagged < idr_lagged_floor, idr_lagged := idr_lagged_floor]
  }

  data_prep[is.na(stars), stars := -1]
  data_prep[, `:=`(
    stars = as.factor(stars),
    stars_bin_high = as.integer(stars > 3)
  )]
  data_prep[, prop_ncd := ncd_death_rate / crude_death_rate]

  remove_cols <- c("location_name", "ci_person_years")
  data_prep[, (remove_cols) := NULL]

  return(data_prep)

}

prep_cov_specific <- function(data, type) {

  stopifnot(type %like% "fit|pred")

  data <- copy(data)

  if (isTRUE(type %like% "fit")) {

    # Fit data specific prep

    data <- data[!location_id %in% map_locs[parent_id == 4749, location_id]]
    data <- data[!location_id %in% 60132:60137]

    data[, ratio := death_rate_excess / death_rate_covid]

    data <- data[!is.na(death_rate_excess) & death_rate_excess > 0]
    data[, outlier := FALSE]
    data[death_rate_covid == 0, outlier := TRUE]

    data[location_id %in% locs_outlier_model_fit, outlier := TRUE]

    data <- data[!(outlier), -c("idr_reference", "mobility_reference", "outlier")]

    # Drop locations under person-years threshold
    if (flag_use_person_years_threshold) {
      data <- data[death_rate_person_years >= person_years_threshold]
    }

    assertable::assert_values(
      data,
      setdiff(colnames(data), c("death_rate_person_years")),
      test = "not_na"
    )

  } else if (isTRUE(type %like% "pred")) {

    # Prediction data specific prep

    data <- data[
      !location_id %in% c(locs_nonestimate, locs_islands, locs_gbd_nonestimate)
    ]
    data <- data[!location_id %in% c(99999, 44538)]

    data[
      death_rate_covid == 0 & !is.na(death_rate_excess),
      death_rate_excess := NA
    ]

    data[, outlier := FALSE]
    data[is.na(cumulative_infections), outlier := TRUE]

    data <- data[
      !(outlier),
      -c("idr_reference", "mobility_reference", "stars", "outlier")
    ]

    if (flag_set_star_bin_high) {
      data[, stars_bin_high := 1]
    }

    stopifnot(nrow(data[is.na(death_rate_person_years) & !is.na(death_rate_excess)]) == 0)

    assertable::assert_values(
      data,
      setdiff(colnames(data), c("death_rate_excess", "death_rate_person_years", "ratio")),
      test = "not_na"
    )

  }

  assertable::assert_values(
    data,
    setdiff(grep("death_rate", colnames(dt_list_raw$pred_2020_draw), value = TRUE), "death_rate_excess"),
    test = "gte",
    test_val = 0,
    na.rm = TRUE
  )

  return(data)

}

dt_list_prep <- dt_list_raw %>%
  purrr::map(prep_cov_general) %>%
  purrr::imap(~prep_cov_specific(data = .x, type = .y))


# Create counterfactual prediction scenarios ------------------------------

## DESCRIPTION
#  From the data frames intended for model prediction, create a copy of the
#  data with covariate values modified to reflect a hypothetical counterfactual
#  scenario where the spread of COVID is minimized.
#
#  The scenario is created by:
#  1) Setting lagged infection detection rate to the max observed value

make_reference_frames <- function(dt_list, frame_names) {

  dt_list_ref <- dt_list[frame_names] %>%
    purrr::map(function(dt) {
      dt <- copy(dt)
      dt[, `:=`(
        idr_lagged = max(idr_lagged)
      )]
    })

  frame_ref_names <- paste0(frame_names, ".reference")
  names(dt_list_ref) <- frame_ref_names

  return(dt_list_ref)

}

dt_list_prep_ref <- make_reference_frames(
  dt_list_prep,
  grep("pred", names(dt_list_prep), value = TRUE)
)
dt_list_prep <- c(dt_list_prep, dt_list_prep_ref)
rm(dt_list_prep_ref)


# Specify models ----------------------------------------------------------

## DESCRIPTION
#  Load model formulas from external specification file. Multiple formulas can
#  be loaded, and for each formula a model will be fit. Formula names follow the
#  format {model_name}.{response_variable}, which will be used to identify
#  different sets of prediction results in later sections.

source(path_model_specifications)

formula_list <- specify_models(c(
  "m1_cdr.death_rate_excess"
))


# Fit models --------------------------------------------------------------

## DESCRIPTION
#  For every model formula, fit one model for each draw in the 'fit' data frame.
#  Different combinations of model parameters (formula, draw, fit data subset)
#  are collected into rows of a data frame, which are then iterated over to
#  fit all the required models.

dt_list_fit <- split(dt_list_prep$fit_draw, by = "draw")

dt_model_params <- CJ(
  formula = formula_list,
  data = dt_list_fit,
  sorted = FALSE
)

dt_model_param_names <- CJ(
  formula_name = names(formula_list),
  draw = as.integer(names(dt_list_fit)),
  sorted = FALSE
)

# Add ID column to aid in joining parameters with parameter names
dt_model_params[, rn := .I]
dt_model_param_names[, rn := .I]
dt_model <- merge(dt_model_param_names, dt_model_params, by = "rn")
dt_model[, rn := NULL]
rm(dt_model_params, dt_model_param_names)

dt_model[, model := purrr::pmap(.(formula, data), lm)]

dt_model[, c("model_name", "response") := tstrsplit(formula_name, ".", fixed = TRUE)]
dt_model[, c("formula_name") := NULL]
setcolorder(dt_model, c("model_name", "response"))

dt_motd <- formula_list$m1_cdr.death_rate_excess |>
  update(NULL ~ location_id + log(death_rate_excess) + . + 0) |>
  model.matrix(dt_list_prep$fit_draw) |>
  as.data.table() |>
  (\(dt) dt[, lapply(.SD, mean), by = "location_id"])()

log_covs <- grep("^log\\(.+\\)$", names(dt_motd), value = TRUE)

dt_motd[, (log_covs) := lapply(.SD, exp), .SDcols = log_covs]
setnames(dt_motd, log_covs, gsub("^log\\(|\\)$", "", log_covs))

lm_motd <- lm(formula_list$m1_cdr.death_rate_excess, dt_motd)

if (flag_trim_residuals) {

  dt_motd_resid <- as.data.table(broom::augment(lm_motd, newdata = dt_motd))
  resid_trim <- quantile(dt_motd_resid[[".resid"]], trim_residuals_quantiles)
  dt_motd_trim <- dt_motd_resid[`.resid` %between% resid_trim, -c(".fitted", ".resid")]
  lm_motd <- lm(formula_list$m1_cdr.death_rate_excess, dt_motd_trim)

}

dt_model[, model := lapply(1:n_draws, function(x) lm_motd)]

# Extract model summaries -------------------------------------------------

## DESCRIPTION
#  Collect draw-level model information for diagnostic purposes.

dt_model_summary <- dt_model[, .(
  model = model_name,
  response,
  draw,
  dt_tidy = purrr::map(model, tidy),
  dt_glance = purrr::map(model, glance)
)]

dt_coef <- setDT(tidyr::unnest(dt_model_summary[, -"dt_glance"], dt_tidy))
dt_rsq <- setDT(tidyr::unnest(dt_model_summary[, -"dt_tidy"], dt_glance))
dt_rsq <- dt_rsq[, .(model, response, draw, rsq = r.squared)]

dt_coef_wide <- dcast(
  dt_coef,
  ...~model,
  value.var = c("estimate", "std.error", "statistic", "p.value")
)


# Group model coefficients ------------------------------------------------

## DESCRIPTION
#  For each model formula, combine the draw-level model coefficients into a
#  single matrix.

dt_model_coef <- dt_model[
  ,
  .(
    formula = unique(formula),
    coefs = .(do.call(rbind, purrr::map(model, coef)))
  ),
  by = .(model_name, response)
]


# Get model residuals -----------------------------------------------------

## DESCRIPTION
#  Collect the residuals of the model fit data frame and also calculate
#  region / super region average residuals for each model formula. These are
#  then used in the prediction step.

calc_region_residuals <- function(dt_resid, loc_map) {

  map_region_draws <- loc_map[level == 2, .(super_region_name, region_name)]
  map_region_draws <- rbindlist(replicate(n_draws, map_region_draws, simplify = FALSE), idcol = "draw")

  dt_region_resid <- dt_resid[
    ,
    .(resid_region = mean(residual, na.rm = TRUE)),
    by = .(draw, region_name)
  ]

  dt_region_resid <- dt_region_resid[
    map_region_draws,
    on = .(region_name, draw)
  ]

  dt_super_region_resid <- dt_resid[
    ,
    .(resid_super_region = mean(residual, na.rm = TRUE)),
    by = .(draw, super_region_name)
  ]

  dt_region_resid[
    dt_super_region_resid,
    resid_super_region := i.resid_super_region,
    on = c("draw", "super_region_name")
  ]

  # Ensure SSA regions use overall super region residual
  dt_region_resid[
    super_region_name == "Sub-Saharan Africa",
    resid_region := resid_super_region
  ]

  dt_region_resid <- dt_region_resid[, .(
    draw,
    super_region_name,
    region_name,
    residual = fifelse(is.na(resid_region), resid_super_region, resid_region)
  )]

  # Fill completely missing super regions with global residual
  dt_region_resid[is.na(residual), residual := mean(dt_resid$residual)]

  return(dt_region_resid)

}

keep_cols_resid <- c(
  "location_id", "ihme_loc_id", "super_region_name", "region_name", ".resid"
)

calc_resid <- function(data, formula, model) {
  pred_val <- rowSums(model.matrix(formula, data) %*% diag(coef(model)))
  data[, .(
    location_id, ihme_loc_id, super_region_name, region_name,
    .resid = log(death_rate_excess) - pred_val
  )]
}

dt_residuals <- dt_model[, .(
  model_name, response, draw,
  dt_resid = purrr::pmap(.(data, formula, model), calc_resid)
)]

dt_residuals[, dt_resid := purrr::map(dt_resid, ~setDT(.x[, ..keep_cols_resid]))]
dt_residuals <- setDT(tidyr::unnest(dt_residuals, dt_resid))
setnames(dt_residuals, ".resid", "residual")

dt_residuals <- dt_residuals[
  ,
  .(
    dt_resid = list(.SD),
    dt_resid_region = list(calc_region_residuals(.SD, map_locs))
  ),
  by = .(model_name, response)
]


# Predict from model ------------------------------------------------------

## DESCRIPTION
#  For each combination of model formula and prediction data frame, calculate
#  predicted values. The method of prediction follows the usual step of applying
#  model coefficients to prediction frame covariates transformed according to
#  the model formula. In addition to this "baseline" prediction, a second
#  prediction is generated by adding region-level residuals to the baseline
#  prediction. Optionally, for locations that existed in the model fit data
#  frame, this second prediction type will use instead in-sample residual.
#
#  Similar to the model fitting step, predication parameter combinations are
#  stored in rows of a data frame and iterated over to get predictions for each
#  combination, identified by a:
#
#   * model name
#   * model response
#   * prediction data frame name
#
#  Prediction results are finally flattened into a single, unnested data frame.

predict_draws <- function(coefs,
                          formula,
                          dt_pred_frame,
                          dt_resid_insample,
                          dt_resid_region,
                          add_insample_residual) {

  # Fixed effect predictions

  cov_formula <- update.formula(formula, NULL ~ .)

  dt_pred <- split(dt_pred_frame, by = c("location_id", "region_name")) %>%
    purrr::map_dfr(
      ~rowSums(model.matrix(cov_formula, data = .x) * coefs),
      .id = "loc_region"
    ) %>%
    setDT() %>%
    melt(
      id.vars = "loc_region",
      variable.name = "draw",
      value.name = "baseline",
      variable.factor = FALSE
    )

  dt_pred[, c("location_id", "region_name") := tstrsplit(
    loc_region, ".", type.convert = TRUE, fixed = TRUE
  )]

  dt_pred[, `:=`(
    loc_region = NULL,
    draw = as.integer(draw)
  )]

  # Append region-level residuals

  dt_pred[
    dt_resid_region,
    resid_region := i.residual,
    on = .(draw, region_name)
  ]

  # Append in-sample residual

  dt_resid_insample <- dt_resid_insample[!ihme_loc_id %like% "IND"]

  dt_pred[
    dt_resid_insample,
    resid_insample := i.residual,
    on = .(draw, location_id)
  ]

  # Align predictions with input data

  if (add_insample_residual) {

    dt_pred[, with_resid := baseline + fifelse(is.na(resid_insample), resid_region, resid_insample)]

  } else {

    dt_pred[, with_resid := baseline + resid_region]

  }

  dt_pred[, .(location_id, draw, baseline, with_resid)]

}

# Create prediction argument lists
dt_pred_params <- copy(dt_model_coef)
dt_pred_params[
  dt_residuals,
  `:=`(
    dt_resid_insample = i.dt_resid,
    dt_resid_region = i.dt_resid_region
  ),
  on = .(model_name, response)
]

dt_data_frame <- CJ(
  model_name = unique(dt_pred_params$model_name),
  data_frame = names(dt_list_prep),
  sorted = FALSE
)

dt_data_frame[
  setDT(tibble::enframe(dt_list_prep, name = "data_frame", value = "dt_pred_frame")),
  dt_pred_frame := i.dt_pred_frame,
  on = "data_frame"
]

dt_data_frame <- dt_data_frame[!data_frame %like% "fit"]

dt_pred_params <- dt_pred_params[dt_data_frame, on = "model_name"]

dt_pred_params[, dt_preds := purrr::pmap(
  .(coefs, formula, dt_pred_frame, dt_resid_insample, dt_resid_region),
  predict_draws,
  add_insample_residual = TRUE
)]

dt_pred <- setDT(tidyr::unnest(
  dt_pred_params[, .(model = model_name, response, data_frame, dt_preds)],
  dt_preds
))

dt_pred <- melt(
  dt_pred,
  id.vars = c("model", "response", "data_frame", "location_id", "draw"),
  variable.name = "pred_type"
)

dt_pred[, value := exp(value)]


# Separate reference frames -----------------------------------------------

## DESCRIPTION
#  Split the column identifying the prediction data frame name into two columns
#  for the base data frame name and prediction scenario.

dt_pred[, c("data_frame", "scenario") := tstrsplit(data_frame, ".", fixed = TRUE)]
dt_pred[is.na(scenario), scenario := "prediction"]


# Handle locations with zero COVID ----------------------------------------

## DESCRIPTION
#  Locations with zero COVID should have no excess deaths

dt_pred[locs_keep_zero, value := 0, on = .(location_id, data_frame)]


# Substitute direct excess deaths estimates -------------------------------

## DESCRIPTION
#  Use stage 1 results where available instead of covariate-predicted excess.

dt_excess_direct <- purrr::map_dfr(
  dt_list_raw[names(dt_list_raw) %like% "pred"],
  ~.x[!is.na(death_rate_excess), .(location_id, draw, death_rate_excess)],
  .id = "data_frame"
)[
  !location_id %in% locs_ignore_neg_excess &
    !location_id %in% map_locs[ihme_loc_id %like% "IND", location_id] &
    !location_id == map_locs[ihme_loc_id == "SWE_4944", location_id]
]

dt_pred[
  dt_excess_direct,
  death_rate_excess_input := i.death_rate_excess,
  on = .(data_frame, location_id, draw)
]

dt_pred[
  scenario == "prediction" & pred_type == "with_resid" & !is.na(death_rate_excess_input),
  value := death_rate_excess_input
]

## Handle single year input residuals ----

## DESCRIPTION
#  For locations with only one year of input data, apply the residual from the
#  year with data to the baseline (without residual) prediction to the year
#  without data to get a new "with-residual" prediction for the missing year.

dt_missing_direct_excess_years <- dcast(
  dt_excess_direct,
  location_id ~ data_frame,
  value.var = "death_rate_excess",
  fun.aggregate = length,
)

dt_resid_single_year <- dt_pred[
  location_id %in% dt_missing_direct_excess_years[xor(pred_2020_draw == 0, pred_2021_draw == 0), location_id] &
    !is.na(death_rate_excess_input) &
    death_rate_excess_input > 0 &
    data_frame != "pred_all_draw" &
    pred_type == "baseline" &
    scenario == "prediction",
  .(
    data_frame_swap = fifelse(data_frame == "pred_2020_draw", "pred_2021_draw", "pred_2020_draw"),
    log_resid = log(death_rate_excess_input) - log(value)
  ),
  by = .(model, response, data_frame, location_id, draw, pred_type, scenario)
]

dt_resid_single_year[, c("data_frame", "data_frame_swap") := .(data_frame_swap, NULL)]

dt_pred_resid_update <- dt_pred[
  dt_resid_single_year,
  .(
    model, response, data_frame, location_id, draw,
    pred_type = "with_resid",
    scenario,
    pred_new = exp(log(x.value) + i.log_resid)
  ),
  on = .(model, response, data_frame, location_id, draw, pred_type, scenario)
]

dt_pred[
  dt_pred_resid_update,
  value := i.pred_new,
  on = .(model, response, data_frame, location_id, draw, pred_type, scenario)
]

rm(dt_resid_single_year, dt_pred_resid_update)

dt_pred[, death_rate_excess_input := NULL]


## Turn off residual ----

if (!flag_use_resid) {

  dt_pred <- dt_pred |>
    dcast(...~pred_type, value.var = "value")

  dt_pred[
    !dt_excess_direct,
    with_resid := baseline,
    on = .(data_frame, location_id, draw)
  ]

  dt_pred <- melt(
    dt_pred,
    measure.vars = c("baseline", "with_resid"),
    variable.name = "pred_type"
  )

}

## Set subnationals to national rate ----

dt_pred_nat <- dt_pred[
  location_id %in% locs_use_national_rate &
    pred_type == "with_resid" & scenario == "prediction"
]

dt_pred[map_locs, parent_id := i.parent_id, on = "location_id"]
dt_pred[
  dt_pred_nat,
  value := i.value,
  on = .(model, response, data_frame, parent_id = location_id, draw, pred_type, scenario)
]

dt_pred[, parent_id := NULL]
rm(dt_pred_nat)


# Format prediction results for aggregation/scaling -----------------------

## DESCRIPTION
#  Append to the prediction results input person-years and COVID death numbers
#  to allow for converting prediction results to count space for location
#  scaling and aggregation.

dt_covid_person_years <- rbindlist(
  dt_list_raw,
  idcol = "data_frame",
  fill = TRUE,
  use.names = TRUE
)

dt_covid_person_years <- dt_covid_person_years[, .(
  data_frame,
  draw,
  location_id,
  person_years,
  person_years_covid = covid_person_years,
  death_rate_covid
)]

dt_pred <- dcast(dt_pred, ...~response, value.var = "value")

dt_pred[
  dt_covid_person_years,
  `:=`(
    person_years = i.person_years,
    person_years_covid = i.person_years_covid,
    deaths_covid = i.person_years_covid * i.death_rate_covid,
    deaths_excess = i.person_years * death_rate_excess
  ),
  on = .(data_frame, draw, location_id)
]

dt_pred[, "death_rate_excess" := NULL]


# Back-fill excess --------------------------------------------------------

dt_back_fill <- dcast(
  dt_pred[
    scenario == "prediction" & pred_type == "with_resid" &
      !location_id %in% dt_missing_direct_excess_years$location_id,
    -c("person_years", "person_years_covid", "deaths_covid")
  ],
  ...~data_frame,
  value.var = "deaths_excess"
)

dt_back_fill[, pred_2021_draw := pred_all_draw - pred_2020_draw]
dt_back_fill[, c("pred_2020_draw", "pred_all_draw", "data_frame") := .(NULL, NULL, "pred_2021_draw")]

dt_pred[
  dt_back_fill,
  deaths_excess := i.pred_2021_draw,
  on = .(model, data_frame, location_id, draw, pred_type, scenario)
]


# Add reference ratios ----------------------------------------------------

dt_pred_ratios <- dcast(
  dt_pred,
  ... ~ scenario + pred_type,
  value.var = "deaths_excess",
  sep = "."
)

dt_pred_ratios <- dt_pred_ratios[, .(
  model, data_frame, location_id, draw,
  person_years, person_years_covid,
  deaths_covid,
  deaths_excess = prediction.with_resid,
  true_covid_prop_excess = reference.baseline / prediction.baseline,
  ratio_covid_excess = deaths_covid / prediction.with_resid
)]

dt_pred_ratios[is.nan(true_covid_prop_excess), true_covid_prop_excess := 1]

dt_pred_ratios[
  deaths_covid == 0,
  c("true_covid_prop_excess", "ratio_covid_excess") := 1
]

dt_pred_ratios[true_covid_prop_excess > 1, true_covid_prop_excess := 1]

dt_pred_ratios[, `:=`(
  ratio_true_reported_covid = pmax(true_covid_prop_excess, ratio_covid_excess) / ratio_covid_excess
)]

dt_pred_ratios[ratio_covid_excess > 1, ratio_true_reported_covid := 1]

dt_pred_ratios[
  deaths_covid == 0 & deaths_excess == 0, ratio_true_reported_covid := 1
]

dt_pred_ratios[deaths_excess < 0, ratio_true_reported_covid := 1]

# Set ratio of total to reported covid for a location to 1 when mean level
# excess is negative
dt_pred_ratios[
  dt_locs_s1_neg_excess,
  ratio_true_reported_covid := 1,
  on = .(location_id, data_frame)
]

# Manually set ratio of total to reported covid for some locations
dt_pred_ratios[
  location_id %in% loc_ids_ratio1,
  ratio_true_reported_covid := 1
]

# Add total covid ratio
dt_pred_ratios[, ratio_total_covid_excess := (deaths_covid * ratio_true_reported_covid) / deaths_excess]

dt_pred_ratios[
  deaths_covid == 0 & deaths_excess == 0, ratio_total_covid_excess := 1
]


# Prep scaling and aggregation --------------------------------------------

# Convert to count space for scaling aggregation
dt_pred_ratios[, `:=`(
  deaths_excess_reference = deaths_excess * true_covid_prop_excess,
  true_covid_prop_excess = NULL,
  ratio_covid_excess = NULL,
  ratio_true_reported_covid = NULL
)]

scale_agg_id_cols <- c("model", "data_frame", "location_id", "draw")

# Separate out variables to be withheld from scaling/aggregation
no_scale_agg_cols <- "ratio_total_covid_excess"

dt_pred_no_scale_agg <- dt_pred_ratios[, c(..scale_agg_id_cols, ..no_scale_agg_cols)]
dt_pred_ratios[, (no_scale_agg_cols) := NULL]

scale_agg_value_cols <- setdiff(colnames(dt_pred_ratios), scale_agg_id_cols)


# Scale locations -----------------------------------------------------------

scale_func <- function(dt_pred, dt_map, id_cols, value_cols) {

  dt_map_sub <- dt_map

  hierarchyUtils::scale(
    dt = dt_pred[location_id %in% unique(c(dt_map_sub$child, dt_map_sub$parent))],
    id_cols = id_cols,
    value_cols = value_cols,
    col_stem = "location_id",
    col_type = "categorical",
    mapping = dt_map_sub,
    agg_function = sum,
    missing_dt_severity = "warning",
    collapse_missing = TRUE
  )

}

locs_scale_list <- purrr::map2(
  locs_scale_list,
  dt_list_prep[names(locs_scale_list)],
  ~c(.x[.x %in% unique(.y$location_id)])
)

scale_loc_map <- purrr::map_dfr(locs_scale_list, function(locs_scale) {

  map_locs[
    purrr::map_lgl(path_ttp, ~any(locs_scale %in% .x)) & !location_id %in% locs_scale,
    .(
      child = location_id,
      parent = parent_id,
      top_parent = purrr::map_int(path_ttp, ~intersect(locs_scale, .x))
    )
  ]

}, .id = "data_frame")

if (nrow(scale_loc_map) > 0) {

  dt_pred_scaled <- purrr::pmap_dfr(
    scale_loc_map[, .(map = .(.SD)), by = .(data_frame, top_parent)],
    ~scale_func(
      dt_pred_ratios[data_frame == ..1],
      dt_map = ..3,
      id_cols = scale_agg_id_cols,
      value_cols = scale_agg_value_cols
    )
  )

  dt_loc_scaled <- rbind(
    dt_pred_ratios[
      !scale_loc_map[, .(data_frame, location_id = child)],
      on = .(data_frame, location_id)
    ],
    dt_pred_scaled[
      scale_loc_map[, .(data_frame, location_id = child)],
      on = .(data_frame, location_id),
      nomatch = NULL
    ],
    use.names = TRUE
  )

} else {

  dt_loc_scaled <- copy(dt_pred_ratios)

}


# Aggregate time periods --------------------------------------------------

if (!is.null(map_data_frame)) {

  dt_time_scaled <- hierarchyUtils::agg(
    dt_loc_scaled[data_frame != "pred_all_draw"],
    id_cols = scale_agg_id_cols,
    value_cols = scale_agg_value_cols,
    col_stem = "data_frame",
    col_type = "categorical",
    mapping = map_data_frame[!is.na(parent), .(child = data_frame, parent)]
  )

  # Account for values that were scaled to 0, introducing NaNs
  setnafill(
    dt_time_scaled,
    type = "const",
    fill = 0,
    nan = NA,
    cols = scale_agg_value_cols
  )

  dt_time_scaled <- rbind(
    dt_loc_scaled[data_frame != "pred_all_draw"],
    dt_time_scaled
  )

} else {

  dt_time_scaled <- copy(dt_loc_scaled)

}


# Calculate total COVID deaths ----------------------------------------------

dt_time_scaled[
  dt_pred_no_scale_agg,
  deaths_total_covid_unfloored := i.ratio_total_covid_excess * deaths_excess,
  on = scale_agg_id_cols
]

dt_time_scaled[
  ,
  deaths_total_covid := fifelse(deaths_total_covid_unfloored < deaths_covid, deaths_covid, deaths_total_covid_unfloored)
]

scale_agg_value_cols <- c(scale_agg_value_cols, "deaths_total_covid", "deaths_total_covid_unfloored")



# Shift mean total COVID --------------------------------------------------

# For some locations with subnational units, we want to ensure that at the
# mean level total COVID is equal to reported COVID (usually when locations
# have more excess that reported COVID deaths).

locs_eng_subs <- map_locs[
  purrr::map_lgl(path_ttp, \(x) 4749 %in% x) & level == 6,
  location_id
]

dt_mean_total_covid_shift <- dt_time_scaled[
  location_id %in% locs_eng_subs,
  .(diff_rep_mtc = mean(deaths_covid) - mean(deaths_total_covid)),
  by = setdiff(scale_agg_id_cols, "draw")
]

dt_time_scaled[
  dt_mean_total_covid_shift,
  deaths_total_covid := i.diff_rep_mtc + deaths_total_covid,
  on = setdiff(scale_agg_id_cols, "draw")
]

dt_time_scaled[deaths_total_covid < 0, deaths_total_covid := 0]


# Aggregate ---------------------------------------------------------------

agg_func <- function(dt, mapping, id_cols, value_cols) {

  # Fill missing locations from aggregation mapping with zero to allow for an
  # incomplete aggregation using available locations
  dt_missing_locs <- dt[
    ,
    .(location_id = setdiff(mapping[level >= 3, child], location_id)),
    by = setdiff(id_cols, "location_id")
  ]

  dt_missing_locs[, (value_cols) := 0]

  dt_agg <- hierarchyUtils::agg(
    rbind(dt, dt_missing_locs),
    id_cols = id_cols,
    value_cols = value_cols,
    col_stem  = "location_id",
    col_type = "categorical",
    mapping = mapping[, -"level"],
    agg_function = sum,
    present_agg_severity = "none",
    missing_dt_severity = "warning",
    na_value_severity = "warning"
  )

  dt_agg_combined <- rbind(
    dt[!location_id %in% unique(dt_agg$location_id)],
    dt_agg
  )

  return(dt_agg_combined)

}

# Create mapping
map_loc_agg <- map_locs[location_id != 1, .(child = location_id, parent = parent_id, level)]

dt_agg <- agg_func(
  dt_time_scaled,
  mapping = map_loc_agg,
  id_cols = scale_agg_id_cols,
  value_cols = scale_agg_value_cols
)


# Fill missing location with parent values --------------------------------

dt_fill_locs <- dt_covid_person_years[
  location_id %in% c(locs_nonestimate, locs_islands)
]

dt_fill_locs <- dt_fill_locs[
  unique(dt_agg[, .(model, data_frame)]),
  on = "data_frame",
  allow.cartesian = TRUE
]

dt_fill_locs[map_locs, parent_id := i.parent_id, on = "location_id"]

# Use China to fill PRK covid
dt_fill_locs[location_id == 7, parent_id := 44533]

dt_fill_locs[
  dt_agg,
  parent_death_rate_covid := i.deaths_covid / i.person_years,
  on = .(parent_id = location_id, draw, data_frame)
]

dt_fill_locs[location_id %in% locs_islands, death_rate_covid := 0]
dt_fill_locs[location_id %in% locs_nonestimate, death_rate_covid := parent_death_rate_covid]

dt_fill_locs[is.na(person_years_covid), person_years_covid := 0]

dt_fill_locs[, `:=`(
  deaths_covid = death_rate_covid * person_years,
  death_rate_covid = NULL,
  parent_death_rate_covid = NULL
)]

dt_fill_locs[
  dt_agg,
  `:=`(
    deaths_excess = i.deaths_excess / i.person_years * person_years,
    deaths_excess_reference = i.deaths_excess_reference / i.person_years * person_years,
    deaths_total_covid_unfloored = i.deaths_total_covid_unfloored / i.person_years * person_years
  ),
  on = .(model, data_frame, draw, parent_id = location_id)
]

dt_fill_locs[
  location_id %in% locs_islands,
  c("deaths_excess", "deaths_total_covid_unfloored") := 0
]

dt_fill_locs[, parent_id := NULL]

# Make year specific excess scale to all time-period prediction
dt_fill_locs_time_scaled <- hierarchyUtils::agg(
  dt_fill_locs[data_frame != "pred_all_draw", -"deaths_total_covid_unfloored"],
  id_cols = scale_agg_id_cols,
  value_cols = setdiff(scale_agg_value_cols, c("deaths_total_covid", "deaths_total_covid_unfloored")),
  col_stem = "data_frame",
  col_type = "categorical",
  mapping = map_data_frame[!is.na(parent), .(child = data_frame, parent)]
)

# Account for values that were scaled to 0, introducing NaNs
setnafill(
  dt_fill_locs_time_scaled,
  type = "const",
  fill = 0,
  nan = NA,
  cols = setdiff(scale_agg_value_cols, c("deaths_total_covid", "deaths_total_covid_unfloored"))
)

dt_fill_locs_time_scaled <- rbind(
  dt_fill_locs[data_frame != "pred_all_draw", -"deaths_total_covid_unfloored"],
  dt_fill_locs_time_scaled
)

dt_fill_locs_time_scaled[
  dt_fill_locs,
  deaths_total_covid_unfloored := i.deaths_total_covid_unfloored,
  on = scale_agg_id_cols
]

dt_fill_locs_time_scaled[
  ,
  deaths_total_covid := fifelse(deaths_total_covid_unfloored < deaths_covid, deaths_covid, deaths_total_covid_unfloored)
]


# Re-aggregate with missing locations filled ------------------------------

dt_time_scaled2 <- rbind(dt_time_scaled, dt_fill_locs_time_scaled, use.names = TRUE)

dt_agg_filled <- agg_func(
  dt_time_scaled2,
  mapping = map_loc_agg,
  id_cols = scale_agg_id_cols,
  value_cols = scale_agg_value_cols
)

stopifnot(map_locs[!location_id %in% unique(dt_agg_filled$location_id), .N] == 0)

# Make total COVID for the all time-period an aggregate of year specific
dt_time_agg <- hierarchyUtils::agg(
  dt_agg_filled[data_frame != "pred_all_draw", c(..scale_agg_id_cols, "deaths_total_covid", "deaths_total_covid_unfloored")],
  id_cols = scale_agg_id_cols,
  value_cols = c("deaths_total_covid", "deaths_total_covid_unfloored"),
  col_stem = "data_frame",
  col_type = "categorical",
  mapping = map_data_frame[!is.na(parent), .(child = data_frame, parent)]
)

dt_agg_filled[
  dt_time_agg,
  `:=`(
    deaths_total_covid = i.deaths_total_covid,
    deaths_total_covid_unfloored = i.deaths_total_covid_unfloored
  ),
  on = scale_agg_id_cols
]


# Recalculate ratios ------------------------------------------------------

dt_agg_filled[, `:=`(
  true_covid_prop_excess = deaths_excess_reference / deaths_excess,
  ratio_covid_excess = deaths_covid / deaths_excess,
  ratio_excess_covid = deaths_excess / deaths_covid,
  ratio_true_reported_covid = deaths_total_covid / deaths_covid,
  deaths_excess_reference = NULL
)]

if (flag_use_region_tcs) {

  dt_agg_filled[map_locs, super_region_id := i.super_region_id, on = "location_id"]
  dt_agg_filled[
    dt_agg_filled[location_id %in% map_locs[level == 1, location_id]],
    `:=`(
      ratio_true_reported_covid = i.ratio_true_reported_covid,
      deaths_total_covid = i.ratio_true_reported_covid * x.deaths_covid
    ),
    on = .(model, data_frame, draw, super_region_id = location_id)
  ]
  dt_agg_filled[, super_region_id := NULL]

}

dt_agg_filled[
  deaths_covid == 0 & deaths_excess == 0,
  c("true_covid_prop_excess", "ratio_covid_excess", "ratio_excess_covid", "ratio_true_reported_covid") := 1
]

dt_agg_filled[
  deaths_covid == 0 & deaths_excess != 0,
  ratio_true_reported_covid := 1
]

dt_agg_filled[ratio_true_reported_covid < 1, ratio_true_reported_covid := 1]

dt_agg_filled[
  location_id %in% setdiff(locs_nonestimate, 7),
  ratio_true_reported_covid := NA
]

tmp <- dt_agg_filled[
  !is.na(ratio_true_reported_covid),
  .(
    mean_scalar = mean(ratio_true_reported_covid),
    q2.5_scalar = quantile(ratio_true_reported_covid, probs = .025),
    q97.5_scalar = quantile(ratio_true_reported_covid, probs = .975),
    max_scalar = max(ratio_true_reported_covid),
    sd = sd(ratio_true_reported_covid),
    max_dev = max(ratio_true_reported_covid) - mean(ratio_true_reported_covid)
  ),
  by = .(location_id, data_frame)
]
tmp[, `:=`(
  flag1 = max_dev > (mean_scalar + sd),
  flag2 = max_scalar > (q97.5_scalar + sd)
)]


# Shuffle draws -----------------------------------------------------------

new_draw_order <- sample(n_draws)
dt_agg_filled[
  order(draw),
  draw := new_draw_order,
  by = .(location_id, model, data_frame)
]

setorderv(dt_agg_filled, c("location_id", "draw"))


# Summarize results -------------------------------------------------------

summary_id_cols <- c(scale_agg_id_cols, "person_years", "deaths_covid")
summary_val_cols <- setdiff(colnames(dt_agg_filled), summary_id_cols)

# Define summary functions that remove NA's since non-default arguments
# can't be passed to the functions used in `demUtils::summarize_dt()`
mean <- function(x) mean(x, na.rm = TRUE)
`q2.5` <- function(x) stats::quantile(x, probs = 0.025, na.rm = TRUE)
`q97.5` <- function(x) stats::quantile(x, probs = 0.975, na.rm = TRUE)

dt_pred_agg_summary <- demUtils::summarize_dt(
  dt_agg_filled,
  id_cols = summary_id_cols,
  summarize_cols = "draw",
  value_cols = summary_val_cols,
  summary_fun = c("mean", "q2.5", "q97.5"),
  probs = NULL
)

# Revert definition of mean
mean <- base::mean

quantile_colnames <- grep("q2\\.5|q97\\.5", colnames(dt_pred_agg_summary), value = TRUE)

if (uniqueN(dt_agg_filled$draw) == 1) {

  dt_pred_agg_summary <- dt_pred_agg_summary[, -..quantile_colnames]

} else {

  dt_pred_agg_summary[!data_frame %like% "draw", (quantile_colnames) := NA]

}


# Format results ----------------------------------------------------------

map_locs_merge <- map_locs[, .(
  location_id,
  ihme_loc_id,
  location_name,
  region_name,
  super_region_name,
  level
)]

dt_pred_final <- map_locs_merge[
  dt_pred_agg_summary,
  on = "location_id"
]

dt_pred_draw_final <- map_locs_merge[
  dt_agg_filled,
  on = "location_id"
]

dt_mean_handoff <- dt_pred_agg_summary[
  model == "m1_cdr",
  .(
    data_frame,
    location_id,
    deaths_excess = deaths_excess_mean,
    scalar = ratio_true_reported_covid_mean,
    deaths_reported_covid = deaths_covid,
    deaths_total_covid = deaths_total_covid_mean
  )
]

dt_mean_handoff[
  map_data_frame,
  `:=`(year_start = i.year_start, year_end = i.year_end, year_id = i.year_id),
  on = "data_frame"
]

dt_mean_handoff[, data_frame := NULL]

dt_mean_handoff_covid <- dt_pred_final[
  model == "m1_cdr" & data_frame != "pred_all_draw",
  .(
    data_frame,
    location_id,
    deaths_reported_covid = deaths_covid,
    deaths_total_covid = deaths_total_covid_mean
  )
]

dt_mean_handoff_covid[
  map_data_frame,
  `:=`(year_id = i.year_id),
  on = "data_frame"
]

dt_mean_handoff_covid[, data_frame := NULL]

dt_draw_handoff <- dt_agg_filled[
  model == "m1_cdr",
  .(
    data_frame,
    location_id,
    draw,
    deaths_excess,
    scalar = ratio_true_reported_covid,
    deaths_reported_covid = deaths_covid,
    deaths_total_covid = deaths_total_covid
  )
]

dt_draw_handoff[
  map_data_frame,
  `:=`(year_start = i.year_start, year_end = i.year_end, year_id = i.year_id),
  on = "data_frame"
]

dt_draw_handoff[, data_frame := NULL]

dt_draw_handoff_covid <- dt_pred_draw_final[
  model == "m1_cdr" & data_frame != "pred_all_draw",
  .(
    data_frame,
    location_id,
    draw,
    deaths_reported_covid = deaths_covid,
    deaths_total_covid = deaths_total_covid
  )
]

dt_draw_handoff_covid[
  map_data_frame,
  `:=`(year_id = i.year_id),
  on = "data_frame"
]

dt_draw_handoff_covid[, data_frame := NULL]

stopifnot(map_locs[!location_id %in% dt_pred_final$location_id, .N] == 0)


# Save results ------------------------------------------------------------

fs::dir_create(
  path = fs::path(base_dir, run_id_s3, c("outputs", "diagnostics")),
  mode = "0775"
)

readr::write_csv(
  dt_coef_wide,
  fs::path(out_dir, paste0("model_coefs-", run_id_s3, ".csv"))
)

readr::write_csv(
  dt_rsq,
  fs::path(out_dir, paste0("model_rsq-", run_id_s3, ".csv"))
)

readr::write_csv(
  dt_residuals$dt_resid[[1]],
  fs::path(out_dir, paste0("model_residual-insample-", run_id_s3, ".csv"))
)

readr::write_csv(
  dt_residuals$dt_resid_region[[1]],
  fs::path(out_dir, paste0("model_residual-region-", run_id_s3, ".csv"))
)

readr::write_csv(
  dt_pred_final,
  fs::path(out_dir, paste0("model_prediction-ratios-summary-", run_id_s3, ".csv"))
)

readr::write_csv(
  dt_pred_draw_final,
  fs::path(out_dir, paste0("model_prediction-ratios-draws-", run_id_s3, ".csv"))
)

readr::write_csv(
  dt_mean_handoff,
  fs::path(out_dir, paste0("covid_em_scalars-mean-", run_id_s3, ".csv"))
)

readr::write_csv(
  dt_draw_handoff,
  fs::path(out_dir, paste0("covid_em_scalars-draw-", run_id_s3, ".csv"))
)

readr::write_csv(
  dt_mean_handoff_covid,
  fs::path(out_dir, paste0("covid_reported_total-mean-", run_id_s3, ".csv"))
)

readr::write_csv(
  dt_draw_handoff_covid,
  fs::path(out_dir, paste0("covid_reported_total-draw-", run_id_s3, ".csv"))
)


# Meta --------------------------------------------------------------------

# Title: Compile stage-1 out-of-sample results and compute ensemble weights


# Settings ----------------------------------------------------------------

## SELECT: directories
code_dir <- here::here()
base_dir <- "FILEPATH"

## SELECT: run IDs
run_id_oos <- "VERSION"


# Libraries ---------------------------------------------------------------

library(data.table)
library(ggplot2)
library(magrittr)

source(paste0(code_dir, "/functions/R/load_summaries_and_data.R"))


# Out-of-sample Inputs ----------------------------------------------------

dt_oos <- load_summaries_and_data(
  run_ids = run_id_oos,
  base_dir = base_dir
)

dt_oos <- dt_oos[age_name == "0 to 125" & sex == "all" & !model_type %like% "ensemble"]

assertthat::assert_that(
  setequal(
    unique(dt_oos$model_type),
    c("regmod_6", "regmod_12", "regmod_18", "regmod_24", "poisson", "previous_year")
  ),
  msg = "Expected model types 6, 12, 18, and 24 month tail are not all present."
)


# RMSE --------------------------------------------------------------------

# out-of-sample, only keep 2019 post time cutoff
dt_oos_rmse <- dt_oos[
  year_start == 2019 & time_start >= time_cutoff &
  !is.na(death_rate_expected) & !is.na(death_rate_observed)
]

# compute rmse
dt_oos_rmse[, sq_error := (death_rate_expected - death_rate_observed) ^2]

# get one rmse for all location-time
rmse_oos <- dt_oos_rmse[, list(rmse = sqrt(mean(sq_error))), by = "model_type"]

# repeat with separate rmse values by location
rmse_oos_2 <- dt_oos_rmse[,
  list(rmse = sqrt(mean(sq_error))),
  by = c("location_id", "model_type")
]


readr::write_csv(
  rmse_oos,
  fs::path(code_dir, "/constant_inputs/stage1_oos_rmse.csv")
)

readr::write_csv(
  rmse_oos_2,
  fs::path(code_dir, "/constant_inputs/stage1_oos_rmse_by_loc.csv")
)


# Plots -------------------------------------------------------------------

# boxplots
gg_oos <- ggplot(rmse_oos_2) +
  geom_boxplot(aes(x = model_type,
                   y = rmse,
                   fill = model_type),
               outlier.shape = NA) +
  geom_abline(slope = 0, intercept = 0) +
  labs(title = "Out-of-sample: all locations",
       x = "",
       y = "location-specific RMSE",
       fill = "") +
  theme_bw() +
  theme(axis.text.x=element_blank(),
        axis.ticks.x=element_blank())


# Ensemble ----------------------------------------------------------------

# compute weights
weights <- copy(rmse_oos)
weights[, weight := (1/(rmse^2))]
weights[, weight := weight / (sum(weight))]

# save weights
readr::write_csv(
  weights,
  fs::path(code_dir, "/constant_inputs/stage1_ensemble_weights.csv")
)

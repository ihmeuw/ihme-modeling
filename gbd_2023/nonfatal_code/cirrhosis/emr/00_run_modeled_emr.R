###############################################################################
## Purpose: EMR regression using MR BRT
###############################################################################

### Packages and directory set up ---------------------------------------------
rm(list=ls())

pacman::p_load(reticulate, data.table, ggplot2, dplyr, stringr, tidyr, DBI, openxlsx, gtools, gridExtra,glue)

date <- Sys.Date()

### USER DEFINED SETTINGS - FILL IN WITH YOUR OWN SPECS ------------------------

# Directory where YOU have cloned this repository
# Make sure that you are on the main branch with the latest commits (git pull)
emr_dir <- "FILEPATH"

# For uploader validation
user_name <- Sys.info()['user']
# General location to save EMR files
home_dir <- paste0("FILEPATH")

# Cause name for saving csvs and for use in plot titles
cause_name <- "cirrhosis"
# EMR data Dismod model version ID from which you want to pull EMR
model_id <- OBJECT

# Upload specs
bundle_id <- OBJECT
# xwalk version to which you want to append the predicted EMR
crosswalk_version_id <- OBJECT
# Bundle version for crosswalk_version_id specified above
bundle_version_id <- OBJECT
# Upload path for new xwalk version
upload_datapath <- FILEPATH
# seq generated for EMR dummy row in bundle version listed above
# THIS MUST COME FROM RUNNING the emr_bundle_prep.R
seq_emr <- 1


# MR-BRT decisions
# Covariates included in MR-BRT run (vary if trying different covariates)
cov_list <- "age_sex_haq"
# Change to trim more/less in MR-BRT regression
trim <- 0.1

# Remove specific locations, inpatient and/or claims
# Fill in location_ids you want to exclude from EMR analysis separated by commas
remove_locations <- c()
remove_inpatient = FALSE  # remove all inpatient?
remove_marketscan = FALSE # remove all U.S. Marketscan claims?
remove_taiwan <- FALSE # remove Taiwan claims?
remove_singapore <- FALSE # remove Singapore claims?
remove_poland <- FALSE # remove Poland claims?
remove_russia <- FALSE # remove Russia claims?

# Remove "claims - flagged" sources
remove_russia_claims_flagged <- TRUE
remove_mongolia_claims_flagged <- FALSE
remove_korea_claims_flagged <- FALSE

# Prediction matrix decisions, 10 - 12 age points recommended
age_predict <- c(0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100) 

location_level <- c(3, 4) # 3 = national only, c(3,4) = national and subnational


## Source functions
functions_dir <- "FILEPATH"

# MR-BRT / Crosswalk is preferred to load through reticulate explicitly now
reticulate::use_python("FILEPATH")
mr <- import("mrtool")

# Load in the central shared functions
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_model_results.R")

# This is where the main functions for modeled emr live
source(file.path(emr_dir, "01_modeled_emr_functions.R"))

# Themes
source('/FILEPATH/ihme-ggplot-themes.R')
source('/FILEPATH/birds-color-palettes.R')


# Get model version metadata
# location_set_id, location_set_version_id, release_id, gbd_round_id
model_version_id_metadata <- get_model_version_metadata(model_id)

### 1) Prep Data --------------------------------------------------------------

## Create date-specific output filepaths if they do not already exist
dir.create(file.path(home_dir, cause_name, date, 'diagnostics'), recursive = TRUE)
bundle_output_path <- paste0(home_dir, cause_name, "/")
date_output_path <- paste0(bundle_output_path, date, "/")
plot_output_path <- paste0(date_output_path, "diagnostics/")

## Pull in EMR data, rename, and remove unneeded variables
dt <- get_emr_data(model_id = model_id)

## This function removes certain clinical and location data specified
dt <- remove_clinical_or_location_data(
  data = dt,
  locations_to_remove = remove_locations
)

## Reformatting data for emr process, adds midage and midyear, changes sex to 0,1
# log mean and standard error. This takes a long time
dt <- reformat_data_for_modeled_emr(data = dt)

## Custom function to merge haqi by year/location, output list of HAQ and data
dt <- merge_haqi_covariate(data = dt, release_id = model_version_id_metadata$release_id)
haq <- dt[["haq"]] #grab haq data from list output
dt <- dt[["dt"]] #grab data table from list output

# Plot distribution of HAQ for all locations
locs <- get_location_metadata(
  location_set_id = 35,
  release_id = model_version_id_metadata$release_id
)

# Produce HAQ Plots
# 1. HAQ distribution by super region
# 2. EMR input distribution vs HAQ faceted by age
plot_input_haq_data(
  data = dt,
  plot_output_path = plot_output_path,
  haq_data = haq,
  locs = locs
)

## Export results as csv file
data.table::fwrite(
  x = dt,
  file = paste0(date_output_path, cause_name, "_mrbrt_emr_", date, ".csv"),
  row.names = F
)

### 2) Link and launch a MR-BRT model --------------------------------------

# MR-BRT specs
knot_number <- 4
spline_type <- 3L
spline_tail <- T
knot_placement <- "frequency"

#Run model
dt <- dt %>%
  dplyr::mutate(row_id = row_number())

dat1 <- mr$MRData()
dat1$load_df(
  data = dt,
  col_obs = "log_ratio",
  col_obs_se = "delta_log_se",
  col_covs = list("midage", "sex_binary", "haqi_mean", "row_id"),
  col_study_id = "nid"
)

mod1 <- mr$MRBRT(
  data = dat1,
  cov_models = list(
    mr$LinearCovModel(
      "intercept",
      use_re = TRUE
    ),
    mr$LinearCovModel(
      "midage",
      use_re = FALSE,
      use_spline = TRUE,
      spline_degree = spline_type,
      spline_knots = array(seq(0, 1, length.out = knot_number)),
      spline_knots_type = knot_placement,
      spline_r_linear = spline_tail,
      spline_l_linear = spline_tail
    ),
    mr$LinearCovModel("sex_binary", use_re = FALSE),
    mr$LinearCovModel(
      "haqi_mean",
      use_re = FALSE,
      prior_beta_uniform = rbind(c(-Inf), c(0))
    )
  ),
  inlier_pct = 1 - trim
)

mod1$fit_model()

## Save fitted model
py_save_object(
  object=mod1,
  filename=file.path(paste0(date_output_path, "mrbrt-model-object.pkl")),
  pickle="dill"
)

# Add weights
df_mod <- cbind(mod1$data$to_df(), data.frame(w = mod1$w_soln))
dt <- merge(dt, df_mod %>% select(row_id, w), by = "row_id", all.x = TRUE)

# custom function to create map from emr input data
map_emr_input(train_data = dt)

# Identify and write out missing locations from the map
identify_missing_locations_from_map(
  dt = dt,
  loc_hier = locs,
  output_path = date_output_path
)

### 3) Run predictions and plot MR-BRT ----------------------------------------

## Set up matrix for predictions for specified ages (age_predict variable),
## male/female, all national and subnational locations, all Dismod years
loc_pull <- locs$location_id[locs$level == 3 | locs$level == 4]

pred_df <- expand.grid(
  year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022),
  location_id = loc_pull,
  midage = age_predict,
  sex_binary = c(0, 1)
)

pred_df <- left_join(
  pred_df,
  haq[, c("haqi_mean", "location_id", "year_id")],
  by = c("location_id", "year_id")
)

# pred_df <- na.omit(pred_df)

if (any(is.na(pred_df$haqi_mean))) {
  stop("NA values in haqi_mean. Check that all locations and years have HAQ data.")
}

pred_df$age_start <- pred_df$midage
pred_df$age_end <- pred_df$midage

## Predict mr brt
## Create the mrdata object to predict on
dat_pred2 <- mr$MRData()
dat_pred2$load_df(
  data = pred_df,
  col_covs = list("midage", "sex_binary", "haqi_mean")
)
pred_df$pred1 <- mod1$predict(data = dat_pred2)

# Make vetting plots of the predictions vs each covariate.

f_pred_midage <- ggplot(data = pred_df, aes(x = midage, y = pred1)) +
  facet_wrap(vars(year_id)) +
  geom_point(alpha = 0.1) +
  cowplot::theme_minimal_grid()

f_pred_sex <- ggplot(data = pred_df, aes(x = factor(sex_binary), y = pred1)) +
  facet_wrap(vars(year_id)) +
  geom_point(alpha = 0.1) +
  cowplot::theme_minimal_grid()

f_pred_haqi <- ggplot(data = pred_df, aes(x = haqi_mean, y = exp(pred1))) +
  facet_wrap(vars((age_end + age_start) / 2)) +
  geom_point(alpha = 0.1) +
  cowplot::theme_minimal_grid()

cowplot::save_plot(
  plot = f_pred_midage,
  filename = paste0(plot_output_path, "mrbrt-pred-plot-midage.pdf"),
  base_height = 5
)
cowplot::save_plot(
  plot = f_pred_sex,
  filename = paste0(plot_output_path, "mrbrt-pred-plot-sex.pdf"),
  base_height = 5
)
cowplot::save_plot(
  plot = f_pred_haqi,
  filename = paste0(plot_output_path, "mrbrt-pred-plot-haqi.pdf"),
  base_height = 5
)

# Plot MR-BRT model results overlayed on EMR input data
graph_emr <- graph_mrbrt_results(
  results = dt,
  predicts = pred_df,
  space = "normal"
)

ggsave(
  graph_emr,
  filename = paste0(plot_output_path, "fit_plot_normal.pdf"),
  width = 10, height = 7
)

graph_emr_log <- graph_mrbrt_results(
  results = dt,
  predicts = pred_df,
  space = "log"
)

ggsave(
  graph_emr_log,
  filename = paste0(plot_output_path, "fit_plot_log.pdf"),
  height = 7, width = 10
)


### 4) Apply ratios to the orginal data ---------------------------------------
# Create uncertainty and append mean, lower, upper onto pred_df
n_samples <- as.integer(100)
samples1 <- mod1$sample_soln(
  sample_size = n_samples
)
draws1 <- mod1$create_draws(
  data = dat_pred2,
  beta_samples = samples1[[1]],
  gamma_samples = matrix(
    data = mod1$gamma_soln,
    nrow = n_samples,
    ncol = length(mod1$gamma_soln),
    byrow = TRUE # 'byrow = TRUE' is important to include
  ),
  random_study = FALSE
)
pred_df <- as.data.table(pred_df)
pred_df[, log_mean := mod1$predict(data = dat_pred2)]
pred_df[, log_lower := apply(draws1, 1, function(x) quantile(x, 0.025))]
pred_df[, log_upper := apply(draws1, 1, function(x) quantile(x, 0.975))]

final_emr <- copy(
  pred_df[, c(
    "year_id",
    "location_id",
    "sex_binary",
    "age_start",
    "age_end",
    "pred1",
    "log_lower",
    "log_upper",
    "log_mean"
  )]
)

setnames(final_emr, "pred1", "emr")

# Create beta summary
beta_samples_log <- as.data.table(samples1[[1]])
setnames(beta_samples_log, names(beta_samples_log), names(mod1$summary()[[1]]))
beta_samples_log <- melt.data.table(
  beta_samples_log,
  variable.name = "covariate",
  value.name = "beta_sample_val"
)
beta_samples_lin <- copy(beta_samples_log)
beta_samples_lin[, beta_sample_val := exp(beta_sample_val)]
beta_samples_log[, scale := "log"]
beta_samples_lin[, scale := "lin"]
beta_samples <- rbind(beta_samples_log, beta_samples_lin)
beta_summary <- beta_samples[
  ,
  .(
    mean = mean(beta_sample_val),
    lower = quantile(beta_sample_val, 0.025),
    upper = quantile(beta_sample_val, 0.975)
  ),
  by = c("covariate", "scale")
]
data.table::fwrite(
  x = beta_summary,
  file = paste0(date_output_path, date, cause_name, "mrbrt-beta-summary", ".csv"),
  row.names = F
)

df_betaplot <- beta_summary[scale == "log"]
f_betas <- ggplot(data = df_betaplot, aes(x = mean, y = covariate)) +
  geom_vline(xintercept = 0, color = "#999999") +
  geom_pointrange(
    aes(xmin = lower, xmax = upper),
    color = "#636efa",
    alpha = 0.9
  ) +
  theme_ihme_light(axes = "x", grid = "y")

cowplot::save_plot(
  plot = f_betas,
  filename = paste0(plot_output_path, "mrbrt-beta-plot.pdf"),
  base_width = 5,
  base_height = 4
)


# Make a plot that will show the fit datapoints and overlay the model fit
df_fitplot <- copy(pred_df)
df_draws <- as.data.table(draws1)
rand_cols <- sample(1:ncol(df_draws), 10)
df_draws <- df_draws[, ..rand_cols]
df_fitplot[, midage_label := factor(
  paste0("Midage = ", midage),
  labels = paste0("Midage = ", unique(df_fitplot$midage))
)]
df_fitplot[sex_binary == 1, sex_label := "F"]
df_fitplot[sex_binary == 0, sex_label := "M"]
df_predplot <- cbind(df_fitplot, df_draws)
df_predplot <- melt.data.table(
  df_predplot,
  id.vars = c(
    "year_id",
    "location_id",
    "midage",
    "sex_binary",
    "haqi_mean",
    "age_start",
    "age_end",
    "pred1",
    "log_mean",
    "log_lower",
    "log_upper",
    "midage_label",
    "sex_label"
  ),
  value.name = "pred_draw_value",
  variable.name = "pred_draw"
)

f_predvet_log <- ggplot(
  data = df_fitplot,
  aes(x = haqi_mean, y = log_mean)
) +
  facet_wrap(vars(midage_label)) +
  geom_point(
    data = df_predplot, aes(y = pred_draw_value, color = sex_label),
    alpha = 0.2
  ) +
  geom_line(aes(color = sex_label)) +
  geom_ribbon(
    aes(ymin = log_lower, ymax = log_upper, fill = sex_label),
    alpha = 0.2
  ) +
  xlim(0, 100) +
  scale_color_birds("plotly_fm") +
  scale_fill_birds("plotly_fm") +
  theme_ihme(axes = "none", grid = "both", font_size = 20) +
  ggplot2::theme(
    panel.background = ggplot2::element_rect(fill = "white", color = NA),
    plot.background = ggplot2::element_rect(fill = "white", color = NA),
  ) +
  labs(
    x = "HAQI mean", y = "Log fit mean",
    subtitle = "MR-BRT model fit mean and uncertainty for EMR over HAQI"
  )

f_predvet <- ggplot(
  data = df_fitplot,
  aes(x = haqi_mean, y = exp(log_mean))
) +
  facet_wrap(vars(midage_label)) +
  geom_point(
    data = df_predplot, aes(y = exp(pred_draw_value), color = sex_label),
    alpha = 0.2
  ) +
  geom_line(aes(color = sex_label)) +
  geom_ribbon(
    aes(ymin = exp(log_lower), ymax = exp(log_upper), fill = sex_label),
    alpha = 0.2
  ) +
  xlim(0, 100) +
  scale_color_birds("plotly_fm") +
  scale_fill_birds("plotly_fm") +
  theme_ihme(axes = "none", grid = "both", font_size = 20) +
  theme(
    panel.background = element_rect(fill = "white", color = NA),
    plot.background = element_rect(fill = "white", color = NA),
  ) +
  labs(
    x = "HAQI mean", y = "Log fit mean",
    subtitle = "MR-BRT model fit mean and uncertainty for EMR over HAQI"
  )

cowplot::save_plot(
  plot = f_predvet_log,
  filename = paste0(plot_output_path, "mrbrt-fit-plot-haqi-log.png"),
  base_width = 9,
  base_height = 5
)

cowplot::save_plot(
  plot = f_predvet,
  filename = paste0(plot_output_path, "mrbrt-fit-plot-haqi.png"),
  base_width = 9,
  base_height = 5
)

final_dt <- as.data.table(final_emr)

# Function that will apply the ratios to the data and reformat for upload
final_dt <- apply_mrbrt_ratios_and_format(
  data = final_dt,
  seq_of_dummy_emr = seq_emr,
  extractor = user_name
)

# Make more vetting plots of the upload sheet, as a final check
f_up_dens <- ggplot(data = final_dt, aes(x = mean)) +
  geom_histogram(
    bins = 20,
    color = "black",
    fill = "steelblue"
  ) +
  cowplot::theme_minimal_hgrid() +
  labs(
    x = "Mean EMR", y = "Count",
    subtitle = "Distribution of the modeled EMR means."
  )

cowplot::save_plot(
  plot = f_up_dens,
  filename = paste0(plot_output_path, "emr-means-density.pdf"),
  base_height = 4
)

### 5) Pull in crosswalk version of choice to append EMR data -----------------
xwalk_dt <- get_crosswalk_version(crosswalk_version_id, export = FALSE)
final_combined <- rbindlist(list(xwalk_dt, final_dt), fill = TRUE)

final_combined[!is.na(crosswalk_parent_seq), seq := NA]

# try deleting seq values for all incidence data and just keeping crosswalk_parent_seq
final_combined[measure == 'incidence', seq := NA]


## This is the dataset with specified xwalk version and predicted EMR to be used for nonfatal modeling
write.xlsx(final_combined, upload_datapath,  sheetName = "extraction")

### 5) Save crosswalk version -------------------------------------------------

data_filepath <- upload_datapath
description <- paste0("Predicted EMR prepared on ", date, " appended to xwalk version ID ", crosswalk_version_id)

result <- save_crosswalk_version(bundle_version_id=bundle_version_id,
                                 data_filepath=data_filepath,
                                 description=description)

print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))


## Run a dismod model!! :)
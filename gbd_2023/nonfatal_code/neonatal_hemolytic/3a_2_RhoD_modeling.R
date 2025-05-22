################################################################################
## Purpose:   Model for RhoD to live births proportion against
##            haqi covariate
## Input:     RhoD sales from Marketing Research Bureau (NID:539703)
##            Bundle ID: 4715, Bundle Version ID: 43365
## Output:    FILEPATH
################################################################################

# set up -----------------------------------------------------------------------
library('openxlsx')
library('data.table')
library('ggplot2')
library(dplyr)
library('msm')

params_global <- readr::read_rds("params_global.rds")
folder_path <- "FILEPATH"

haqi <-
  ihme::get_covariate_estimates(nch::id_for("covariate", "Healthcare access and quality index"),
                                release_id = params_global$release_id)[, c("mean_value", "location_id", "year_id")] %>%
  dplyr::rename(haqi_mean = mean_value) %>%
  data.table::setDT(haqi)

locs <- ihme::get_location_metadata(
  location_set_id = 35,
  release_id = params_global$release_id,
)

# set up mrbrt -----------------------------------------------------------------
library(reticulate, lib.loc = "FILEPATH")
Sys.setenv("RETICULATE_PYTHON" = "FILEPATH")
library(mrbrt003, lib.loc = "FILEPATH")
mr <- import("mrtool") # load mrbrt tool

# rhod to live births proportions ----------------------------------------------

rhod_livebirths <- fread(
  fs::path(
    "FILEPATH"
  )
)

# drop estimates
rhod_livebirths <- rhod_livebirths[Estimate=="No"]

# keep necessary columns
rhod_livebirths <- rhod_livebirths[,.(location_id, year_id, rhod_livebirths_prop, nid)]

# set up mean, sd and se -------------------------------------------------------

setDT(rhod_livebirths)

df <- rhod_livebirths %>%
  rename(mean = rhod_livebirths_prop)

# calculate standard error - we are using confidence level of 95% and confidence bounds being 15% from the mean
# standard deviation = (0.15 * mean) / 1.96
# standard error = SD / sqrt(n)
# assuming n is 500
df <- df %>%
  mutate(sd = (mean * .15) / 1.96) %>%
  mutate(standard_error = sd / (sqrt(500)))

# append haqi ------------------------------------------------------------------
df <- merge(
  df,
  haqi,
  by = c('location_id', 'year_id'),
  all.x = TRUE,
  all.y = FALSE
)

# append locations -------------------------------------------------------------
df <- merge(
  df,
  locs[,.(location_id, location_name, region_name, super_region_name)],
  by = "location_id",
  all.x = TRUE,
  all.y = FALSE
)

# logit transform --------------------------------------------------------------

df$mean_logit <- nch::logit(df$mean)
df$se_logit <- nch::logit_se(df$mean, df$standard_error)

# check data -------------------------------------------------------------------
ggplot() +
  geom_point(data = df, aes(x = haqi_mean, y = mean_logit, color = super_region_name)) +
  xlab("HAQI") +
  ylab("RhoD to live births") +
  ggtitle("Logit mean of RhoD to live births on haqi by super region") +
  theme_minimal()

ggplot() +
  geom_point(data = df, aes(x = haqi_mean, y = mean_logit, color = region_name)) +
  xlab("HAQI") +
  ylab("RhoD to live births") +
  ggtitle("Logit mean of RhoD to live births on haqi by region") +
  theme_minimal()

# outlier ----------------------------------------------------------------------
df <- df %>%
  dplyr::filter(!(location_name == "Botswana")) %>%
  dplyr::filter(!(location_name == "China"))

# load mrbrt data --------------------------------------------------------------
# using location_id_year_id as the col_study_id
data <- df %>%
  dplyr::mutate(study_id = paste0(location_id, "_", year_id))

dat <- mr$MRData()
dat$load_df(
  data = data,
  col_obs = "mean_logit",
  col_obs_se = "se_logit",
  col_covs = list("haqi_mean"), #"year_id"),
  col_study_id = "study_id"
)

# optimize parameter values and specify covariates to be used in model----------
mod <- mr$MRBRT(
  data = dat,
  cov_models = list(
    mr$LinearCovModel("intercept", use_re = TRUE), #random effects should be estimated
    mr$LinearCovModel(
      alt_cov = "haqi_mean",
      use_spline = TRUE,
      prior_spline_monotonicity = "increasing", #increasing monotonicity prior
      spline_knots_type = 'frequency'
    )
    #mr$LinearCovModel(
    # alt_cov = "year_id"
  )
)

# begin process of optimizing parameter values ---------------------------------
mod$fit_model(inner_print_level = 3L, inner_max_iter = 50000L)

# extract information from a fitted model --------------------------------------
mod$summary()

# saving and loading model objects ---------------------------------------------
py_save_object(object = mod,
               filename = file.path("FILEPATH"), pickle = "dill")
mod <- py_load_object(filename = file.path("FILEPATH"), pickle = "dill")

# cascading spline -------------------------------------------------------------
mod_cascade <- run_spline_cascade(
  stage1_model_object = mod,
  df = data,
  col_obs = "mean_logit", col_obs_se = "se_logit", col_study_id = "study_id",
  stage_id_vars = c("super_region_name", 'region_name'),
  thetas = c(1, 5),
  output_dir = folder_path,
  model_label = "mod_cascade",
  gaussian_prior = TRUE,
  overwrite_previous = TRUE
)

# make predictions #############################################################
# create prediction frame
df_pred <- merge(
  haqi,
  locs[,.(location_id, location_name, super_region_name, region_name)],
  by = c("location_id"),
  all = TRUE
)
df_pred <- df_pred[year_id %in% params_global$year_id]
df_pred <- df_pred[location_id %in% params_global$location_id]

# create general prediction frame ----------------------------------------------
# List all pickle files in the folder
file_mapping <- list.files(path = paste0(folder_path, "/mod_cascade/pickles"))
file_mapping <- file_mapping[file_mapping != "stage1__stage1.pkl"]
file_mapping <- as.data.table(file_mapping) |>
  rename(file_name = file_mapping)
file_mapping$region <- NA
file_mapping$super_region <- NA
file_mapping$region <- ifelse(
  grepl("^region", file_mapping$file_name),
  sub(".*__(.*)\\.pkl$", "\\1", file_mapping$file_name),
  file_mapping$region
)
file_mapping$super_region <- ifelse(
  grepl("^super", file_mapping$file_name),
  sub(".*__(.*)\\.pkl$", "\\1", file_mapping$file_name),
  file_mapping$super_region
)

# draws for region level fit ---------------------------------------------------
# set up for creating draws
dat_pred1 <- mr$MRData()
n_samples1 <- as.integer(1000)

# prediction frame for region fit
pickle_regions <- unique(na.omit(file_mapping$region))
region_subsets <- list()
for(i in pickle_regions) {
  region_subsets[[gsub(" ", "_", i)]] <- df_pred[region_name == i, ]
}

region_draws <- list()
pred_pt <- "pred_pt"
pred_lo <- "pred_lo"
pred_up <- "pred_up"

for(region_i in names(region_subsets)){

  dat_pred1$load_df(
    data = region_subsets[[region_i]],
    col_covs=list("haqi_mean")
  )

  cascade_fit1 <- py_load_object(
    filename = paste0(
      "FILEPATH"
    )
  )

  samples1 <- cascade_fit1$sample_soln(
    sample_size = n_samples1
  )

  draws <- cascade_fit1$create_draws(
    data = dat_pred1,
    beta_samples = samples1[[1]],
    gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
    random_study = FALSE,
    sort_by_data_id = TRUE)

  prediction <- cascade_fit1$predict(data = dat_pred1)

  region_subsets[[region_i]][, (pred_pt) := prediction]
  region_subsets[[region_i]][, (pred_lo) := apply(draws, 1, function(x) quantile(x, 0.025))]
  region_subsets[[region_i]][, (pred_up) := apply(draws, 1, function(x) quantile(x, 0.975))]

  colnames(draws) <- unlist(lapply(0:999, function(x) paste('draw', x, sep = "_")))

  draws <- cbind(region_subsets[[region_i]], draws)

  region_draws[[region_i]] <- as.data.table(draws)

}

combined_region_draws <- bind_rows(region_draws)

# draws for super region level fit ---------------------------------------------
# set up for creating draws
dat_pred1 <- mr$MRData()
n_samples1 <- as.integer(1000)
pickle_superregions <- unique(na.omit(file_mapping$super_region))
# below we're isolating the countries that don't have regional fits to map to super region fits
df_pred_superregion <- df_pred[super_region_name %in% pickle_superregions &
                                 !region_name %in% pickle_regions, ]

# prediction frame for super region fit
superregion_subsets <- list()
for(i in df_pred_superregion$super_region_name) {
  superregion_subsets[[gsub(" ", "_", i)]] <- df_pred_superregion[super_region_name == i, ]
}

superregion_draws <- list()
pred_pt <- "pred_pt"
pred_lo <- "pred_lo"
pred_up <- "pred_up"

for(superregion_i in names(superregion_subsets)){

  dat_pred1$load_df(
    data = superregion_subsets[[superregion_i]],
    col_covs=list("haqi_mean")
  )

  cascade_fit1 <- py_load_object(
    filename = paste0(
      "FILEPATH",
      gsub("_", " ", superregion_i),
      ".pkl"
    )
  )

  samples1 <- cascade_fit1$sample_soln(
    sample_size = n_samples1
  )

  draws <- cascade_fit1$create_draws(
    data = dat_pred1,
    beta_samples = samples1[[1]],
    gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
    random_study = FALSE,
    sort_by_data_id = TRUE)

  prediction <- cascade_fit1$predict(data = dat_pred1)

  superregion_subsets[[superregion_i]][, (pred_pt) := prediction]
  superregion_subsets[[superregion_i]][, (pred_lo) := apply(draws, 1, function(x) quantile(x, 0.025))]
  superregion_subsets[[superregion_i]][, (pred_up) := apply(draws, 1, function(x) quantile(x, 0.975))]

  colnames(draws) <- unlist(lapply(0:999, function(x) paste('draw', x, sep = "_")))

  draws <- cbind(superregion_subsets[[superregion_i]], draws)

  superregion_draws[[superregion_i]] <- as.data.table(draws)

}

combined_superregion_draws <- bind_rows(superregion_draws)

# combine all draws for every location -----------------------------------------
rhod_livebirths_draws <- rbind(combined_region_draws, combined_superregion_draws)

# transform predictions into linear space --------------------------------------
rhod_livebirths_draws_linear <- rhod_livebirths_draws %>%
  mutate(across(starts_with("draw_"), ~exp(.) / (1 + exp(.)))) %>%
  mutate(pred_pt = (exp(pred_pt)/(1 + exp(pred_pt))),
         pred_lo = (exp(pred_lo)/(1 + exp(pred_lo))),
         pred_up = (exp(pred_up)/(1 + exp(pred_up)))
  )

# save draws csv ---------------------------------------------------------------
data.table::fwrite(
  rhod_livebirths_draws_linear,
  fs::path(
    "FILEPATH"
  )
)

# PLOT RESULTS -----------------------------------------------------------------
location = ""
ggplot(
  data = rhod_livebirths_draws_linear[location_name == location],
  aes(
    x = year_id,
    y = pred_pt,
    ymin = pred_lo,
    ymax = pred_up,
    color = location,
    fill = location
  )) +
  geom_ribbon(alpha = 0.2, color = NA) +
  geom_line() +
  theme_minimal() +
  labs(x = "Year",
       y = "RhoD to live births ratio",
       title = paste("RhoD to live births - ", location)
  )

ggplot(
  data = preds[region_name == "Eastern Europe"],
  aes(x = haqi_mean, y = pred, color = location_name)) +
  #geom_point(alpha = .7) +
  geom_point(data = preds[preds$region_name == "Eastern Europe", ], aes(x = haqi_mean, y = pred)) +
  ggtitle("Eastern Europe: logit mean RhoD to livebirths against haqi - thetas(superregion: 1, region: 5)") +
  theme_minimal()

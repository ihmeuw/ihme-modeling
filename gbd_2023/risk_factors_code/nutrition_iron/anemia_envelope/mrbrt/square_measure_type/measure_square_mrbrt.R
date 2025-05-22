
# setup environment -------------------------------------------------------

library(reticulate)
use_python("FILEPATH")
mr <- import("mrtool")

library(data.table)
library(ggplot2)
library(stringr)

# get cluster arguments ---------------------------------------------------

if(interactive()){
  task_id <- 89
  map_file_name <- file.path(getwd(), "mrbrt/square_measure_type/config_map.csv")
  output_dir <- "FILEPATH"
}else{
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  command_args <- commandArgs(trailingOnly = TRUE)
  map_file_name <- command_args[1]
  output_dir <- command_args[2]
}

message('Task ID: ', task_id)

config_map <- fread(map_file_name)
config_map[config_map == ""] <- NA

ref_var <- config_map$ref[task_id]
alt_var <- config_map$alt[task_id]
model_anemia_category <- config_map$anemia_category[task_id]

# load in elevation data --------------------------------------------------

df <- rbindlist(lapply(
  list.files(
    path = "FILEPATH",
    full.names = TRUE
  ),
  fread
), use.names = TRUE, fill = TRUE)

df[df==""] <- NA
df <- df[!(is.na(ihme_loc_id))]

# add id column -----------------------------------------------------------

df[, id := .GRP, .(nid)]
df <- df[order(id)]
df$id <- as.character(df$id)

# split and match data ----------------------------------------------------

ref_df <- df[var==ref_var]
alt_df <- df[var==alt_var]

merge_cols <- c(
  "nid",
  "survey_name",
  "ihme_loc_id",
  "year_start",
  "year_end",
  "survey_module",
  "file_path",
  "id",
  "age_start",
  "age_end",
  "sex_id",
  "cv_pregnant",
  "sample_size",
  "nstrata",
  "nclust"
)

merge_df <- merge.data.table(
  x = ref_df,
  y = alt_df,
  by = merge_cols,
  suffixes = c('.ref', '.alt')
)

merge_df <- unique(merge_df)

# log transform mean and SE -----------------------------------------------

if(grepl("hemog", ref_var)){
  # offset_value_min <- merge_df |>
  #   dplyr::filter(mean.alt > 0) |>
  #   purrr::chuck('mean.alt') |>
  #   min() / 2
  # offset_value_max <- (1 - merge_df |>
  #   dplyr::filter(mean.alt < 1) |>
  #   purrr::chuck('mean.alt') |>
  #   max()) / 2
  # 
  # merge_df <- merge_df |>
  #   dplyr::mutate(
  #     mean.alt = dplyr::case_when(
  #       mean.alt >= 1 ~ (1 - offset_value_max),
  #       mean.alt <= 0 ~ offset_value_min,
  #       .default = mean.alt
  #     )
  #   )
  
  merge_df <- merge_df[
    !(is.na(mean.ref)) &
      !(is.na(mean.alt)) &
      !(is.na(standard_error.ref)) &
      !(is.na(standard_error.alt)) &
      mean.ref > 25 & mean.ref < 250 &
      mean.alt > 0 & mean.alt < 1 &
      standard_error.ref > 0.0001 &
      standard_error.alt > 0.0001
  ]
  
  #merge_df$mean.ref <- log(merge_df$mean.ref)
  merge_df$logit_mean_prev <- nch::logit(merge_df$mean.alt)
  merge_df$logit_se_prev <- nch::logit_se(
    mean_vec = merge_df$mean.alt,
    se_vec = merge_df$standard_error.alt
  )
  #merge_df$log_exposure <- log(merge_df$mean.ref)
}else{
  merge_df <- merge_df[
    !(is.na(mean.ref)) &
      !(is.na(mean.alt)) &
      !(is.na(standard_error.ref)) &
      !(is.na(standard_error.alt)) &
      mean.alt > 25 & mean.alt < 250 &
      mean.ref > 0 & mean.ref < 1 &
      standard_error.ref > 0.0001  &
      standard_error.alt > 0.0001
  ]
  
  #merge_df$mean.ref <- nch::logit(merge_df$mean.ref)
  merge_df$logit_mean_prev <- log(merge_df$mean.alt)
  merge_df$logit_se_prev <- nch::log_se(
    mean_vec = merge_df$mean.alt,
    se_vec = merge_df$standard_error.alt
  )
}

# assign age groups and anemia categories ---------------------------------

age_df <- ihme::get_age_metadata(release_id = 16)
anemia_map <- fread(file.path(getwd(), "mrbrt/square_measure_type/anemia_map.csv"))

merge_df$age_end[merge_df$age_end == 1 & merge_df$age_start == 1] <- 2
merge_df$age_group_id <- NA_integer_
merge_df$temp_age_end <- merge_df$age_end - 0.0001 # just bump down age_end so age_group_id can be assigned
for(r in seq_len(nrow(age_df))){
  i_vec <- which(merge_df$age_start >= age_df$age_group_years_start[r] &
                   merge_df$temp_age_end < age_df$age_group_years_end[r])
  merge_df$age_group_id[i_vec] <- age_df$age_group_id[r]
}
merge_df$temp_age_end <- NULL

merge_df <- merge.data.table(
  x = merge_df,
  y = anemia_map,
  by = c("age_group_id", "sex_id", "cv_pregnant")
)

merge_df <- merge_df[anemia_category == model_anemia_category]

# diagnostics pre model ---------------------------------------------------

if(interactive()){
  plot_title <- if(grepl('hemog', ref_var)){
    "Mean Hemoglobin vs. Logit Anemia Prevalence"
  }else{
    "Anemia Prevalence vs. Log Mean Hemoglobin"
  }
  
  diagnostic_plot1 <- ggplot(merge_df,aes(x = mean.ref, y = logit_mean_prev)) +
    geom_point(aes(colour = standard_error.ref, size = logit_se_prev), alpha = 0.5) +
    scale_colour_gradientn(colours = terrain.colors(10)) +
    geom_smooth(method = "loess", se = FALSE)+
    theme_classic() +
    labs(title = plot_title,
         subtitle = paste(ref_var, alt_var, model_anemia_category, sep = " - "),
         x= if(grepl('hemog', ref_var)) "Mean Hemoglobin (g/L)" else 'Anemia Prevalence',
         y= if(grepl('hemog', ref_var)) "Logit Anemia Prevalence" else "Log Mean Hemoglobin (g/L)") +
    theme(axis.line = element_line(colour = "black"))
  
  plot(diagnostic_plot1)
}

# start mrbrt model -------------------------------------------------------

dat1 <- mr$MRData()

covariates <- list("mean.ref")

dat1$load_df(
  data = merge_df,  col_obs = "logit_mean_prev", col_obs_se = "logit_se_prev",
  col_covs = covariates, col_study_id = NULL) 

# set up covariates and splines
cov_list <- list()
#add intercept, use_re = F since we aren't using random intercepts on NID
cov_mods <- append(mr$LinearCovModel("intercept", use_re = F), cov_list)

# add the following covariates
# 1) all elevation categories (but not category 0, which will be used as the reference) without REs
# 2) the mean.ref anemia value with a decreasing spline
# knot_vec <- stringr::str_split(
#   string = config_map$knots[task_id],
#   pattern = ',',
#   simplify = TRUE
# ) |>
#   as.numeric()
# domain_knots <- bophf::get_spline_knots(
#   min_exposure = min(merge_df$mean.ref),
#   max_exposure = max(merge_df$mean.ref),
#   knot_value_vec = knot_vec
# )
# num_knots <- length(domain_knots)

knot_vec <- if(config_map$model_type[task_id] == 1){
  c(0, 0.1, 0.3, 0.7, 0.9, 1)
} else {
  c(0, 0.33, 0.66, 1)
}
num_knots <- length(knot_vec)

last_prior <- config_map$last_prior[task_id]
cov_mods <- append(lapply(covariates, function(x){
  if(x == "mean.ref"){
    mr$LinearCovModel(
      alt_cov = x,
      use_spline = TRUE,
      spline_knots = knot_vec,
      spline_degree = 2L,
      spline_knots_type = 'frequency',
      spline_r_linear = config_map$r_linear[task_id],
      spline_l_linear = config_map$l_linear[task_id],
      prior_spline_monotonicity = unlist(ifelse(is.na(config_map$monotinicity[task_id]), list(NULL), list(config_map$monotinicity[task_id]))),
      prior_spline_convexity = unlist(ifelse(is.na(config_map$convexity[task_id]), list(NULL), list(config_map$convexity[task_id]))),
      prior_spline_maxder_gaussian = rbind(rep(0, num_knots - 1), c(rep(0.1, num_knots - 2), last_prior))
      #prior_spline_maxder_gaussian = NULL
    )
  }else{
    mr$LinearCovModel(x,use_re = F)
  }
}),cov_mods)

# run mr-brt model --------------------------------------------------------

reticulate::py_set_seed(123)

fit1 <- mr$MRBRT(
  data = dat1,
  cov_models = cov_mods,
  #inlier_pct = if(config_map$model_type[task_id] == 1) 0.95 else 1
  inlier_pct = 1
)

fit1$fit_model(inner_print_level = 3L, inner_max_iter = 50000L, outer_tol = 0.0001)

# assess mr-brt output ----------------------------------------------------

dat_pred <- mr$MRData()
dat_pred$load_df(data = merge_df, col_covs = covariates, col_study_id = NULL)

merge_df$pred_logit_mean <- fit1$predict(dat_pred, predict_for_study = F)

# diagnostics post model --------------------------------------------------

if(interactive()){
  model_plot1 <- ggplot(merge_df,aes(x = mean.ref)) +
    geom_point(aes(y = logit_mean_prev, colour = standard_error.ref, size = logit_se_prev), alpha = 0.5) +
    scale_colour_gradientn(colours = terrain.colors(10)) +
    geom_line(aes(y = pred_logit_mean), colour = 'blue', linewidth = 1) +
    theme_classic() +
    labs(title = paste("Mean Hemoglobin vs. Logit Anemia Prevalence"),
         subtitle = paste(ref_var, alt_var, sep = " - "),
         x="Mean Hemoglobin (g/L)",
         y="Logit Anemia Prevalence") +
    theme(axis.line = element_line(colour = "black"))
  
  plot(model_plot1)
}

# create draws for FE only ------------------------------------------------

n_samples1 <- as.integer(100)

samples1 <- fit1$sample_soln(
  sample_size = n_samples1
)

draws1 <- fit1$create_draws(
  data = dat_pred,
  beta_samples = samples1[[1]],
  gamma_samples = matrix(rep(0, n_samples1 * fit1$num_z_vars), ncol = fit1$num_z_vars),
  random_study = FALSE 
) 

merge_df$pred1_lo <- apply(draws1, 1, function(x) quantile(x, 0.025))
merge_df$pred1_up <- apply(draws1, 1, function(x) quantile(x, 0.975))
merge_df$draw_se <- (merge_df$pred1_up - merge_df$pred1_lo)/(qnorm(0.975)*2)

# write out data ----------------------------------------------------------

file_name <- file.path(
  output_dir,
  "csv",
  paste0(
    task_id, "_",
    ref_var, "-",
    alt_var, "-",
    model_anemia_category, ".csv"
  )
)
fwrite(
  x = merge_df,
  file = file_name
)

file_name_pkl <- file.path(
  output_dir,
  "pkl",
  paste0(
    task_id, "_",
    ref_var, "-",
    alt_var, "-",
    model_anemia_category, ".pkl"
  )
)
reticulate::py_save_object(
  object = fit1, 
  filename = file_name_pkl, 
  pickle = "dill"
)

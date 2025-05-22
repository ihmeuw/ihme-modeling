
# source libraries --------------------------------------------------------

source(file.path(getwd(), 'mrbrt/age_sex_split/mrbrt_cascade_functions_v1.R'))
library(data.table)
library(ggplot2)
library(stringr)


box::use(
  mlh = ./map_location_hierarchy
)

# load in job metadata ----------------------------------------------------

if(interactive()){
  current_var <- 'anemia_anemic_who'
  current_sex <- 1
  current_preg <- 0
}else{
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  command_args <- commandArgs(trailingOnly = TRUE)
  map_file_name <- command_args[1]

  param_map <- fread(map_file_name)
  current_var <- param_map$var[task_id]
  current_sex <- param_map$sex_id[task_id]
  current_preg <- param_map$cv_pregnant[task_id]
  
  message(paste(task_id, current_var, current_sex, current_preg))
}

# load in collapsed microdata and subset to current var -------------------

df <- rbindlist(lapply(
  list.files(
    path = "FILEPATH",
    full.names = TRUE
  ),
  fread
))

# global outlier ----------------------------------------------------------

df <- df[var == current_var & sex_id == current_sex & cv_pregnant == current_preg]
df[df==""] <- NA

message(paste("Nrow df =", nrow(df)))

# add id column -----------------------------------------------------------

df[, id := .GRP, .(nid)]
df <- df[order(id)]
df$id <- as.character(df$id)

# append on location_id ---------------------------------------------------

location_df <- ihme::get_location_metadata(
  location_set_id = 35,
  release_id = 16
)

hierarchy_col_names <- c(
  'global_id',
  'super_region_id',
  'region_id',
  'country_id',
  'subnat1_id',
  'subnat2_id',
  'subnat3_id'
)

df <- mlh$map_location_hierarchy(
  input_df = df,
  loc_df = location_df,
  hierarchy_col_names = hierarchy_col_names
)

# assign age group ids ----------------------------------------------------

age_df <- ihme::get_age_metadata(release_id = 16)

df$age_end[df$age_end == 1 & df$age_start == 1] <- 2
df$age_group_id <- NA_integer_
df$temp_age_end <- df$age_end - 0.0001 # just bump down age_end so age_group_id can be assigned
for(r in seq_len(nrow(age_df))){
  i_vec <- which(df$age_start >= age_df$age_group_years_start[r] &
                   df$temp_age_end < age_df$age_group_years_end[r])
  df$age_group_id[i_vec] <- age_df$age_group_id[r]
}
df$temp_age_end <- NULL
df$mid_age <- (df$age_start + df$age_end) / 2

# log transform mean and SE -----------------------------------------------

if(grepl("hemog", current_var)){
  df <- df[
    !(is.na(mean)) &
      !(is.na(standard_error)) &
      mean > 25 & mean < 250 
  ]
  
  df$transformed_mean_prev <- log(df$mean)
  df$transformed_se_prev <- nch::log_se(
    mean_vec = df$mean,
    se_vec = df$standard_error
  )
}else{
  df <- df[
    !(is.na(mean)) &
      !(is.na(standard_error)) 
  ]
  
  df$transformed_mean_prev <- nch::logit(df$mean)
  df$transformed_se_prev <- nch::logit_se(
    mean_vec = df$mean,
    se_vec = df$standard_error
  )
}

low_age_df <- df[age_start <= 2]
high_age_df <- df[age_start >= 2]

# diagnostics pre model ---------------------------------------------------

if(interactive()){
  age_cov_diagnostic <- ggplot(high_age_df,aes(x = age_start, y = transformed_mean_prev)) +
    geom_point(alpha = 0.5) +
    geom_smooth(method = "loess", se = FALSE) +
    theme_classic() +
    labs(title = paste("Age/sex patterns for", current_var, "-", current_sex),
         x="Age Start",
         y=if(grepl("hemog", current_var)) "Log Mean hemoglobin (g/L)" else "Anemia Prevalence") +
    theme(axis.line = element_line(colour = "black"))
  
  plot(age_cov_diagnostic)
}

# run mrbrt for 2+ years old ----------------------------------------------

dat1 <- MRData()

covariates <- list('age_start')

dat1$load_df(
  data = high_age_df,  col_obs = "transformed_mean_prev", col_obs_se = "transformed_se_prev",
  col_covs = covariates, col_study_id = "id" 
)

# set up covariates and splines
cov_list <- list()
#add intercept
cov_mods <- append(LinearCovModel("intercept", use_re = T), cov_list)

# add the age covariate

age_knot_vec <- switch(
  as.character(current_preg),
  '1' = c(25, 35),
  '0' = c(10, 15, 55, 70)
)

domain_knots <- bophf::get_spline_knots(
  min_exposure = min(high_age_df$age_start),
  max_exposure = max(high_age_df$age_start),
  knot_value_vec = age_knot_vec
)

last_prior <- 0.01

cov_mods <- append(lapply(covariates, function(x){
  if(x == "age_start"){
    LinearCovModel(
      alt_cov = x,
      use_spline = TRUE,
      spline_knots = domain_knots,
      spline_degree = 2L,
      spline_knots_type = 'domain',
      spline_r_linear = TRUE,
      spline_l_linear = FALSE,
      prior_spline_monotonicity = NULL,
      prior_spline_convexity = if(current_preg == 1) NULL else if(grepl('hemog', current_var)) 'concave' else 'convex',
      prior_spline_maxder_gaussian = rbind(
        rep(0, length(domain_knots) - 1), 
        c(rep(0.1, length(domain_knots) - 2), last_prior)
      )
    )
  }else{
    LinearCovModel(x,use_re = F)
  }
}),cov_mods)

# run mr-brt model --------------------------------------------------------

fit1 <- MRBRT(
  data = dat1,
  cov_models = cov_mods,
  inlier_pct = 1.0
)

fit1$fit_model(inner_print_level = 3L, inner_max_iter = 50000L, outer_tol = 0.00001)

# plot mr-brt outputs ------------------------------------------------------

if(interactive()){
  
  dat_pred <- MRData()
  dat_pred$load_df(data = high_age_df, col_covs = covariates)
  
  high_age_df$pred_mean <- fit1$predict(dat_pred, predict_for_study = FALSE)
  
  global_mrbrt_plot <- ggplot(high_age_df, aes(x = age_start)) +
    geom_point(aes(y = transformed_mean_prev)) + 
    geom_line(aes(y = pred_mean), colour = 'blue', linewidth = 1.5) +
    labs(
      title = paste0('Global MR-BRT Age Trends - ', current_var),
      subtitle = if(current_sex == 1) 'Males' else 'Females',
      x = 'Age Start',
      y = 'Log Transformed Mean'
    ) +
    theme_minimal()
  
  plot(global_mrbrt_plot)
}

# cascade by super region -------------------------------------------------

fit_super_region <- run_spline_cascade(
  stage1_model_object = fit1,
  df = high_age_df,
  col_obs = "transformed_mean_prev", 
  col_obs_se = "transformed_se_prev", 
  col_study_id = "id",
  stage_id_vars = c("super_region_id", 'region_id', 'country_id', 'subnat1_id', 'subnat2_id'),
  thetas = c(10, 8, 2, 1, 0.1),
  output_dir = "FILEPATH",
  model_label = paste0("age_sex_cascade_", current_var, "_", current_sex, "_", current_preg), 
  gaussian_prior = TRUE,
  overwrite_previous = TRUE
)

# plot cascade results ----------------------------------------------------

template_df <- CJ(
  ihme_loc_id = unique(high_age_df$ihme_loc_id),
  sex_id = unique(high_age_df$sex_id),
  age_start = unique(high_age_df$age_start)
) |> dplyr::left_join(
  y = high_age_df,
  by = colnames(.data)
) |>
  dplyr::group_by(ihme_loc_id) |>
  tidyr::fill(
    hierarchy_col_names[1:4], .direction = 'updown'
  ) |>
  dplyr::ungroup() |>
  setDT()

for(i in hierarchy_col_names[1:4]){
  new_col_name <- paste('pred_mean', i, sep = "_")
  template_df[[new_col_name]] <- NA_real_
  for(x in unique(template_df[[i]])){
    
    if(i == 'global_id'){
      dat_pred <- MRData()
      dat_pred$load_df(data = template_df, col_covs = list('age_start'))
      
      template_df[[new_col_name]] <- fit1$predict(dat_pred, predict_for_study = FALSE)
    }else{
      pkl_file_name <- file.path(
        'FILEPATH',
        paste0('age_sex_cascade_', paste(current_var, current_sex, current_preg, sep = "_")),
        '/pickles',
        paste0(i, "__", x, ".pkl")
      )
      
      if(file.exists(pkl_file_name)){
        i_vec <- which(template_df[[i]] == x)
        
        fit_obj <- reticulate::py_load_object(pkl_file_name)
        
        dat_pred <- MRData()
        dat_pred$load_df(data = template_df[i_vec, ], col_covs = list('age_start'))
        
        template_df[[new_col_name]][i_vec] <- fit_obj$predict(dat_pred, predict_for_study = FALSE)
      }
    }
  }
}

pdf_file_name <- file.path(
  "FILEPATH",
  paste0(
    paste("2_and_over", current_var, current_sex, current_preg, sep = "_"),
    '.pdf'
  )
)

pdf(pdf_file_name, width = 11, height = 8)

for(i in unique(template_df$country_id)){
  p <- ggplot(template_df[country_id == i & !(is.na(pred_mean_country_id))], aes(x = age_start)) +
    geom_point(aes(y = transformed_mean_prev), colour = 'black', alpha = 0.8, size = 2) +
    geom_line(aes(y = pred_mean_global_id), colour = 'blue') +
    geom_line(aes(y = pred_mean_super_region_id), colour = 'orange') +
    geom_line(aes(y = pred_mean_region_id), colour = 'red') +
    geom_line(aes(y = pred_mean_country_id), colour = 'green') +
    labs(
      title = paste0('Cascade MR-BRT Age Trends - ', current_var, ' - ', nch::name_for(type = 'location', id = i)),
      subtitle = if(current_sex == 1) 'Males' else if (current_sex == 2 && current_preg == 0) 'Females' else 'Pregnant Females',
      x = 'Age Start',
      y = 'Log Transformed Mean'
    ) +
    theme_minimal()
  
  plot(p)
}

dev.off()

# stop flow if cv_pregnant == 1 -------------------------------------------

if(current_preg == 1){
  stop('Not running model for <2 years old for pregnant status = TRUE.')
}

# run mrbrt for 2 years old and under -------------------------------------

low_age_df$sqrt_age_start <- sqrt(low_age_df$age_start)
low_age_df$log_age_mid <- log(low_age_df$mid_age)

if(grepl('hemog', current_var)){
  global_low_age_df <- low_age_df[age_start == 0 & transformed_mean_prev >= 4.85 | age_start > 0]
}else{
  global_low_age_df <- low_age_df[age_start == 0 & transformed_mean_prev < 1 | age_start > 0]
}

if(interactive()){
  age_cov_diagnostic <- ggplot(global_low_age_df,aes(x = if(grepl('hemog', current_var)) log_age_mid else sqrt_age_start, y = transformed_mean_prev)) +
    geom_point(alpha = 0.5) +
    geom_smooth(method = "loess", se = FALSE) +
    theme_classic() +
    labs(title = paste("Age/sex patterns for", current_var, "-", current_sex),
         x="Sqrt Age Start",
         y=if(grepl("hemog", current_var)) "Log Mean hemoglobin (g/L)" else "Anemia Prevalence") +
    theme(axis.line = element_line(colour = "black"))
  
  plot(age_cov_diagnostic)
}

dat1 <- MRData()

covariates <- list(if(grepl('hemog', current_var)) "log_age_mid" else 'sqrt_age_start')

dat1$load_df(
  data = global_low_age_df,  col_obs = "transformed_mean_prev", col_obs_se = "transformed_se_prev",
  col_covs = covariates, col_study_id = "id" 
)

# set up covariates and splines
cov_list <- list()
#add intercept
cov_mods <- append(LinearCovModel("intercept", use_re = T), cov_list)

domain_knots <- bophf::get_spline_knots(
  min_exposure = min(global_low_age_df$log_age_mid),
  max_exposure = max(global_low_age_df$log_age_mid),
  knot_value_vec = if(grepl('hemog', current_var)) -2.5 else c(0.5)
)

cov_mods <- append(lapply(covariates, function(x){
  if(x == if(grepl('hemog', current_var)) "log_age_mid" else 'sqrt_age_start'){
    LinearCovModel(
      alt_cov = x,
      use_spline = TRUE,
      spline_knots = domain_knots,
      spline_degree = 3L,
      spline_knots_type = 'domain',
      spline_r_linear = FALSE,
      spline_l_linear = FALSE,
      prior_spline_monotonicity = NULL,
      prior_spline_convexity = if(grepl('hemog', current_var)) 'convex' else NULL,
      prior_spline_maxder_gaussian = if(grepl('anemia', current_var)) rbind(
        c(0, 0),
        c(0.01, 0.1)
      )
    )
  }else{
    LinearCovModel(x,use_re = F)
  }
}),cov_mods)

# run mr-brt model --------------------------------------------------------

fit1 <- MRBRT(
  data = dat1,
  cov_models = cov_mods,
  inlier_pct = 1.0
)

fit1$fit_model(inner_print_level = 3L, inner_max_iter = 50000L, outer_tol = 0.00001)

# plot mr-brt outputs ------------------------------------------------------

if(interactive()){
  
  dat_pred <- MRData()
  dat_pred$load_df(data = low_age_df, col_covs = covariates)
  
  low_age_df$pred_mean <- fit1$predict(dat_pred, predict_for_study = FALSE)
  
  global_mrbrt_plot <- ggplot(low_age_df, aes(x = if(grepl('hemog', current_var)) log_age_mid else sqrt_age_start)) +
    geom_point(aes(y = transformed_mean_prev)) + 
    geom_line(aes(y = pred_mean), colour = 'blue', linewidth = 1.5) +
    labs(
      title = paste0('Global MR-BRT Age Trends - ', current_var),
      subtitle = if(current_sex == 1) 'Males' else 'Females',
      x = 'Log Age Midpoint',
      y = 'Log Transformed Mean'
    ) +
    theme_minimal()
  
  plot(global_mrbrt_plot)
  
  if(grepl('hemog', current_var)){
    template_df <- data.table(
      log_age_mid = seq(min(low_age_df$log_age_mid), max(low_age_df$log_age_mid), 0.1)
    )
  }else{
    template_df <- data.table(
      sqrt_age_start = seq(min(low_age_df$sqrt_age_start), max(low_age_df$sqrt_age_start), 0.1)
    )
  }
  dat_pred <- MRData()
  dat_pred$load_df(data = template_df, col_covs = covariates)
  
  template_df$pred_mean <- fit1$predict(dat_pred, predict_for_study = FALSE)
  
  global_mrbrt_plot <- ggplot(template_df, aes(x = if(grepl('hemog', current_var)) log_age_mid else sqrt_age_start)) +
    geom_line(aes(y = pred_mean), colour = 'blue', linewidth = 1.5) +
    labs(
      title = paste0('Global MR-BRT Age Trends - ', current_var),
      subtitle = if(current_sex == 1) 'Males' else 'Females',
      x = 'Log Age Midpoint',
      y = 'Log Transformed Mean'
    ) +
    theme_minimal()
  plot(global_mrbrt_plot)
}

# cascade by super region -------------------------------------------------

fit_super_region <- run_spline_cascade(
  stage1_model_object = fit1,
  df = global_low_age_df,
  col_obs = "transformed_mean_prev", 
  col_obs_se = "transformed_se_prev", 
  col_study_id = "id",
  stage_id_vars = c("super_region_id", 'region_id', 'country_id', 'subnat1_id', 'subnat2_id'),
  thetas = c(10, 8, 2, 1, 0.1),
  output_dir = "FILEPATH",
  model_label = paste0("age_sex_cascade_", current_var, "_", current_sex, "_", current_preg), 
  gaussian_prior = TRUE,
  overwrite_previous = TRUE
)

# plot cascade results ----------------------------------------------------

if(grepl('hemog', current_var)){
  template_df <- CJ(
    ihme_loc_id = unique(low_age_df$ihme_loc_id),
    sex_id = unique(low_age_df$sex_id),
    log_age_mid = unique(low_age_df$log_age_mid)
  ) 
}else{
  template_df <- CJ(
    ihme_loc_id = unique(low_age_df$ihme_loc_id),
    sex_id = unique(low_age_df$sex_id),
    sqrt_age_start = unique(low_age_df$sqrt_age_start)
  ) 
}

template_df <- template_df |>
  dplyr::left_join(
    y = low_age_df,
    by = colnames(.data)
  ) |>
  dplyr::group_by(ihme_loc_id) |>
  tidyr::fill(
    hierarchy_col_names[1:4], .direction = 'updown'
  ) |>
  dplyr::ungroup() |>
  setDT()

for(i in hierarchy_col_names[1:4]){
  new_col_name <- paste('pred_mean', i, sep = "_")
  template_df[[new_col_name]] <- NA_real_
  for(x in unique(template_df[[i]])){
    
    if(i == 'global_id'){
      dat_pred <- MRData()
      dat_pred$load_df(data = template_df, col_covs = covariates)
      
      template_df[[new_col_name]] <- fit1$predict(dat_pred, predict_for_study = FALSE)
    }else{
      
      pkl_file_name <- file.path(
        'FILEPATH',
        paste0('age_sex_cascade_', paste(current_var, current_sex, current_preg, sep = "_")),
        '/pickles',
        paste0(i, "__", x, ".pkl")
      )
      
      if(file.exists(pkl_file_name)){
        i_vec <- which(template_df[[i]] == x)
        
        fit_obj <- py_load_object(pkl_file_name)
        
        dat_pred <- MRData()
        dat_pred$load_df(data = template_df[i_vec, ], col_covs = covariates)
        
        template_df[[new_col_name]][i_vec] <- fit_obj$predict(dat_pred, predict_for_study = FALSE)
      }
    }
  }
}

pdf_file_name <- file.path(
  "FILEPATH",
  paste0(
    paste("under_2", current_var, current_sex, current_preg, sep = "_"),
    '.pdf'
  )
)

pdf(pdf_file_name, width = 11, height = 8)

for(i in unique(template_df$country_id)){
  p <- ggplot(template_df[country_id == i & !(is.na(pred_mean_country_id))], aes(x = if(grepl('hemog', current_var)) log_age_mid else sqrt_age_start)) +
    geom_point(aes(y = transformed_mean_prev), colour = 'black', alpha = 0.8, size = 2) +
    geom_line(aes(y = pred_mean_global_id), colour = 'blue') +
    geom_line(aes(y = pred_mean_super_region_id), colour = 'orange') +
    geom_line(aes(y = pred_mean_region_id), colour = 'red') +
    geom_line(aes(y = pred_mean_country_id), colour = 'green') +
    labs(
      title = paste0('Cascade MR-BRT Age Trends - ', current_var, ' - ', nch::name_for(type = 'location', id = i)),
      subtitle = if(current_sex == 1) 'Males' else if (current_sex == 2 && current_preg == 0) 'Females' else 'Pregnant Females',
      x = if(grepl('hemog', current_var)) 'Log of Age Mid Point' else 'Sqrt of Age Start',
      y = 'Transformed Mean'
    ) +
    theme_minimal()
  
  plot(p)
}

dev.off()

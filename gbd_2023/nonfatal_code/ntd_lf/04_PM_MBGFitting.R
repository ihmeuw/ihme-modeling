# Purpose: create MBG formula, add missing time data, and fit MBG model

############ setup pipeline, paths, parameters, etc. ############ 
user <- Sys.info()["user"]
cause <- "ntd_lf"
source("FILEPATH")

# setup preambles
## loads pipeline objects saved in the run_date/objects/node_id folder 
inputs <- if (interactive()) { # otherwise NULL
  list(
    run_date = run_date,
    indicator = indicator,
    indicator_group = indicator_group,
    node_id = 4,
    node_name = "j04_MBG_fitting",
    dag_path = sprintf("FILEPATH"),
    loopvar_index = 1)}
pipeline_preamble(inputs = inputs)

# Populate global variables tied to loopvar_index
assign("reg", pipeline$loopvars[loopvar_index, region])
assign("age", pipeline$loopvars[loopvar_index, age])
assign("holdout", pipeline$loopvars[loopvar_index, holdout])
assign("seed", pipeline$config_list$seed)

######################## prep data for fitting MBG model ######################## 
# bound GBM to 0-1 (necessary for rates, proportions, etc.)
gbm_bounded_0_1 <- TRUE
if (exists("gbm_bounded_0_1")) {
  if (as.logical(gbm_bounded_0_1) == T & "gbm" %in% names(cov_list)) {
    message("Truncating GBM values >= 1 to 0.999 and <= 0 to 1e-4")
    values(cov_list[["gbm"]])[values(
      cov_list[["gbm"]]
    ) >= 1 & !is.na(values(cov_list[["gbm"]]))] <- 0.999
    values(cov_list[["gbm"]])[values(
      cov_list[["gbm"]]
    ) <= 0 & !is.na(values(cov_list[["gbm"]]))] <- 1e-4
    gbm_cols <- grep(paste0("(gbm)(.*_pred)"),
                     names(df),
                     value = T
    )
    replace_one <- function(x) {
      x[x >= 1 & !is.na(x)] <- 0.999
      return(x)
    }
    replace_zero <- function(x) {
      x[x <= 0 & !is.na(x)] <- 1e-4
      return(x)
    }
    df[, (gbm_cols) := lapply(.SD, replace_one), .SDcols = gbm_cols]
    df[, (gbm_cols) := lapply(.SD, replace_zero), .SDcols = gbm_cols]
  }
}

## convert stackers to logit space (for computing probability values from 0 to 1)
if (
  as.logical(pipeline$config_list$stackers_in_transform_space)
  && as.logical(pipeline$config_list$use_stacking_covs)
  && pipeline$config_list$indicator_family == "binomial") {
  message("Converting stackers to logit space")
  
  ## transform the rasters
  for (ii in child_model_names) {
    
    ## Preserve variable names in the raster first
    tmp_rastvar <- names(cov_list[[ii]])
    
    ## Logit
    cov_list[[ii]] <- logit(cov_list[[ii]])
    
    ## Reassign names
    names(cov_list[[ii]]) <- tmp_rastvar
    rm(tmp_rastvar)
  }
  
  ## transform the stacker values that are in df
  stacker_col_regexp <- sprintf(
    "(%s)(.*_pred)", paste(child_model_names, collapse = "|")
  )
  stacker_cols <- grep(stacker_col_regexp, names(df), value = TRUE)
  df[, (stacker_cols) := lapply(.SD, logit), .SDcols = stacker_cols]
}

############ prep objects for fitting MBG model ############ 
## for stacking, overwrite the columns matching the model_names so that inla will be the  stacker
if (pipeline$config_list$use_stacking_covs) {
  df[, paste0(child_model_names) := lapply(
    child_model_names, function(x) get(paste0(x, "_cv_pred"))
  )]
}

## generate MBG formula for INLA call
mbg_formula <- build_mbg_formula_with_priors(
  fixed_effects = all_fixed_effects,
  add_nugget = as.logical(pipeline$config_list$use_inla_nugget),
  nugget_prior = pipeline$config_list$nugget_prior,
  add_ctry_res = as.logical(pipeline$config_list$use_country_res),
  ctry_re_prior = pipeline$config_list$ctry_re_prior,
  temporal_model_theta1_prior = pipeline$config_list$rho_prior,
  no_gp = !as.logical(pipeline$config_list$use_gp),
  coefs.sum1 = as.logical(pipeline$config_list$coefs_sum1),
  use_space_only_gp = as.logical(pipeline$config_list$use_space_only_gp),
  use_time_only_gmrf = as.logical(pipeline$config_list$use_time_only_gmrf),
  time_only_gmrf_type = pipeline$config_list$time_only_gmrf_type,
  subnat_RE = as.logical(pipeline$config_list$use_subnat_res),
  subnat_country_to_get = pipeline$config_list$subnat_country_to_get,
  subnat_re_prior = pipeline$config_list$subnat_re_prior,
  timebycountry_RE = as.logical(pipeline$config_list$use_timebyctry_res),
  adm0_list = gaul_list
)

# For INLA--add data for missing time points to ensure we get predictions
missing_years <- base::setdiff(
  pipeline$config_list$year_list,
  df$year
)

print(paste0("Missing years: ", missing_years))

if(pipeline$config_list$use_timebyctry_res) {
  df$adm0code <- gaul_convert(df$country)
  for(adm0_code in gaul_list) {
    dfsub <- df[df$adm0code == adm0_code, ]
    missing_years <- setdiff(pipeline$config_list$year_list, dfsub$year)
    if (length(missing_years) > 0) {
      fake_data <- dfsub[1:length(missing_years), ]
      fake_data[, year := missing_years]
      fake_data[, c(pipeline$indicator, 'N', 'weight') := 0]
      fake_data[, period := NULL]
      fake_data <- merge(fake_data, period_map)
      df <- rbind(df, fake_data)
    }
  }
} else {
  missing_years <- setdiff(pipeline$config_list$year_list, df$year)
  if (length(missing_years) > 0) {
    fake_data <- df[1:length(missing_years), ]
    fake_data[, year := missing_years]
    fake_data[, c(pipeline$indicator, 'N', 'weight') := 0]
    fake_data[, period := NULL]
    fake_data <- merge(fake_data, period_map)
    df <- rbind(df, fake_data)
  }
}

# get covariate constraints for data stack
cov_constraints <- covariate_constraint_vectorize(
  pipeline$config_list$fixed_effects,
  pipeline$config_list$gbd_fixed_effects,
  pipeline$config_list$fixed_effects_constraints,
  pipeline$config_list$gbd_fixed_effects_constraints
)

## create SPDE INLA stack 
## used to organize data, effects, and projection matrices
## must shape the data into the correct format, which is done using build_mbg_data_stack()
with_globals(
  new = list(indicator = pipeline$indicator),
  input_data <- build_mbg_data_stack(
    df = df,
    fixed_effects = all_fixed_effects,
    mesh_s = mesh_s,
    mesh_t = mesh_t,
    # raw covs will get center scaled here
    exclude_cs = child_model_names,
    spde_prior = pipeline$config_list$spde_prior,
    coefs.sum1 = pipeline$config_list$coefs_sum1,
    use_ctry_res = pipeline$config_list$use_country_res,
    use_subnat_res = pipeline$config_list$use_subnat_res,
    use_nugget = as.logical(pipeline$config_list$use_inla_nugget),
    yl = pipeline$config_list$year_list,
    zl = pipeline$config_list$z_list,
    zcol = pipeline$config_list$zcol,
    scale_gaussian_variance_N = pipeline$config_list$scale_gaussian_variance_N,
    tmb = pipeline$config_list$fit_with_tmb,
    cov_constraints = cov_constraints,
    use_gp = as.logical(pipeline$config_list$use_gp),
    use_space_only_gp = as.logical(pipeline$config_list$use_space_only_gp),
    use_time_only_gmrf = as.logical(pipeline$config_list$use_time_only_gmrf),
    shapefile_version = pipeline$config_list$modeling_shapefile_version,
    st_gp_int_zero = as.logical(pipeline$config_list$st_gp_int_zero),
    s_gp_int_zero = as.logical(pipeline$config_list$s_gp_int_zero),
    use_age_only_gmrf = as.logical(pipeline$config_list$use_age_only_gmrf),
    use_nid_res = as.logical(pipeline$config_list$use_nid_res),
    use_timebyctry_res = as.logical(pipeline$config_list$use_timebyctry_res),
    adm0_list = gaul_list,
    seed = seed,
    indicator_family = pipeline$config_list$indicator_family,
    nugget_prior = pipeline$config_list$nugget_prior,
    ctry_re_prior = pipeline$config_list$ctry_re_prior,
    nid_re_prior = pipeline$config_list$nid_re_prior,
    use_s2_mesh = pipeline$config_list$use_s2_mesh,
    mesh_s_max_edge = pipeline$config_list$mesh_s_max_edge,
    mesh_s_offset = pipeline$config_list$mesh_s_offset,
    s2_mesh_params = pipeline$config_list$s2_mesh_params
  )
)

## combine all the inputs, other than cs_df
stacked_input  <- input_data[[1]]
spde           <- input_data[[2]] 
cs_df          <- input_data[[3]]
spde.sp        <- input_data[[4]] 

## Generate other necessary inputs
outcome <- df[[pipeline$indicator]] 
N <- df$N 
weights <- df$weight

if (is.null(weights)) {
  weights <- rep(1, nrow(df))
}

set.seed(seed)
increment_seed(seed)

############ fit the MBG model ############ 
if (!as.logical(pipeline$config_list$skipinla)) {
  if (!as.logical(pipeline$config_list$fit_with_tmb)) {
    message("Fitting model with R-INLA")
    
    model_fit <- fit_mbg(
      indicator_family = pipeline$config_list$indicator_family,
      stack.obs = stacked_input,
      spde = spde,
      cov = outcome,
      N = N,
      int_prior_mn = pipeline$config_list$intercept_prior,
      f_mbg = mbg_formula,
      run_date = pipeline$run_date,
      keep_inla_files = pipeline$config_list$keep_inla_files,
      cores = Sys.getenv("ADDRESS"),
      wgts = weights,
      intstrat = pipeline$config_list$intstrat,
      fe_sd_prior = 1 / 9, ## sets precision, prec=1/9 -> sd=3
      verbose_output = TRUE,
      omp_strat = pipeline$config_list$omp_strat,
      blas_cores = as.integer(pipeline$config_list$blas_cores),
      pardiso_license = pipeline$config_list$pardiso_license
    ) 
  } else {
    message("Fitting model with TMB")
    message(sprintf(
      "%s Data points and %s mesh nodes",
      nrow(df),
      length(input_data$Parameters$Epsilon_stz)
    ))
    
    # save RDS file of input data for replication
    saveRDS(
      object = input_data, ## save this here in case predict dies
      file = sprintf(
        paste0("FILEPATH"),
        pipeline$indicator_group, pipeline$indicator, pipeline$run_date,
        ifelse(
          pipeline$config_list$fit_with_tmb, "tmb", "inla"
        ),
        reg, holdout, age
      )
    )
    # run the model
    model_fit <- fit_mbg_tmb(
      cpp_template = pipeline$config_list$tmb_template_path,
      tmb_input_stack = input_data,
      control_list = list(
        trace = 1,
        eval.max = 500, iter.max = 300, abs.tol = 1e-20
      ),
      optimizer = "nlminb",
      ADmap_list = NULL,
      sparse_ordering = as.logical(pipeline$config_list$sparse_ordering),
      seed = seed
    )
    
    # clamping
    clamp_covs <- pipeline$config_list$clamp_covs
  }
  
  saveRDS(
    object = model_fit,
    file = sprintf("FILEPATH"))
} else {
  model_fit <- readRDS(file = sprintf("FILEPATH"))
}

# distribution of the predicted prevalence values 
if (pipeline$config_list$fitting_plots == TRUE){
  png("FILEPATH")
  plot(model_fit)
  dev.off()
  
  png("FILEPATH")
  plot(density(inv.logit(model_fit$summary.fitted.values$mean)), main = "Distribution of Pixel-Year Prevelance Estimates", xlab = "Pixel-Year Prevelance Estimates", ylab = "Density")
  dev.off()
  
  # returns the predicted prevalence rates of LF by pixel
  png("FILEPATH")
  plot(model_fit$summary.fitted.values)
  dev.off()
}

######################## postamble ######################## 
# the pipeline_postamble saves all of the objects within R’s global environment as individual rda files in the Objects folder's corresponding node_id folder. The next script in the Dag looks up the previous node_id and loads in all of those rda files back into the global environment
pipeline_postamble()
q("no")

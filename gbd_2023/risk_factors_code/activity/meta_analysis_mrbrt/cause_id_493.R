rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
work_dir <- "FILEPATH"


library(ggplot2)
library(grid)
library(gridExtra, lib.loc = paste0(h, "FILEPATH"))
library(data.table)
source(paste0(code_dir, "FILEPATH")) # Loads functions like add_ui and get_knots
source("FILEPATH")
source(paste0(code_dir, 'FILEPATH'))
# Set up main variables
cause_num <- 493 # IHD
version <- 4

# Cause template
cause_ids <- get_ids("cause")

if(!dir.exists(paste0(work_dir, "../results/cause_id_", cause_num))){
  dir.create(paste0(work_dir, "../results/cause_id_", cause_num), recursive = T)
  save_dir <- paste0(work_dir, "../results/cause_id_", cause_num, "/")
  dir.create(paste0(save_dir, "models"))
  dir.create(paste0(save_dir, "plots"))
  dir.create(paste0(save_dir, "estimates"))
  dir.create(paste0(save_dir, "intermediates"))
} else {
  save_dir <- paste0(work_dir, "../results/cause_id_", cause_num, "/")
}
##### ---- Read data ---- #####
df <- fread(sprintf('FILEPATH', work_dir, cause_num, version)) %>%
  setnames(., c('a_0', 'a_1', 'b_0', 'b_1'), c('sr_a_0', 'sr_a_1', 'sr_b_0', 'sr_b_1'))
df[, seq := 1:.N]
# Apply SR to measure coefficient
sr_coef <- 1/1.351 # Update as of 2020_12_03

df[, `:=` (a_0 = sr_a_0 * sr_coef,
           a_1 = sr_a_1 * sr_coef,
           b_0 = sr_b_0 * sr_coef,
           b_1 = sr_b_1 * sr_coef)]
# Swap rows where alternative def is less than the reference
print(paste0("There are ", nrow(df[b_1 <= a_1,]), " rows where alternative is less than reference. Adjusting rows now."))
row_total <- nrow(df)

temp <- df[b_1 <= a_1,]
setnames(temp, 
         c("a_0", "a_1", "b_0", "b_1", "ln_effect"), 
         c("orig_a_0", "orig_a_1", "orig_b_0", "orig_b_1", "orig_ln_effect"))
temp[, `:=` (
  a_0 = orig_b_0,
  a_1 = orig_b_1,
  b_0 = orig_a_0,
  b_1 = orig_a_1,
  ln_effect = -(orig_ln_effect)
)]

# Append rows back on
df <- df[!seq %in% unique(temp$seq)]
df <- rbind(df, temp, use.names = T, fill = TRUE)

if(nrow(df) != row_total) stop(message("Rows of data after swapping alt and ref groups were not preserved."))

df <- df[order(seq),]
# dose column will be the exposure column used in CovFinder
df[,a_mid := (a_1 + a_0)/2][, b_mid := (b_1 + b_0)/2][,dose := b_mid - a_mid]

fit_model <- T


##### ---- Fit signal model
if(fit_model){
  np <- import("numpy")
  np$random$seed(as.integer(3197))
  set.seed(3197)
  # Create data object
  mrdata <- MRData()
  
  mrdata$load_df(
    data = df,
    col_obs = 'ln_effect',
    col_obs_se = 'ln_se',
    col_study_id = 'nid',
    col_covs = list("b_0", "b_1", "a_0", "a_1", "seq")
  )
  
  knots <- import("mrtool.core.model")
  
  knot_samples <- knots$create_knots_samples(
    data = mrdata,
    l_zero = T,
    num_splines = 50L,
    num_knots = 5L, 
    width_pct = 0.2,
    alt_cov_names = c("b_0", "b_1"),
    ref_cov_names = c("a_0", "a_1")
  )
  
  
  signal_model <- MRBeRT(
    data = mrdata,
    inlier_pct = 0.9,
    ensemble_cov_model =
      LogCovModel(
        alt_cov = c('b_0', 'b_1'),
        ref_cov = c('a_0', 'a_1'),
        use_re = FALSE,
        use_spline = TRUE,
        spline_knots = array(c(0, 0.25, 0.5, 0.75, 1)),
        spline_degree = 2L,
        spline_knots_type = 'domain',
        spline_r_linear = TRUE,
        prior_spline_funval_uniform = array(c(-1 + 1e-6, 19)),
        prior_spline_num_constraint_points = 150L,
        prior_spline_maxder_gaussian = rbind(c(0, 0, 0, 0), 
                                             c(0.01, 0.01, 0.01, Inf)),
        prior_spline_der2val_gaussian = NULL,
        prior_spline_der2val_gaussian_domain = array(c(0.0, 1.0)),
        prior_spline_monotonicity = 'decreasing',
        name = 'exposure'
      ),
    ensemble_knots = knot_samples
  )
  
  t0 <- Sys.time()
  signal_model$fit_model(inner_print_level=5L, inner_max_iter=200L, outer_step_size=200L, outer_max_iter=100L)
  print(Sys.time() - t0)
  
  # Get dataframe from model object
  df_data <- as.data.table(mrdata$to_df())
  
  # Predict out signal
  df_data$signal <- signal_model$predict(mrdata, predict_for_study = FALSE)
  
  # Determine weight of each observation
  w <- t(do.call(rbind, 
                 lapply(1:length(signal_model$sub_models), 
                        function(i){signal_model$sub_models[[i]]$w_soln}))
  ) %*% signal_model$weights
  
  # Drop trimmed data
  df_data$weight <- w
  trim_data <- df_data[weight >= 0.1,]
  
  if(save_dir %like% "FILEPATH"){

      py_save_object(signal_model, filename = paste0(save_dir, "FILEPATH"), pickle = "dill")
      fwrite(trim_data, file = paste0(save_dir, "FILEPATH"))
  }
  
} else {
  
  signal_model <- py_load_object(filename = paste0(save_dir, "FILEPATH"), pickle = "dill")
  trim_data <- fread(paste0(save_dir, "FILEPATH"))
}

plot_lpa_signal_model(signal_model, cause_num)

# Fit log linear models
if(fit_model){
  dat2 <- MRData()
  
  dat2$load_df(
    data = trim_data,
    col_obs = "obs",
    col_obs_se = "obs_se",
    col_study_id = "study_id",
    col_covs = as.list("signal")
  )
  
  cov_model <- list(LinearCovModel(
    alt_cov = "signal",
    use_re = TRUE,
    prior_beta_uniform=array(c(1.0, 1.0))
  ))
  
  mod2 <- MRBRT(data = dat2,
                cov_models = cov_model,
                inlier_pct = 1.0)
  
  mod2$fit_model(inner_print_level=5L, inner_max_iter=200L, outer_step_size=200L, outer_max_iter=100L)
  py_save_object(object = mod2,
                filename = paste0(save_dir, "FILEPATH"),
                pickle = "dill")
  
} else {
  mod2 <- py_load_object(paste0(save_dir, "FILEPATH"), pickle = "dill")
}


sampling <- import("mrtool.core.other_sampling")

beta_samples <- sampling$sample_simple_lme_beta(1000L, mod2)
beta_std <- sd(beta_samples)


##### ---- CovFinder ---- #####

# Create dummy variables for custom_domain column
unique(df$domain)
df[,custom_leisure_pa := ifelse(domain %in% c("sports", "recreation"), 1, 0)]
df[,custom_met_conversion := ifelse(unit %like% "MET", 0, 1)]

# Bias columns
bias_cols <- c(grep('_score', names(df), value = T), grep('custom_', names(df), value = T))
apply(df[,bias_cols, with = F], 2, function(x) length(unique(x)))

bias_cols <- bias_cols[!bias_cols %in% c("custom_domain", "exposure_idv_score", "exposure_obj_score", "outcomes_blind_score",
                                         "control_rct_other_score", "selection_bias_score", "custom_male", "custom_female", "custom_incidence", "custom_mortality")]

trim_data <- merge(trim_data, df[, c("seq", bias_cols), with = F], by = "seq")
trim_data <- make.eff.mod(trim_data, bias_cols, "signal")
df <- make.eff.mod(df, bias_cols, "dose")
e_m_cols <- grep("e_m_", names(trim_data), value = T)

test_cols <- e_m_cols[!e_m_cols %in% c("e_m_custom_incidence", "e_m_custom_mortality", "e_m_custom_male", "e_m_custom_female", 
                                       "e_m_custom_met_conversion", "e_m_custom_leisure_pa")]
cov_cols <- c('signal', e_m_cols)

if(fit_model){
  mrdata <- MRData()
  
  
  mrdata$load_df(
    data = trim_data,
    col_obs = 'obs',
    col_obs_se = 'obs_se',
    col_study_id = 'study_id',
    col_covs = as.list(cov_cols)
  )
  
  # Beta prior from first loglinear model results.
  
  covfinder <- do.call(
    CovFinder,
    c(COV_FINDER_CONFIG, 
      list(
        data = mrdata, 
        pre_selected_covs = list("signal"),
        covs = as.list(test_cols)),
      beta_gprior_std = BETA_PRIOR_MULTIPLIER * beta_std
    )
  )
  
  covfinder$select_covs(verbose = TRUE)
  
  selected_covs <- covfinder$selected_covs
  selected_covs <- selected_covs[selected_covs != "signal"]
  saveRDS(selected_covs, file = paste0(save_dir, "FILEPATH"))
  
} else {
  
  selected_covs <- readRDS(paste0(save_dir, "FILEPATH"))
  
}


# Fit mixed effects model
if(fit_model){
  
  cov_models <- list()
  for (cov in selected_covs) {
    cov_models <- append(cov_models,
                         list(do.call(LinearCovModel,
                                      list(alt_cov = cov,
                                           prior_beta_gaussian = array(c(0, BETA_PRIOR_MULTIPLIER * beta_std))))))
  }
  
  cov_models <-
    append(cov_models,
           LinearCovModel(
             'signal',
             use_re = TRUE,
             prior_beta_uniform = array(c(1, 1))
           ))
  
  final_model <- MRBRT(data = mrdata,
                       cov_models = cov_models,
                       inlier_pct = 1)
  
  final_model$fit_model(
    inner_print_level = 5L,
    inner_max_iter = 200L,
    outer_step_size = 200L,
    outer_max_iter = 100L
  )
  
  py_save_object(final_model, paste0(save_dir, "models/step4_signal_model.pkl"), pickle = "dill")
} else {
  final_model <- py_load_object(paste0(save_dir, "models/step4_signal_model.pkl"), pickle = "dill")
}

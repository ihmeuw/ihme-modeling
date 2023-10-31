#
# 01_create_template.R
#
# 
# 
#
library(dplyr)
library(mrbrt001, lib.loc = "FILEPATH")
library(data.table)
library(ggplot2)

# set seed
np <- import("numpy")
np$random$seed(as.integer(2738))
## 

if(interactive()){
  
  ro_pair <- "calcium_colorectal"
  out_dir <- "FILEPATH"
  WORK_DIR <- "FILEPATH"
  note <- ""
  test <- F
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  ro_pair <- args[1]
  out_dir <- args[2]
  WORK_DIR <- args[3]
  note <- ""
  test <- F
  
}

setwd(WORK_DIR)
source(paste0(out_dir, "/config.R"))
source("FILEPATH/helper_functions.R")

is_j_shaped <- ro_pair %in% J_SHAPE_RISKS

# Load in data
data <- readRDS(paste0(out_dir, "00_prepped_data/", ro_pair, ".RDS"))
df <- data$df

cov_names <- c(REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS)

mrdata <- MRData()

mrdata$load_df(
  data = df, 
  col_obs = OBS_VAR,
  col_obs_se = OBS_SE_VAR, 
  col_study_id = STUDY_ID_VAR, 
  col_covs = as.list(cov_names)
)

if(is_j_shaped){
  
  # Fit signal model for J-shape RO pairs
  ensemble_cov_model <- do.call(
    LogCovModel, 
    c(J_SHAPED_CONFIG, 
      list(
        alt_cov = ALT_EXPOSURE_COLS, 
        ref_cov = REF_EXPOSURE_COLS)
    )
  )
  
  N_I_KNOTS <- J_N_I_KNOTS

}else{

  # Fit signal model for all monotonic RO pairs
  
  monotonicity <- DIRECTION[strsplit(ro_pair, "_")[[1]][1]][[1]]
  
  if(ro_pair %like% "redmeat_hemstroke"){
    monotonicity <- "decreasing"
  }
  if(ro_pair %like% "prostate"){
    monotonicity <- "increasing"
  }
  if(ro_pair %like% "calcium"){
    # the prior has a large effect on calcium because calcium is in grams space so values are <1
    #   make the prior less strong here. 
    CONFIG$prior_spline_maxder_gaussian <- cbind(rbind(rep(0, N_I_KNOTS),
                                                       rep(Inf, N_I_KNOTS)),
                                                 c(0, sqrt(PRIOR_VAR_RSLOPE)*100))
    
  }
  
  
  ensemble_cov_model <- do.call(
    LogCovModel, 
    c(CONFIG, 
      list(
        alt_cov = ALT_EXPOSURE_COLS, 
        ref_cov = REF_EXPOSURE_COLS,
        prior_spline_monotonicity = monotonicity)
    )
  )
}

knots <- import("mrtool.core.model")
knots_samples <- knots$create_knots_samples(
  data = mrdata, 
  l_zero = TRUE, 
  num_splines = 50L, 
  num_knots = N_I_KNOTS + 2L, 
  width_pct = 0.2,
  alt_cov_names = ALT_EXPOSURE_COLS,
  ref_cov_names = REF_EXPOSURE_COLS
)


# Fit ensemble model with exposure only 
signal_model <- MRBeRT(mrdata,
                      ensemble_cov_model=ensemble_cov_model,
                      ensemble_knots=knots_samples,
                      inlier_pct=INLIER_PCT)

signal_model$fit_model(inner_print_level=5L, inner_max_iter=200L, 
	outer_step_size=200L, outer_max_iter=100L)

df_data <-  mrdata$to_df()

# Extract weights of data point
w <- t(do.call(rbind, 
          lapply(1:length(signal_model$sub_models), 
                 function(i){signal_model$sub_models[[i]]$w_soln}))
       ) %*% signal_model$weights


# Assign signal to data for use in later stages
signal <- signal_model$predict(mrdata, predict_for_study=FALSE)
df_data$signal <-  signal

# Drop data trimmed so don't need to worry about trimming the rest of it
df_data <-  df_data[w >= 0.1,]

# Save model and visualize curve
if(!test){
  py_save_object(object = signal_model, 
  	filename = paste0(out_dir, "01_template_pkl_files/", ro_pair, ".pkl"), 
  	pickle = "dill")
  
  out <- append(data, list(df_data=df_data))
  saveRDS(out, paste0(out_dir, "01_template_models/", ro_pair, ".RDS"))
}


# J- shape get TMREL uncertainty
if(is_j_shaped){
  
  bottom <- data.table()
  
  NUM_POINTS <- 1000
  percentiles <- c("05", "10", "15", "50", "85", "90", "95")
  
  exposure_lower <- min(df[,c(REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS)])
  exposure_upper <- max(df[,c(REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS)])
  exposure <- seq(exposure_lower, exposure_upper, length.out=NUM_POINTS)

  min_cov <- rep(min(exposure), NUM_POINTS)
  df_signal_pred <- data.frame(a_0=min_cov, a_1=min_cov, b_0=exposure, b_1=exposure)
  
  # Sample the model solution
  # takes a while since this is an ensemble model
  sampling <- import("mrtool.core.other_sampling")
  samples <- signal_model$sample_soln(sample_size = 1000L)
  
  data_signal_pred <- MRData()
  
  data_signal_pred$load_df(
    data = df_signal_pred,
    col_covs = as.list(names(df_signal_pred))
  )
  
  draws_fe <- signal_model$create_draws(
    data = data_signal_pred,
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study = FALSE
  )
  
  # set names
  draws_fe <- as.data.table(draws_fe)
  setnames(draws_fe, colnames(draws_fe), paste0("draw_",0:999))
  df_draws_fe <- cbind(data.table("b_0" = exposure,
                              "a_0" = min_cov), as.data.table(draws_fe))
  
  temp <- melt(df_draws_fe, id.vars = setdiff(names(df_draws_fe), paste0("draw_", 0:999)))
  temp[, min_rr := min(value), by = "variable"]
  min_exp <- temp[value == min_rr,]$b_0
  
  lapply(percentiles, function(p){
    bottom[, paste0("perc_", p) := as.numeric(quantile(min_exp, probs = as.numeric(paste0(".", p))))]
  })
  bottom[, mean := mean(min_exp)]
  
  bottom <- as.data.frame(bottom)
  
  write.csv(bottom, paste0(out_dir, "01_template_models/", ro_pair, "_tmrel.csv"), row.names = F)
  
  write.csv(data.table("min_exp" = min_exp), paste0(out_dir, "01_template_models/", ro_pair, "_tmrel_draws.csv"), row.names = F)
  
}



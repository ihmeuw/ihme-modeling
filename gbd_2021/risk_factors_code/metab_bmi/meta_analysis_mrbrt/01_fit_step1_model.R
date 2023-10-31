# STEP 1: Fit log ratio ensemble model with exposure covariates only

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
work_dir <- "FILEPATH"

library(dplyr)
source(paste0(code_dir, "FILEPATH")) # Loads functions like add_ui and get_knots
library(ggplot2)
library(data.table)
source("FILEPATH")
source(paste0(code_dir, 'FILEPATH'))
cause_ids <- get_ids("cause")
# Set up arguments
args <- commandArgs(trailingOnly = TRUE)

version <- as.numeric(args[1])
cause_num <- as.numeric(args[2])
save_dir <- args[3]
fit_model <- args[4]
lower_bound <- as.numeric(args[5])
upper_bound <- as.numeric(args[6])
num.knots <- as.numeric(args[7])

# Read in data

df <- fread(sprintf('FILEPATH', work_dir, cause_num, version))

# Make seq column for unique identifier
df[, seq := 1:.N]

##### Adjust upper and lower bounds of data
# Set lower bound for reference group
df[a_0 < lower_bound, a_0 := lower_bound]

# For rows where the upper bound of reference group is less than fixed lower bound, set upper bound to fixed lower
df[a_1 < a_0, a_1 := lower_bound]

# Similar approach for alternative group
df[b_0 < lower_bound, b_0 := lower_bound]
df[b_1 < b_0, b_1 := lower_bound]

# Set upper bounds
df[b_1 >= upper_bound, b_1 := upper_bound]

if(nrow(df[b_0 > upper_bound]) != 0){
  
  warning(message(paste0(
    "There are row(s) in the data where the lower bound of the alternative group (b_0) is/are greater than the fixed upper bound of ", upper_bound
    )))
  message(paste0("Dropping ", nrow(df[b_0 > upper_bound]), " row(s)"))
  
  df <- df[b_0 <= upper_bound]
}

if(nrow(df[a_0 > upper_bound | a_1 > upper_bound]) != 0){
  
  stop(message(paste0(
    "There are row(s) in the data where the reference group is/are greater than the fixed upper bound of ", upper_bound
  )))
  
}

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
# Calculate mid points and dose
df[,a_mid := (a_1 + a_0)/2][, b_mid := (b_1 + b_0)/2][,dose := b_mid - a_mid]

##### Fit model

if(fit_model){
  np <- import("numpy")
  np$random$seed(as.integer(3197))
  
  set.seed(3197)
  
  dat1 <- MRData()
  dat1$load_df(
    data = df,
    col_obs = "ln_effect",
    col_obs_se = "ln_se",
    col_covs = list("b_0", "b_1", "a_0", "a_1", "seq"),
    col_study_id = "nid"
  )
  
  knots <- import("mrtool.core.model")
  
  if(num.knots == 5) {
    
    knot_samples <- knots$create_knots_samples(
      data = dat1,
      l_zero = F,
      num_splines = 50L,
      num_knots = 5L, 
      width_pct = 0.2,
      alt_cov_names = c("b_0", "b_1"),
      ref_cov_names = c("a_0", "a_1")
    )
    
    signal_model <- MRBeRT(
      data = dat1,
      inlier_pct = 0.9,
      ensemble_cov_model =
        LogCovModel(
          alt_cov = c('b_0', 'b_1'),
          ref_cov = c('a_0', 'a_1'),
          use_re = FALSE,
          use_spline = TRUE,
          spline_knots = array(c(0, 0.25, 0.5, 0.75, 1)),
          spline_degree = 3L,
          spline_knots_type = 'domain',
          spline_l_linear = TRUE,
          spline_r_linear = TRUE,
          prior_spline_funval_uniform = array(c(-1 + 1e-6, 19)),
          prior_spline_num_constraint_points = 150L,
          prior_spline_maxder_gaussian = rbind(c(0, 0, 0, 0), 
                                               c(Inf, 0.01, 0.01, Inf)),
          prior_spline_der2val_gaussian = NULL,
          prior_spline_der2val_gaussian_domain = array(c(0.0, 1.0)),
          name = 'exposure'
        ),
      ensemble_knots = knot_samples
    )
  } 
  
  if(num.knots == 4) {
    
    knot_samples <- knots$create_knots_samples(
      data = dat1,
      l_zero = F,
      num_splines = 50L,
      num_knots = 4L, 
      width_pct = 0.2,
      alt_cov_names = c("b_0", "b_1"),
      ref_cov_names = c("a_0", "a_1")
    )
    
    signal_model <- MRBeRT(
      data = dat1,
      inlier_pct = 0.9,
      ensemble_cov_model =
        LogCovModel(
          alt_cov = c('b_0', 'b_1'),
          ref_cov = c('a_0', 'a_1'),
          use_re = FALSE,
          use_spline = TRUE,
          spline_knots = array(c(0, 0.5, 0.75, 1)),
          spline_degree = 3L,
          spline_knots_type = 'domain',
          spline_l_linear = TRUE,
          spline_r_linear = TRUE,
          prior_spline_funval_uniform = array(c(-1 + 1e-6, 19)),
          prior_spline_num_constraint_points = 150L,
          prior_spline_maxder_gaussian = rbind(c(0, 0, 0),
                                              c(Inf, 0.01, Inf)),
          prior_spline_der2val_gaussian = NULL,
          prior_spline_der2val_gaussian_domain = array(c(0.0, 1.0)),
          name = 'exposure'
        ),
      ensemble_knots = knot_samples
    )
  }
  
  t0 <- Sys.time()
  signal_model$fit_model(inner_print_level=5L, inner_max_iter=200L, outer_step_size=200L, outer_max_iter=100L)
  print(Sys.time() - t0)
  
  print(paste0("Saving to ", save_dir, "FILEPATH"))
  py_save_object(
    signal_model,
    filename = paste0(save_dir, "FILEPATH"),
    pickle = "dill"
  )
  print("success")

  
} else{
  
  signal_model <- py_load_object(paste0(save_dir, "FILEPATH"), pickle = "dill")
  
}

# Get dataframe from model object
df_data <- as.data.table(dat1$to_df())


# Predict out signal
df_data$signal <- signal_model$predict(dat1, predict_for_study = FALSE)

# Determine weight of each observation
w <- t(do.call(rbind, 
               lapply(1:length(signal_model$sub_models), 
                      function(i){signal_model$sub_models[[i]]$w_soln}))
) %*% signal_model$weights

# Drop trimmed data
df_data$weight <- w
trim_data <- df_data[weight >= 0.1,]

# Save trimmed data
if(save_dir %like% "FILEPATH"){
  fwrite(df, file = paste0(save_dir, "FILEPATH"))
  fwrite(trim_data, file = paste0(save_dir, "FILEPATH"))
}

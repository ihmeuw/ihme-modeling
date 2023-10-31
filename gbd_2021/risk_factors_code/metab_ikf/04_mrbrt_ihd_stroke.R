#--------------------------------------------------------------------
# Name: QWR
# Date: Feb. 10, 2021
# Project: KD RR Evidence Score + Pub Bias
# Purpose: Run MRBRT models for all ihd and stroke risk-outcome pairs, MIXED b/c of splines
# Room for improvements: Update so "no_bp" is in if statement, 
# for loop to run all ro for escore, append all summaries together
#---------------------------------------------------------------------

# Set up ---------------------------------------------------------------------------------------------------
rm(list = ls())
setwd("/ihme/code/qwr/ckd_qwr/evidence_score_pipeline/") # change the dir

# map drives
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/"
  h_root <- "~/"
} else {
  j_root <- "J:/"
  h_root <- "H:/"
}

# libs
require(reticulate)
library(data.table)
library(mrbrt001, lib.loc = "/ihme/code/mscm/R/packages/")
library(ggplot2)
library(openxlsx)
library(dplyr)
source("/ihme/code/qwr/escore/evidence_score_pipeline/src/utils/mixed_functions.R") 

# Settings ---------------------------------------------------------------------------------------------------
folder <- "/ihme/code/qwr/ckd_qwr/evidence_score_pipeline/" # "/ihme/epi/ckd/ckd_code/kd/" #update this
cvs <- list("cov_selection_bias", "age_mean")
ver <- "fisher_age_group"
# run a test on stage3/str before running all ros
exp <- "stage4" 
out <- "str"
dataset <- fread("/ihme/epi/ckd/kd/prepped_data/kd_prepped_data_all.csv") # this kd_prepped data will need to be moved

# graphing function
add_ui <- function(dat, x_var, lo_var, hi_var, color = "darkblue", opacity = 0.2) {
  polygon(
    x = c(dat[, x_var], rev(dat[, x_var])),
    y = c(dat[, lo_var], rev(dat[, hi_var])),
    col = adjustcolor(col = color, alpha.f = opacity), border = FALSE
  )
}

i <- 1
gammas <- matrix(1, nrow = 8, ncol = 3)


for(exp in c("albuminuria", "stage3", "stage4", "stage5")) {
  for(out in c("chdh", "str")) {
    message(paste0("working on ", exp, " : ", out))
    df <- dataset[risk==exp & outcome==out]
    
  
    # set up folder
    model_dir_bp <- paste0(exp, "_", out, "_bp/")
    dir.create(paste0(folder, "mrbrt_model_outputs/",ver,"/", model_dir_bp), recursive = TRUE)
    dir.create(paste0(folder, "plots/v", ver), recursive = TRUE)
    
    # set up data
    mr_df_bp <- MRData()
    
    mr_df_bp$load_df(
      data = df[blood_pressure==1], col_obs = "log_mean", col_obs_se = "se",
      col_covs = cvs, col_study_id = "study_name")
    
    # set up model
    model_bp <- MRBRT(
      data = mr_df_bp,
      cov_models =list(
        LinearCovModel("intercept", use_re = TRUE),
        LinearCovModel("cov_selection_bias", use_re = FALSE),
        LinearCovModel(alt_cov = "age_mean",
                       use_spline = TRUE,
                       spline_knots = array(c(0, 0.25, 0.5, 0.75, 1)),
                       spline_degree = 3L,
                       spline_knots_type = 'frequency',
                       spline_r_linear = TRUE,
                       spline_l_linear = TRUE  # ver 4 = FALSE, ver 5 = TRUE
                       # prior_spline_monotonicity = 'increasing'
                       # prior_spline_convexity = "convex"
                       # prior_spline_maxder_gaussian = array(c(0, 0.01))
                       # prior_spline_maxder_gaussian = rbind(c(0,0,0,0,-1), c(Inf,Inf,Inf,Inf,0.0001))
        )),
      inlier_pct = .9)
    
    
    # fit model
    model_bp$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

    # save model object
    py_save_object(object = model_bp, filename = paste0(folder, "mrbrt_model_outputs/",ver,"/", model_dir_bp, "mod1.pkl"), pickle = "dill") 
  
    }
}

# get draws with fisher mat ----------------------------------------------------------------------------------------------------------
repl_python()
quit

path <- paste0("/ihme/code/qwr/ckd_qwr/evidence_score_pipeline/mrbrt_model_outputs/",ver,"/")

for (exp in c("albuminuria", "stage3", "stage4", "stage5")) {
  for (out in c("chdh", "str")) {
    model_path <- paste0(path, exp, "_", out, "_bp/mod1.pkl")
    model_path
    ro_pair <- paste0("ckd_", exp, "_", out) # change to match your model_path ro
    ro_pair
    
    model <- py_load_object(filename = model_path, pickle = "dill")
    
    ### Extract data
    data_info <- extract_data_info(model, cont_cov = "age_mean")
    df <- data_info$df
    data_info$ro_pair <- ro_pair
    

    data_info$pred_cov <- c(27.5,32.5,37.5,42.5,47.5,52.5,57.5,62.5,67.5, 72.5, 77.5, 82.5, 87.5, 92.5, 97.5)

    draw_fish <- get_draws(data_info, model)

    # write draws for evidence score
    dir.create( paste0("/ihme/scratch/users/qwr/ylds/ckd/escore/",ver, "/"), recursive = TRUE)
    write.csv(draw_fish, file = paste0("/ihme/scratch/users/qwr/ylds/ckd/escore/",ver,"/rr_draws_", exp, "_", out, ".csv"), row.names = FALSE)
  }
}

# grab evidence score without pubilcation bias --------------------------------------------------------------------
# repl_python()
# quit
# # type 'exit' or hit escape
# evidence_score <- import("mrtool.evidence_score.scorelator")
# 
# for(exp in c("albuminuria", "stage3", "stage4", "stage5")) {
#   for(out in c("chdh", "str")) {
#     message(paste0("working on scorelator for ", exp, " : ", out))
#     
#     mrbrt_output_dir<-paste0(folder,"mrbrt_model_outputs/",ver, "/", exp, "_", out, "_bp/")
#     mrbrt_output_dir
#     model <- py_load_object(filename = paste0(mrbrt_output_dir, "mod1.pkl"), pickle = "dill")
#     
#     scorelator <- evidence_score$MixedScorelator(model = model,
#                                                  cont_cov_name='age_mean',
#                                                  cont_quantiles = c(0,1),
#                                                  num_samples=5000L,
#                                                  name=paste0(exp, "_", out))
#     scorelator$plot_model(folder=paste0(folder, "plots/v", ver))
#     score <- scorelator$get_score()
#     low_score <- scorelator$get_score(use_gamma_ub=TRUE)
#     
#     scores <- data.frame(
#       cause_id <- 589,
#       rei_id <- 341,
#       exposure <- exp,
#       outcome <- out,
#       score = scorelator$get_score(),
#       score_gamma = scorelator$get_score(use_gamma_ub=TRUE),
#       gamma = scorelator$gamma
#     )
#     scorelator$get_score() # score
#     scorelator$get_score(use_gamma_ub = T) # low score
#     
#     if (exp=="albuminuria" & out=="chdh") {
#       allscores <- scores
#     } else {
#       allscores <- rbind(allscores, scores)
#     }
#     
#   }
# }
# 
# write.csv(allscores, file =paste0("/ihme/code/qwr/ckd_qwr/evidence_score_pipeline/scorelator/scores_v", ver, ".csv"), row.names = TRUE)
# 


## Get evidence score with Pub Bias ------------------------------------------------------------------------------------
# ### Detect publication bias
# egger_model_all <- egger_regression(df$residual, df$residual_se)
# egger_model <- egger_regression(df[!df$outlier,]$residual, df[!df$outlier,]$residual_se)
# has_pub_bias <- egger_model$pval < 0.05
# 
# 
# ### Adjust for publication bias
# if (has_pub_bias) {
#   df_fill <- get_df_fill(df)
#   num_fill <- nrow(df_fill)
# } else {
#   num_fill <- 0
# }
# 
# # fill the data if needed and refit the model
# if (num_fill > 0) {
#   df <- rbind(df, df_fill)
#   data_info$df <- df
#   
#   # refit the model
#   data = MRData()
#   data$load_df(
#     data=df[!df$outlier,],
#     col_obs='obs',
#     col_obs_se='obs_se',
#     col_covs=as.list(model$cov_names),
#     col_study_id='study_id'
#   )
#   model_fill <- MRBRT(data, cov_models=model$cov_models)
#   model_fill$fit_model()
# } else {
#   model_fill <- NULL
# }
# 
# 
# ### Extract scores
# uncertainty_info <- get_uncertainty_info(data_info, model)
# if (is.null(model_fill)) {
#   uncertainty_info_fill <- NULL
# } else {
#   uncertainty_info_fill <- get_uncertainty_info(data_info, model_fill)
# }
# 
# 
# ### Output diagnostics
# pdf(file = paste0("/ihme/code/qwr/ckd_qwr/evidence_score_pipeline/plots/v",ver,"/", ro_pair, ".pdf"))
# title <- (paste(ro_pair, "\n","sim", ": egger_mean=", round(egger_model$mean, 3),
#                 ", egger_sd=", round(egger_model$sd,3), ", egger_pval=",
#                 round(egger_model$pval, 3)))
# plot_residual(df, title)
# 
# plot_model(
#   data_info,
#   uncertainty_info,
#   model,
#   uncertainty_info_fill,
#   model_fill)
# dev.off()
# 
# summary <- summarize_model(data_info,
#                            uncertainty_info,
#                            model,
#                            egger_model,
#                            egger_model_all,
#                            uncertainty_info_fill = uncertainty_info_fill,
#                            model_fill = model_fill)
# # summary
# dir.create(paste0("/ihme/code/qwr/ckd_qwr/evidence_score_pipeline/summary/",ver,"/"), recursive = TRUE)
# write.csv(summary, file = paste0("/ihme/code/qwr/ckd_qwr/evidence_score_pipeline/summary/",ver,"/", ro_pair,"_v",ver, ".csv"), row.names = FALSE)
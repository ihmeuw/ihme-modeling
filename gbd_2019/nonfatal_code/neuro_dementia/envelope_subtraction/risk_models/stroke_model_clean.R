##########################################################################
### Author: USERNAME
### Date: 6/18/2019 
### Project: GBD Nonfatal Estimation
### Purpose: Stroke MR-BRT MODEL
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
library(msm, lib.loc = paste0(j_root, "FILEPATH"))
date <- gsub("-", "_", Sys.Date())

# OBJECTS -----------------------------------------------------------------

stroke_dir <- paste0("FILEPATH")
functions_dir <- paste0("FILEPATH")
mrbrt_helper_dir <- paste0("FILEPATH")

# LOAD FUNCTIONS ----------------------------------------------------------

functs <- c("get_bundle_data.R", "save_bundle_version.R", "get_bundle_version.R", "get_crosswalk_version.R",
            "save_crosswalk_version.R", "upload_bundle_data.R", "get_location_metadata.R", "get_age_metadata.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x))))
mrbrt_functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
                  "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
                  "load_mr_brt_preds_function", "plot_mr_brt_function")
invisible(lapply(mrbrt_functs, function(x) source(paste0(mrbrt_helper_dir, x, ".R"))))

# GET DEMOGRAPHICS --------------------------------------------------------

age_dt <- get_age_metadata(12)
age_dt <- age_dt[age_group_id >=13]
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_years_end == 124, age_group_years_end := 99]

# GET AND FORMAT DATA -----------------------------------------------------

stroke_dt <- as.data.table(read.xlsx(paste0("FILEPATH")))
stroke_dt <- stroke_dt[is.na(group_review) | group_review == 1]
stroke_dt <- stroke_dt[!case_name_stroke == "Ischemic stroke"]

## AGE AND SEX (NOT DOING SEX BECAUSE ONLY HAVE 3 NON-BOTH DATA POINTS)
stroke_dt[!is.na(mean_age_diagnosis), risk_age := mean_age_diagnosis]
stroke_dt[mean_age_verbose == "small age band", mean_age := (age_end-age_start)*0.6 + age_start]
stroke_dt[is.na(risk_age), risk_age := mean_age + follow_up_time]

## COVS
stroke_dt[, case_control := as.numeric(study_design == "case control")]

all_covs <- c("risk_age", "case_control", "control_demographics", "control_education", "control_cvd",
              "stroke_self_report_Y_N", "stroke_clinical_record", "stroke_inclusive_TIA", "incident_stroke",
              "clinical_sample", "dementia_diagnosis_clinical_records")

## TRANSFORMATION
stroke_dt[, standard_error := (upper-lower)/3.92]
stroke_dt[, log_mean := log(mean)]
stroke_dt$log_se <- sapply(1:nrow(stroke_dt), function(i) {
  mean_i <- as.numeric(stroke_dt[i, "mean"])
  se_i <- as.numeric(stroke_dt[i, "standard_error"])
  deltamethod(~log(x1), mean_i, se_i^2)
})

## VIZUALIZE DATA
viz_data <- ggplot(stroke_dt, aes(x = risk_age, y = log_mean, size = 1/log_se^2)) +
  geom_point() +
  labs(x = "Age", y = "Log RR") +
  scale_size_continuous(name = "Inverse Variance") +
  theme_classic()

# RUN MODEL ---------------------------------------------------------------

create_covs <- function(x){
  if (x == "risk_age"){
    return(cov_info(x, "X", degree = 3, n_i_knots = 3, knot_placement_procedure = "frequency",
                    bspline_mono = "decreasing", bspline_gprior_mean = "0, 0, 0, 0",
                    bspline_gprior_var = "1e-5, inf, inf, 1e-5"))
  } else {
    return(list(cov_info(x, "X", gprior_mean = 0, gprior_var = 0.01), 
                cov_info(x, "Z", gprior_mean = 0, gprior_var = 0.01)))
  }
}

cov_list <- lapply(all_covs, create_covs)
second_elements <- list()
for(n in 1:length(cov_list)){
  if (length(cov_list[[n]]) == 2){
    save <- cov_list[[n]][[2]]
    cov_list[[n]][[2]] <- NULL
    cov_list[[n]] <- cov_list[[n]][[1]]
    second_elements[[n]] <- save
  } else {
    next
  }
}
cov_list <- c(cov_list, second_elements)

model_name <- paste0("stroke_model_", date)

stroke_model <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = model_name,
  data = stroke_dt,
  mean_var = "log_mean",
  se_var = "log_se",
  covs = cov_list,
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1
)
readr::write_rds(stroke_model, paste0("FILEPATH"))

# PREDICTIONS ------------------------------------------------------------- 
pred_dt <- expand.grid(risk_age = seq(42.5, 97.5, by = 5), case_control = 0, control_demographics = 1, control_education = 1, 
                       control_cvd = 1, stroke_self_report_Y_N = 0, stroke_clinical_record = 1, stroke_inclusive_TIA = 0,
                       incident_stroke = 0, clinical_sample = 0, dementia_diagnosis_clinical_records = 0)

stroke_predicts <- predict_mr_brt(stroke_model, newdata = pred_dt, write_draws = T)

# GRAPHS ------------------------------------------------------------------

graph_mrbrt_results <- function(results, predicts){
  data_dt <- as.data.table(results$train_data)
  model_dt <- as.data.table(predicts$model_summaries)
  shapes <- c(15, 16, 18)
  y_lab <- "Relative Risk (log scale)"
  breaks <- seq(data_dt[, floor(min(log_mean))], data_dt[, ceiling(max(log_mean))])
  
  data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
  gg_subset <- ggplot() +
    geom_point(data = data_dt, aes(x = risk_age, y = log_mean, color = as.factor(excluded), size = 1/log_se^2)) +
    geom_smooth(data = model_dt, aes(x = X_risk_age, y = Y_mean), color = "black") +
    geom_ribbon(data = model_dt, aes(x = X_risk_age, ymin = Y_mean_lo, ymax = Y_mean_hi), alpha = 0.05) +
    scale_fill_manual(guide = F, values = c("midnightblue", "purple")) +
    scale_y_continuous(name = y_lab, breaks = breaks, labels = round(exp(breaks), 2)) +
    scale_color_manual(name = "", values = c("blue", "red"), labels = c("Included", "Trimmed")) +
    scale_size_continuous(name = "Inverse Variance") +
    labs(x = "Age") +
    ggtitle(paste0("Meta-Analysis Results for RR of Stroke")) +
    theme_classic() +
    geom_hline(yintercept = 0) +
    theme(text = element_text(size = 15, color = "black"))
  return(gg_subset)
}

cov_plot <- function(model){
  dt <- as.data.table(model[[4]])
  graph_dt <- copy(dt)
  graph_dt <- graph_dt[!grepl("risk_age", x_cov)]
  graph_dt[, `:=` (lower = beta_soln - 1.96*sqrt(beta_var), upper = beta_soln + 1.96*sqrt(beta_var))]
  cov_plot <- ggplot(graph_dt, aes(x = beta_soln, y = as.factor(x_cov))) +
    geom_vline(xintercept = 0, linetype = "dotted", color = "midnightblue") +
    geom_point(color = "blue") +
    geom_errorbarh(aes(xmin = lower, xmax = upper), color = "blue") +
    labs(x = "Log Effect Size", y = "Variable") +
    ggtitle(paste0("Effect Sizes")) +
    theme_classic()
  return(cov_plot)
}

pdf(paste0("FILEPATH"), width = 10)
graph_mrbrt_results(stroke_model, stroke_predicts)
cov_plot(stroke_model)
dev.off()

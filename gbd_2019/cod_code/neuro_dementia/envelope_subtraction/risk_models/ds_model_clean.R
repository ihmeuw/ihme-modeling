##########################################################################
### Author: USERNAME
### Date: 6/18/2019 
### Project: GBD Nonfatal Estimation
### Purpose: DS MR-BRT MODEL
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
library(msm, lib.loc = paste0("FILEPATH"))
date <- gsub("-", "_", Sys.Date())

# OBJECTS -----------------------------------------------------------------

ds_dir <- paste0("FILEPATH")
functions_dir <- paste0("FILEPATH")
mrbrt_helper_dir <- paste0("FILEPATH")
dem_cid <- ID

# LOAD FUNCTIONS ----------------------------------------------------------

functs <- c("get_bundle_data.R", "save_bundle_version.R", "get_bundle_version.R", "get_crosswalk_version.R",
            "save_crosswalk_version.R", "upload_bundle_data.R", "get_location_metadata.R", "get_age_metadata.R",
            "get_ids.R", "get_outputs.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x))))
mrbrt_functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
                  "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
                  "load_mr_brt_preds_function", "plot_mr_brt_function")
invisible(lapply(mrbrt_functs, function(x) source(paste0(mrbrt_helper_dir, x, ".R"))))

get_closest_age <- function(i, var, start = T, dt){
  age <- dt[i, get(var)]
  if (start == T){
    age_dt[, age_group_years_start][which.min(abs(age_dt[, age_group_years_start] - age))]
  } else if (start == F){
    age_dt[, age_group_years_end][which.min(abs(age_dt[, age_group_years_end] - age))]
  }
}

# GET DEMOGRAPHICS --------------------------------------------------------

age_dt <- get_age_metadata(12)
age_dt <- age_dt[age_group_id >=13]
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_years_end == 124, age_group_years_end := 99]
sex_dt <- get_ids(table = "sex")
loc_dt <- get_location_metadata(location_set_id = 22)

# GET DATA AND FORMAT -----------------------------------------------------

ds_dt <- as.data.table(read.xlsx(paste0("FILEPATH")))

## AGE AND SEX 
ds_dt[age_end == 99, age_end := 69] ## SET AGE END TO 69 WHICH IS THE HIGHEST AGE WE HAVE DS PREVALENCE
ds_dt[, risk_age := ifelse(calc_age == 1, (age_end-age_start) * 0.6 + age_start, as.numeric(mean_age))]
ds_dt <- merge(ds_dt, sex_dt, by = "sex")

## GET LOCATION INFO
ds_dt[, ihme_loc_id := gsub(".*\\|", "", location_name_extraction)]
ds_dt <- merge(ds_dt, loc_dt[, .(ihme_loc_id, location_id)], by = "ihme_loc_id")

## CALCULATE PREVALENCE RATIO
ds_dt[is.na(mean), mean := cases_dementia/sample_size_downs]
ds_dt <- ds_dt[!mean == 0]
ds_dt[, year_id := round((year_start+year_end)/2, 0)]
ds_dt[year_id<1990, year_id := 1990]
ds_dt$age_start_pull <- sapply(1:nrow(ds_dt), function(i) get_closest_age(i, var = "risk_age", dt = ds_dt))
ds_dt <- merge(ds_dt, age_dt[, .(age_group_years_start, age_group_id)], by.x = "age_start_pull", by.y = "age_group_years_start")
prev_dt <- get_outputs(topic = "cause", measure_id = 5, year_id = ds_dt[, unique(year_id)], location_id = ds_dt[, unique(location_id)],
                       age_group_id = ds_dt[, unique(age_group_id)], sex_id = ds_dt[, unique(sex_id)], cause_id = dem_cid,
                       gbd_round_id = 5, metric_id = 3)
ds_dt <- merge(ds_dt, prev_dt[, .(age_group_id, sex_id, location_id, year_id, val)], by = c("age_group_id", "sex_id", "location_id", "year_id"), all.x = T)
ds_dt[, rr := mean/val]

## COVS
ds_dt[, case_name_AD := as.numeric(grepl("Alz", case_name_dementia))]
ds_dt[, case_ascer_hosp := ifelse(hospital_inpatient_only == 1 | hospitalization_only == 1, 1, 0)]

all_covs <- c("risk_age", "case_name_AD", "case_ascer_hosp", "dementia_diagnosis_clinical_records")

## TRANSFORMATION
z <- qnorm(0.975)
ds_dt[, standard_error := sqrt(mean*(1-mean)/sample_size_downs + z^2/(4*sample_size_downs^2))] ## FORMULA FROM UPLOADER FOR STANDARD ERROR
ds_dt[, log_mean := log(rr)]
ds_dt$log_se <- sapply(1:nrow(ds_dt), function(i) {
  mean_i <- as.numeric(ds_dt[i, "mean"])
  se_i <- as.numeric(ds_dt[i, "standard_error"])
  deltamethod(~log(x1), mean_i, se_i^2)
})

## VIZUALIZE DATA
viz_data <- ggplot(ds_dt, aes(x = risk_age, y = log_mean, size = 1/log_se^2, color = sex)) +
  geom_point() +
  labs(x = "Age", y = "Log RR") +
  scale_color_manual(values = c("Both" = "midnightblue", "Female" = "purple", "Male" = "red"),
                     name = "Sex") +
  scale_size_continuous(name = "Inverse Variance") +
  theme_classic()

# RUN MR-BRT --------------------------------------------------------------

create_covs <- function(x){
  if (x == "risk_age"){
    return(cov_info(x, "X", degree = 3, n_i_knots = 3, knot_placement_procedure = "frequency",
                    bspline_mono = "decreasing", bspline_gprior_mean = "0, 0, 0, 0", r_linear = F,
                    bspline_gprior_var = "1e-3, inf, inf, 1e-3"))
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

model_name <- paste0("ds_model_", date)

ds_model <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = model_name,
  data = ds_dt,
  mean_var = "log_mean",
  se_var = "log_se",
  covs = cov_list,
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1
)
readr::write_rds("FILEPATH")

# PREDICTIONS -------------------------------------------------------------

pred_dt <- expand.grid(risk_age = seq(42.5, 97.5, by = 5), case_name_AD = 0, case_ascer_hosp = 0,
                       dementia_diagnosis_clinical_records = 0)

ds_predicts <- predict_mr_brt(ds_model, newdata = pred_dt, write_draws = T)

# GRAPHS ------------------------------------------------------------------

graph_mrbrt_results <- function(results, predicts){
  data_dt <- as.data.table(results$train_data)
  model_dt <- as.data.table(predicts$model_summaries)
  shapes <- c(15, 16, 18)
  y_lab <- "Relative Risk (log scale)"
  breaks <- seq(data_dt[, floor(min(log_mean))], data_dt[, ceiling(max(log_mean))])
  model_dt[Y_mean < 0, Y_mean := 0][Y_mean_hi < 0, Y_mean_hi := 0][Y_mean_lo < 0, Y_mean_lo := 0]
  
  data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
  gg_subset <- ggplot() +
    geom_point(data = data_dt, aes(x = risk_age, y = log_mean, color = as.factor(excluded), size = 1/log_se^2)) +
    geom_line(data = model_dt, aes(x = X_risk_age, y = Y_mean), color = "black") +
    geom_ribbon(data = model_dt, aes(x = X_risk_age, ymin = Y_mean_lo, ymax = Y_mean_hi), alpha = 0.05) +
    scale_fill_manual(guide = F, values = c("midnightblue", "purple")) +
    scale_y_continuous(name = y_lab, breaks = breaks, labels = round(exp(breaks), 2)) +
    scale_color_manual(name = "", values = c("blue", "red"), labels = c("Included", "Trimmed")) +
    scale_size_continuous(name = "Inverse Variance") +
    labs(x = "Age") +
    ggtitle(paste0("Meta-Analysis Results for RR of Down's Syndrome")) +
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
graph_mrbrt_results(ds_model, ds_predicts)
cov_plot(ds_model)
dev.off()

##########################################################################
### Author: USERNAME
### Date: 7/10/2019 
### Project: GBD Nonfatal Estimation
### Purpose: PD MR-BRT MODEL
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

pd_dir <- paste0("FILEPATH")
functions_dir <- paste0("FILEPATH")
mrbrt_helper_dir <- paste0("FILEPATH")
dem_cid <- ID
draws <- paste0("draw_", 0:999)

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

get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

floor_zero <- function(x){
  x[x < 0] <- 0
  return(x)
}

# GET DEMOGRAPHICS --------------------------------------------------------

age_dt <- get_age_metadata(12)
age_dt <- age_dt[age_group_id >=13]
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_years_end == 124, age_group_years_end := 99]
sex_dt <- get_ids(table = "sex")
loc_dt <- get_location_metadata(location_set_id = 22)

# GET DATA AND FORMAT -----------------------------------------------------

pd_dt <- as.data.table(read.xlsx(paste0("FILEPATH")))

## FILL OUT MISSING COLUMNS
pd_dt <- pd_dt[!(sample_size == 0 & measure == "prevalence")] ## GET RID OF SAMPLE SIZE ZERO ROWS
z <- qnorm(0.975)
pd_dt <- get_cases_sample_size(pd_dt)
pd_dt <- get_se(pd_dt)
pd_dt[is.na(standard_error), standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))] ## FORMULA FROM UPLOADER FOR STANDARD ERROR

## SELECT RELEVANT STUDIES AND STANDARDIZE CASE NAMES
pd_dt <- pd_dt[!case_name_dementia %in% c("DLB", "LBD", "lewy body dementia", "severe cognitive impairment")]
pd_dt[grepl("CI|cognitive", case_name_dementia), case_name_dementia := "mci"]
pd_dt[CI_withdementia == 1, case_name_dementia := paste0(case_name_dementia, "_plusdementia")]
pd_dt[case_name_dementia %in% c("parkinson's disease dementia", "PDD"), case_name_dementia := "pdd"]
pd_dt[case_name_dementia %in% c("Dementia", "Senile dementia of Alzheimer's type (SDAT)"), case_name_dementia := "dementia"]

pd_dt[measure %in% c("OR", "relative risk", "RR"), measure := "risk"]

## AGE AND SEX
sex_merge <- data.table(sex = c("Both", "Female", "Male"),
                        sex_code = c(0, 0.5, -0.5))
pd_dt <- merge(pd_dt, sex_merge, by = "sex")
pd_dt <- merge(pd_dt, sex_dt, by = "sex")
pd_dt[, risk_age := ifelse(is.na(mean.follow.up), mean.age, mean.age + mean.follow.up)]

## CALCULATE PREVALENCE RATIOS
pd_dt[is.na(mean), mean := cases/sample_size]
pd_dt <- pd_dt[!mean == 0]
pd_dt[, year_id := round((year_start+year_end)/2, 0)]
pd_dt[year_id<1990, year_id := 1990]
pd_dt$age_start_pull <- sapply(1:nrow(pd_dt), function(i) get_closest_age(i, var = "risk_age", dt = pd_dt))
pd_dt <- merge(pd_dt, age_dt[, .(age_group_years_start, age_group_id)], by.x = "age_start_pull", by.y = "age_group_years_start")
prev_dt <- get_outputs(topic = "cause", measure_id = 5, year_id = pd_dt[, unique(year_id)], location_id = pd_dt[, unique(location_id)],
                       age_group_id = pd_dt[, unique(age_group_id)], sex_id = pd_dt[, unique(sex_id)], cause_id = dem_cid,
                       gbd_round_id = 5, metric_id = 3)
pd_dt <- merge(pd_dt, prev_dt[, .(age_group_id, sex_id, location_id, year_id, val)], by = c("age_group_id", "sex_id", "location_id", "year_id"), all.x = T)
pd_dt[, rr := ifelse(measure == "prevalence", mean/val, mean)]

## COVS
pd_dt[, case_name_mci := as.numeric(case_name_dementia == "mci")]
pd_dt[, case_name_mci_plusdementia := as.numeric(case_name_dementia == "mci_plusdementia")]
pd_dt[, case_name_pdd := as.numeric(case_name_dementia == "pdd")]

all_covs <- c("risk_age", "case_name_mci", "case_name_mci_plusdementia",
              "case_name_pdd", "clinical_sample", "diagnosis_clinical_records_dementia")

## TRANSFORM DATA
pd_dt[, log_mean := log(rr)]
pd_dt$log_se <- sapply(1:nrow(pd_dt), function(i) {
  mean_i <- as.numeric(pd_dt[i, "mean"])
  se_i <- as.numeric(pd_dt[i, "standard_error"])
  deltamethod(~log(x1), mean_i, se_i^2)
})

## VIZUALIZE DATA
viz_data <- ggplot(pd_dt, aes(x = risk_age, y = log_mean, size = 1/log_se^2, color = sex)) +
  geom_point() +
  labs(x = "Age", y = "Log RR") +
  scale_color_manual(values = c("Both" = "midnightblue", "Female" = "purple", "Male" = "red"),
                     name = "Sex") +
  scale_size_continuous(name = "Inverse Variance") +
  theme_classic()

# RUN MODEL ---------------------------------------------------------------

create_covs <- function(x){
  if (x == "risk_age"){
    return(cov_info(x, "X", degree = 3, n_i_knots = 3, knot_placement_procedure = "frequency",
                    bspline_mono = "decreasing", bspline_gprior_mean = "0, 0, 0, 0", r_linear = F,
                    bspline_gprior_var = "1e-5, inf, inf, 1e-3"))
  } else if (x == "sex_code") {
    return(cov_info(x, "X"))
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

model_name <- paste0("pd_model_", date)

pd_model <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = model_name,
  data = pd_dt,
  mean_var = "log_mean",
  se_var = "log_se",
  covs = cov_list,
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1
)
readr::write_rds(pd_model, paste0("FILEPATH"))

# PREDICTIONS -------------------------------------------------------------

pred_dt <- expand.grid(risk_age = seq(42.5, 97.5, by = 5), sex_code = c(-0.5, 0.5), case_name_mci = 0,
                       case_name_mci_plusdementia = 0, case_name_pdd = 0, clinical_sample = 0, 
                       diagnosis_clinical_records_dementia = 0)

pd_predicts <- predict_mr_brt(pd_model, newdata = pred_dt, write_draws = T)

# GRAPHS ------------------------------------------------------------------

graph_mrbrt_results <- function(results, predicts, sex = T){
  data_dt <- as.data.table(results$train_data)
  model_dt <- as.data.table(predicts$model_draws)
  model_dt[, (draws) := lapply(.SD, floor_zero), .SDcols = draws]
  model_dt <- summaries(model_dt, draws)
  if (sex == T){
    model_dt[, sex := ifelse(X_sex_code == 0.5, "Female", "Male")]
  } 
  shapes <- c(15, 16, 18)
  y_lab <- "Relative Risk (log scale)"
  breaks <- seq(data_dt[, floor(min(log_mean))], data_dt[, ceiling(max(log_mean))])
  
  data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
  gg_subset <- ggplot() +
    geom_point(data = data_dt, aes(x = risk_age, y = log_mean, color = as.factor(excluded), size = 1/log_se^2)) +
    scale_fill_manual(guide = F, values = c("midnightblue", "purple")) +
    scale_y_continuous(name = y_lab, breaks = breaks, labels = round(exp(breaks), 2)) +
    scale_size_continuous(name = "Inverse Variance") +
    labs(x = "Age") +
    ggtitle(paste0("Meta-Analysis Results for RR of PD")) +
    theme_classic() +
    geom_hline(yintercept = 0) +
    theme(text = element_text(size = 15, color = "black"))
  if (sex == T){
    gg_subset <- gg_subset +
      geom_smooth(data = model_dt, aes(x = X_risk_age, y = mean, color = sex)) +
      geom_ribbon(data = model_dt, aes(x = X_risk_age, ymin = lower, ymax = upper, fill = sex), alpha = 0.05) +
      scale_color_manual(name = "", values = c("blue", "red", "midnightblue", "purple"), 
                         labels = c("Included", "Trimmed", "Female", "Male"))
  } else if (sex == F){
    gg_subset <- gg_subset +
      geom_smooth(data = model_dt, aes(x = X_risk_age, y = mean), color = "black") +
      geom_ribbon(data = model_dt, aes(x = X_risk_age, ymin = lower, ymax = upper), alpha = 0.05) +
      scale_color_manual(name = "", values = c("blue", "red"), 
                         labels = c("Included", "Trimmed"))
  }
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
graph_mrbrt_results(pd_model, pd_predicts, sex = F)
cov_plot(pd_model)
dev.off()
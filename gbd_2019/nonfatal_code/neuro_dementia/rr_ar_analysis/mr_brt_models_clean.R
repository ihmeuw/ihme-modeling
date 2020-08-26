##########################################################################
### Project: GBD Nonfatal Estimation
### Purpose: MR-BRT Models for Dementia Mortality
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
library(msm, lib.loc = paste0("FILEPATH"))
library(mortcore, lib = "FILEPATH")
library(mortdb, lib = "FILEPATH")
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------

mrbrt_dir <- paste0("FILEPATH")
emr_dir <- paste0("FILEPATH")
plot_dir <- paste0("FILEPATH")
functions_dir <- paste0("FILEPATH")
helper_dir <- paste0("FILEPATH")
eta <- 0.000001

# SOURCE FUNCTIONS --------------------------------------------------------

functs <- c("run_mr_brt_function", "check_for_outputs_function", "check_for_preds_function", "cov_info_function",
            "load_mr_brt_outputs_function", "load_mr_brt_preds_function", "predict_mr_brt_function")
for (funct in functs){
  source(paste0(helper_dir, funct, ".R"))
}

source(paste0(functions_dir, "get_age_metadata.R"))
source(paste0(functions_dir, "get_envelope.R"))
source(paste0(functions_dir, "get_ids.R"))
source(paste0(functions_dir, "get_population.R"))

## FUNCTION TO GRAPH MR-BRT RESULTS
graph_mrbrt_results <- function(results, predicts, sex = T, rr = T, label = ""){
  data_dt <- as.data.table(results$train_data)
  model_dt <- as.data.table(predicts$model_summaries)
  if (sex == T){
    model_dt[X_sex_cov == -0.5, sex := "Male"][X_sex_cov == 0.5, sex := "Female"]
  }
  shapes <- c(15, 16, 18)
  if (rr == T){
    data_dt <- data_dt[!log_effect_size < 0]
    y_lab <- "Relative Risk (log scale)"
    breaks <- c(0:3)
  } else if (rr == F){
    y_lab <- "Absolute Risk (log scale)"
    breaks <- seq(-8, 0, by = 2)
  }
  data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
  gg_subset <- ggplot() +
    geom_point(data = data_dt, aes(x = risk_age, y = log_effect_size, color = as.factor(excluded), shape = as.factor(sex)), size = 3) +
    scale_fill_manual(guide = F, values = c("midnightblue", "purple")) +
    scale_y_continuous(name = y_lab, breaks = breaks, labels = round(exp(breaks), 2)) +
    scale_shape_manual(name = "Sex", values = shapes) +
    labs(x = "Age") +
    ggtitle(paste0("Meta-Analysis Results ", label)) +
    theme_classic() +
    theme(text = element_text(size = 15, color = "black"))
  if (sex == T){
    gg_subset <- gg_subset +
      geom_smooth(data = model_dt, aes(x = X_risk_age, y = Y_mean, color = sex)) +
      geom_ribbon(data = model_dt, aes(x = X_risk_age, ymin = Y_mean_lo, ymax = Y_mean_hi, fill = sex), alpha = 0.05) +
      scale_color_manual(name = "", values = c("blue", "red", "midnightblue", "purple"),
                         labels = c("Included", "Trimmed", "Female", "Male"))
  } else if (sex == F){
    gg_subset <- gg_subset +
      geom_smooth(data = model_dt, aes(x = X_risk_age, y = Y_mean), color = "black") +
      geom_ribbon(data = model_dt, aes(x = X_risk_age, ymin = Y_mean_lo, ymax = Y_mean_hi), alpha = 0.05) +
      scale_color_manual(name = "", values = c("blue", "red"),
                         labels = c("Included", "Trimmed"))
  }
  return(gg_subset)
}

## FUNCTION TO GET MORTALITY DATA (TO CALCULATE AR)
get_mort_data <- function(i){
  sex <- sex_names[sex == arlit_dt[, sex][i], sex_id]
  first_id <- age_dt[, age_group_id][findInterval(arlit_dt[, age_start][i], vec = start_years)]
  last_id <- age_dt[, age_group_id][findInterval(arlit_dt[, age_end][i], vec = end_years)]
  ages <- age_dt[age_group_id >= first_id & age_group_id <= last_id, age_group_id]
  if (length(ages) == 0) ages <- first_id
  mort_path <- paste0("/share/mortality/reckoning/", version_id, "/envelope_hivdel/result/combined_env_aggregated_", arlit_dt[, midyear][i], ".h5")
  mort_dt <- as.data.table(mortcore::load_hdf(filepath = mort_path, by_val = arlit_dt[, location_id][i]))
  mort_dt <- mort_dt[age_group_id %in% ages & sex_id == sex & year_id == arlit_dt[, midyear][i]]
  pop_dt <- copy(pop[age_group_id %in% ages & sex_id == sex
                     & year_id == arlit_dt[, midyear][i] & location_id == arlit_dt[, location_id][i]])
  mort_dt <- merge(mort_dt, pop, by = c("age_group_id", "location_id", "year_id", "sex_id"))
  mort_dt[, `:=` (deaths = sum(deaths), population = sum(population)), by = draw]
  mort_dt <- unique(mort_dt, by = "draw")
  mort_dt[, death_rate := deaths/population]
  new_row <- data.table(id_num = i, mort = mort_dt[, mean(death_rate)], mort_se = mort_dt[, sd(death_rate)])
  return(new_row)
}

# GET METADATA ------------------------------------------------------------

age_dt <- get_age_metadata(12)
age_dt <- age_dt[age_group_id >=13]
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_years_end == 124, age_group_years_end := 99]
start_years <- age_dt[, age_group_years_start]
end_years <- age_dt[, age_group_years_end]
version_id <- get_envelope(age_group_id = 30, location_id = 102, year_id = 2010, sex_id = 2, ## GET MORTALITY VERSION
                           decomp_step = 'iterative', with_hiv = 0)
version_id <- version_id[, unique(run_id)]
sex_names <- get_ids(table = "sex")

# SET UP DATA -------------------------------------------------------------

lit_dt <- fread(paste0(emr_dir, "formatted_data.csv"))
lit_dt[effect_size == 0, effect_size := effect_size + eta]
lit_dt <- lit_dt[!effect_size == 0] ## GET RID OF THIS POINT BECAUSE IS EXTREME OUTLIER- MESSING UP MODEL FIT
rrlit_dt <- copy(lit_dt)
rrlit_dt[, log_effect_size := log(effect_size)]
rrlit_dt[, se := (upper - lower) / 3.92]
## DELTA TRANSFORMATION FOR THE CONVERSION OF STANDARD ERROR
rrlit_dt$log_se <- sapply(1:nrow(rrlit_dt), function(i){
  mean <- rrlit_dt[i, effect_size]
  se <- rrlit_dt[i, se]
  deltamethod(~log(x1), mean, se^2)
})

## CONVERT TO AR
arlit_dt <- copy(lit_dt)
arlit_dt[, id_num := 1:.N]
arlit_dt[, midyear := round((year_end + year_start)/2, 0)]
pop <- get_population(age_group_id = age_dt[, unique(age_group_id)], location_id = arlit_dt[, unique(location_id)], year_id = arlit_dt[, unique(midyear)],
                      sex_id = 1:3, decomp_step = "step1")
mort_dt <- rbindlist(lapply(1:nrow(arlit_dt), get_mort_data))
arlit_dt <- merge(arlit_dt, mort_dt, by = "id_num")
arlit_dt[, se := (upper - lower) / 3.92]
arlit_dt[, ar := (effect_size-1)*mort]
arlit_dt <- arlit_dt[!ar < 0]
arlit_dt[, ar_se := sqrt(se*mort_se + se*mort + effect_size*mort_se)]
arlit_dt[, log_effect_size := log(ar)]
arlit_dt$log_se <- sapply(1:nrow(arlit_dt), function(i){
  mean <- rrlit_dt[i, effect_size]
  se <- rrlit_dt[i, se]
  deltamethod(~log(x1), mean, se^2)
})

## GET PREDICTION MATRICES (WITH AND WITHOUT SEX)
pred_dt_sex <- expand.grid(risk_age = seq(42.5, 97.5, by = 5), sex_cov = c(-0.5, 0.5), CI = 0, vascular = 0,
                           clinical_sample = 0, control_education = 0, control_cd_basic = 0, control_cd_advanced = 0,
                           control_lifestyle = 0, overcontrol = 0)
pred_dt <- expand.grid(risk_age = seq(42.5, 97.5, by = 5), CI = 0, vascular = 0,
                       clinical_sample = 0, control_education = 0, control_cd_basic = 0, control_cd_advanced = 0,
                       control_lifestyle = 0, overcontrol = 0)


# RR MR-BRT MODEL ---------------------------------------------------------

rr_name <- paste0("rr_", date)

rr_model <- run_mr_brt(
  output_dir = mrbrt_dir,
  model_label = rr_name,
  data = rrlit_dt,
  mean_var = "log_effect_size",
  se_var = "log_se",
  covs = list(cov_info("risk_age", "X", degree = 3, bspline_mono = "decreasing",
                       i_knots = paste(rrlit_dt[, quantile(risk_age, prob = seq(0, 1, by = 0.2))][2:5], collapse = ", "),
                       r_linear = T, l_linear = T, bspline_gprior_mean = "0, 0, 0, 0, 0",
                       bspline_gprior_var = "1e-5, inf, inf, inf, 1e-5"),
              cov_info("CI", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("CI", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("vascular", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("vascular", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("clinical_sample", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("clinical_sample", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("control_education", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("control_education", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("control_cd_basic", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("control_cd_basic", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("control_cd_advanced", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("control_cd_advanced", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("control_lifestyle", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("control_lifestyle", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("overcontrol", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("overcontrol", "Z", gprior_mean = 0, gprior_var = 0.01)),
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1
)

rr_model_pred <- predict_mr_brt(rr_model, newdata = pred_dt, write_draws = T)

pdf(paste0("FILEPATH"))
graph_mrbrt_results(rr_model, rr_model_pred, sex = F)
dev.off()

# AR MR-BRT MODEL ---------------------------------------------------------

ar_name <- paste0("ar_", date)

ar_model <- run_mr_brt(
  output_dir = mrbrt_dir,
  model_label = ar_name,
  data = arlit_dt,
  mean_var = "log_effect_size",
  se_var = "log_se",
  covs = list(cov_info("risk_age", "X", degree = 3, bspline_mono = "increasing",
                       i_knots = paste(rrlit_dt[, quantile(risk_age, prob = seq(0, 1, by = 0.2))][2:5], collapse = ", "),
                       r_linear = T, l_linear = T, bspline_gprior_mean = "0, 0, 0, 0, 0",
                       bspline_gprior_var = "inf, inf, inf, inf, inf"),
              cov_info("CI", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("CI", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("vascular", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("vascular", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("clinical_sample", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("clinical_sample", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("control_education", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("control_education", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("control_cd_basic", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("control_cd_basic", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("control_cd_advanced", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("control_cd_advanced", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("control_lifestyle", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("control_lifestyle", "Z", gprior_mean = 0, gprior_var = 0.01),
              cov_info("overcontrol", "X", gprior_mean = 0, gprior_var = 0.01), cov_info("overcontrol", "Z", gprior_mean = 0, gprior_var = 0.01)),
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1
)

ar_model_pred <- predict_mr_brt(ar_model, newdata = pred_dt, write_draws = T)

pdf(paste0("FILEPATH"))
graph_mrbrt_results(ar_model, ar_model_pred, rr = F, sex = F)
dev.off()

# RR -----------------------------------------------------------------

run_rr_lambda <- function(lambda){
  rr_name <- paste0("rr_lambda", lambda, "_", date)

  rr_model <- run_mr_brt(
    output_dir = test_dir,
    model_label = rr_name,
    data = rrlit_dt,
    mean_var = "log_effect_size",
    se_var = "log_se",
    covs = list(cov_info("risk_age", "X", degree = 3, bspline_mono = "decreasing",
                         i_knots = paste(rrlit_dt[, quantile(risk_age, prob = seq(0, 1, by = 0.2))][2:5], collapse = ", "),
                         r_linear = T, l_linear = T, bspline_gprior_mean = "0, 0, 0, 0, 0",
                         bspline_gprior_var = "1e-5, inf, inf, inf, 1e-5"),
                cov_info("CI", "X"), cov_info("CI", "Z"),
                cov_info("vascular", "X"), cov_info("vascular", "Z"),
                cov_info("clinical_sample", "X"), cov_info("clinical_sample", "Z"),
                cov_info("control_education", "X"), cov_info("control_education", "Z"),
                cov_info("control_cd_basic", "X"), cov_info("control_cd_basic", "Z"),
                cov_info("control_cd_advanced", "X"), cov_info("control_cd_advanced", "Z"),
                cov_info("control_lifestyle", "X"), cov_info("control_lifestyle", "Z"),
                cov_info("overcontrol", "X"), cov_info("overcontrol", "Z")),
    study_id = "id",
    method = "trim_maxL",
    trim_pct = 0.1,
    lasso = T,
    lambda_multiplier = lambda
  )

  rr_model_pred <- predict_mr_brt(rr_model, newdata = pred_dt)

  return(list(lambda, rr_model, rr_model_pred))
}

rrlit_dt <- rrlit_dt[log_se < 1]
test_dir <- paste0("FILEPATH")
lambdas <- c(0.0001, 0.0005, 0.001, 0.005, seq(0.01, 0.10, by = 0.02), 0.1)
rr_lambda_tests <- lapply(lambdas, run_rr_lambda)

pdf(paste0(test_dir, "rr_lambda_tests.pdf"))
lapply(1:length(lambdas), function(x)
  graph_mrbrt_results(rr_lambda_tests[[x]][[2]], rr_lambda_tests[[x]][[3]], label = paste0("Lambda: ", lambdas[x]), sex = F))
dev.off()

cov_plot <- function(n){
  dt <- as.data.table(rr_lambda_tests[[n]][[2]][[4]])
  lambda <- lambdas[n]
  graph_dt <- copy(dt)
  graph_dt <- graph_dt[!grepl("risk_age", x_cov)]
  graph_dt[, `:=` (lower = beta_soln - 1.96*sqrt(beta_var), upper = beta_soln + 1.96*sqrt(beta_var))]
  cov_plot <- ggplot(graph_dt, aes(x = beta_soln, y = as.factor(x_cov))) +
    geom_vline(xintercept = 0, linetype = "dotted", color = "midnightblue") +
    geom_point(color = "blue") +
    geom_errorbarh(aes(xmin = lower, xmax = upper), color = "blue") +
    labs(x = "Log Effect Size", y = "Variable") +
    ggtitle(paste0("Effect Sizes for Lambda = ", lambda)) +
    theme_classic()
  return(cov_plot)
}

pdf(paste0(test_dir, "coef_plots.pdf"))
lapply(1:length(lambdas), cov_plot)
dev.off()

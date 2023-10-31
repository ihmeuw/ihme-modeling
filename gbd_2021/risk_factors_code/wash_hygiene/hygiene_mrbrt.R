## RRs & evidence score for no access to handwashing station
## 9/4/20

library(mrbrt001, lib.loc = "FILEPATH")

hyg_rr <- data.table(read.xlsx("FILEPATH"))[is_outlier == 0]
hyg_rr <- hyg_rr[, .(nid, field_citation_value, source_type, location_name, location_id, ihme_loc_id, cause, year_start, year_end, age_start, age_end,
                     measure, mean, lower, upper, standard_error, int_cases, int_sample_size, control_cases, control_sample_size, 
                     int_episodes_person_time, int_total_person_time, control_episodes_person_time, control_total_person_time,
                     cohort_cases_person_years, cohort_total_person_years, cv_exposure_population, cv_exposure_selfreport, cv_exposure_study,
                     cv_outcome_selfreport, cv_outcome_unblinded, cv_reverse_causation, cv_confounding_nonrandom, cv_confounding_uncontrolled,
                     cv_selection_bias, is_outlier, inverse_risk)]
hyg_rr[cv_selection_bias == "undefined" | is.na(cv_selection_bias), cv_selection_bias := 1]
cols <- c("age_start","age_end","mean","lower","upper","standard_error","int_cases","int_sample_size",
          "control_cases","control_sample_size","int_episodes_person_time","int_total_person_time",
          "control_episodes_person_time","control_total_person_time","is_outlier")
cv_cols <- names(hyg_rr)[names(hyg_rr) %like% "cv_"]
hyg_rr[, c(cols,cv_cols) := lapply(.SD, as.numeric), .SDcols = c(cols,cv_cols)]

### some RRs/CIs were not reported - these are functions to calculate them
## calculate RR
# input: dt - extraction sheet
# input: type - either "cumulative incidence" (sample size & cases reported) or "incidence rate" (person-time and episodes per person-time reported)
calc_rr <- function(dt, type) {
  if (type == "cumulative incidence") {
    a <- dt$int_cases %>% as.numeric
    b <- dt$int_sample_size %>% as.numeric
    c <- dt$control_cases %>% as.numeric
    d <- dt$control_sample_size %>% as.numeric
    
  } else if (type == "incidence rate") {
    a <- dt$int_episodes_person_time %>% as.numeric
    b <- dt$int_total_person_time %>% as.numeric
    c <- dt$control_episodes_person_time %>% as.numeric
    d <- dt$control_total_person_time %>% as.numeric
  }
  
  rr <- (a/b)/(c/d)
  
  return(round(rr, 3))
}

## calculate 95% CI & standard error
# input: dt - extraction sheet
# input: type - either "cumulative incidence" (sample size & cases reported) or "incidence rate" (person-time and episodes per person-time reported)
calc_uncertainty <- function(dt, type) {
  if (type == "cumulative incidence") {
    
    a <- dt$int_cases %>% as.numeric
    b <- dt$int_sample_size %>% as.numeric
    c <- dt$control_cases %>% as.numeric
    d <- dt$control_sample_size %>% as.numeric
    
    log_rr <- log((a/b)/(c/d))
    log_se <- sqrt((1/a) + (1/c) - (1/(b)) - (1/(d)))

  } else if (type == "incidence rate") {
    
    a <- dt$int_episodes_person_time %>% as.numeric
    b <- dt$int_total_person_time %>% as.numeric
    c <- dt$control_episodes_person_time %>% as.numeric
    d <- dt$control_total_person_time %>% as.numeric
    
    log_rr <- log((a/b)/(c/d))
    log_se <- sqrt((1/a) + (1/c))

  }
  
  lower <- exp(log_rr - qnorm(0.975)*log_se)
  upper <- exp(log_rr + qnorm(0.975)*log_se)
  standard_error <- log_to_linear(array(log_rr), array(log_se))[[2]]

  return(list(lower = round(lower, 3), upper = round(upper, 3), standard_error = standard_error))
}

### some RRs were reported as risk of getting outcome if person did NOT wash hands - need to invert them
invert_risk <- function(mean, lower, upper) {
  rr <- 1/mean
  rr_lower <- 1/upper
  rr_upper <- 1/lower
  
  return(list(mean = round(rr, 3), lower = round(rr_lower, 3), upper = round(rr_upper, 3)))
}

### LRI #####
hyg_lri <- hyg_rr[cause == "lri"]
# calculate RR and uncertainty for studies w/o them that report person-time and episodes per person-time
# some studies have both sample size/cases AND person-time/episodes - defaulting to using the latter to calculate RR & uncertainty
calc_rr_incidence_rate <- hyg_lri[(is.na(mean) | is.na(lower)) & 
                                    (!is.na(int_episodes_person_time) & !is.na(int_total_person_time) & 
                                       !is.na(control_episodes_person_time) & !is.na(control_total_person_time))]
hyg_lri[(is.na(mean) | is.na(lower)) & 
          (!is.na(int_episodes_person_time) & !is.na(int_total_person_time) & !is.na(control_episodes_person_time) & !is.na(control_total_person_time)),
        `:=` (mean = calc_rr(calc_rr_incidence_rate, type = "incidence rate"),
              lower = calc_uncertainty(calc_rr_incidence_rate, type = "incidence rate")$lower,
              upper = calc_uncertainty(calc_rr_incidence_rate, type = "incidence rate")$upper,
              standard_error = calc_uncertainty(calc_rr_incidence_rate, type = "incidence rate")$standard_error)]
# calculate RR and uncertainty for studies w/o them that report sample size and cases
calc_rr_cumulative_incidence <- hyg_lri[(is.na(mean) | is.na(lower)) &
                                          (!is.na(int_cases) & !is.na(int_sample_size) & !is.na(control_cases) & !is.na(control_sample_size))]
hyg_lri[(is.na(mean) | is.na(lower)) &
          (!is.na(int_cases) & !is.na(int_sample_size) & !is.na(control_cases) & !is.na(control_sample_size)), 
        `:=` (mean = calc_rr(calc_rr_cumulative_incidence, type = "cumulative incidence"),
              lower = calc_uncertainty(calc_rr_cumulative_incidence, type = "cumulative incidence")$lower,
              upper = calc_uncertainty(calc_rr_cumulative_incidence, type = "cumulative incidence")$upper,
              standard_error = calc_uncertainty(calc_rr_cumulative_incidence, type = "cumulative incidence")$standard_error)]
# invert RRs reported as risk of getting outcome if person did NOT wash hands
hyg_lri[inverse_risk == 1, `:=` (mean = invert_risk(mean, lower, upper)$mean,
                                 lower = invert_risk(mean, lower, upper)$lower,
                                 upper = invert_risk(mean, lower, upper)$upper)]

# calc se
hyg_lri[, standard_error := log_to_linear(array(log(mean)), array((log(upper)-log(lower))/(2*qnorm(0.975))))[[2]]]

# transform to log space
hyg_lri[, log_rr := linear_to_log(array(mean), array(standard_error))[[1]]]
hyg_lri[, log_se := linear_to_log(array(mean), array(standard_error))[[2]]]

# change categorical vars to binary
hyg_lri[, cv_confounding_uncontrolled_1 := ifelse(cv_confounding_uncontrolled == 1, 1, 0)]
hyg_lri[, cv_confounding_uncontrolled_2 := ifelse(cv_confounding_uncontrolled == 2, 1, 0)]
hyg_lri[, cv_selection_bias_1 := ifelse(cv_selection_bias == 1, 1, 0)]
hyg_lri[, cv_selection_bias_2 := ifelse(cv_selection_bias == 2, 1, 0)]
hyg_lri[, c("cv_confounding_uncontrolled","cv_selection_bias") := NULL]

# subset to only covs that have variation (i.e. >1 unique value)
cv_cols <- names(hyg_lri)[names(hyg_lri) %like% "cv_"]
cols_delete <- c()
for (col in cv_cols) {
  if (length(hyg_lri[, unique(get(col))]) == 1) {
    print(col)
    cols_delete <- append(cols_delete, col)
  }
}
hyg_lri <- hyg_lri[, -cols_delete, with = F]

# save
write.csv(hyg_lri, "FILEPATH", row.names = F)

# select covariates with CovFinder
covs_lri <- names(hyg_lri)[names(hyg_lri) %like% "cv_"]

hyg_lri_covs <- MRData()
hyg_lri_covs$load_df(
  data = hyg_lri, col_obs = "log_rr", col_obs_se = "log_se",
  col_covs = as.list(covs_lri), col_study_id = "nid"
)

hyg_lri_covfinder <- CovFinder(
  data = hyg_lri_covs,
  covs = as.list(covs_lri),
  pre_selected_covs = list("intercept"),
  normalized_covs = FALSE,
  num_samples = 1000L,
  power_range = list(-4,4),
  power_step_size = 1,
  laplace_threshold = 1e-5
)

hyg_lri_covfinder$select_covs(verbose = FALSE)
hyg_lri_covs_final <- hyg_lri_covfinder$selected_covs # intercept, cv_confounding_nonrandom, cv_selection_bias_2

# run model
hyg_lri_data <- MRData()
hyg_lri_data$load_df(
  data = hyg_lri, col_obs = "log_rr", col_obs_se = "log_se",
  col_covs = list("cv_confounding_nonrandom","cv_selection_bias_2"), 
  col_study_id = "nid"
)

hyg_lri_model <- MRBRT(
  data = hyg_lri_data,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE),
    LinearCovModel("cv_confounding_nonrandom"),
    LinearCovModel("cv_selection_bias_2")),
  inlier_pct = 0.9
)

# save model object
py_save_object(object = hyg_lri_model, filename = "FILEPATH", pickle = "dill")
# load model object
hyg_lri_model <- py_load_object(filename = "FILEPATH", pickle = "dill")

hyg_lri_model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

# predict
hyg_lri_pred_df <- expand.grid(intercept = 1,
                               cv_confounding_nonrandom = c(0,1), cv_selection_bias_2 = c(0,1))

hyg_lri_pred_data <- MRData()
hyg_lri_pred_data$load_df(
  data = hyg_lri_pred_df,
  col_covs = list("intercept",
                  "cv_confounding_nonrandom","cv_selection_bias_2")
)

hyg_lri_pred <- hyg_lri_model$predict(data = hyg_lri_pred_data)

hyg_lri_samples <- hyg_lri_model$sample_soln(sample_size = 1000L)
hyg_lri_draws <- hyg_lri_model$create_draws(
  data = hyg_lri_pred_data,
  beta_samples = hyg_lri_samples[[1]],
  gamma_samples = hyg_lri_samples[[2]],
  random_study = TRUE
)

hyg_lri_pred_df$pred1 <- hyg_lri_pred %>% exp
hyg_lri_pred_df$pred1_lo <- apply(hyg_lri_draws, 1, function(x) quantile(x, 0.025)) %>% exp
hyg_lri_pred_df$pred1_hi <- apply(hyg_lri_draws, 1, function(x) quantile(x, 0.975)) %>% exp

### diarrhea #####
hyg_dia <- hyg_rr[cause == "Diarrheal diseases"]
# calculate RR and uncertainty for studies w/o them that report person-time and episodes per person-time
# some studies have both sample size/cases AND person-time/episodes - defaulting to using the latter to calculate RR & uncertainty
calc_rr_incidence_rate <- hyg_dia[(is.na(mean) | is.na(lower)) & 
                                    (!is.na(int_episodes_person_time) & !is.na(int_total_person_time) & 
                                       !is.na(control_episodes_person_time) & !is.na(control_total_person_time))]
hyg_dia[(is.na(mean) | is.na(lower)) & 
          (!is.na(int_episodes_person_time) & !is.na(int_total_person_time) & !is.na(control_episodes_person_time) & !is.na(control_total_person_time)),
        `:=` (mean = calc_rr(calc_rr_incidence_rate, type = "incidence rate"),
              lower = calc_uncertainty(calc_rr_incidence_rate, type = "incidence rate")[[1]],
              upper = calc_uncertainty(calc_rr_incidence_rate, type = "incidence rate")[[2]])]
# calculate RR and uncertainty for studies w/o them that report sample size and cases
calc_rr_cumulative_incidence <- hyg_dia[(is.na(mean) | is.na(lower)) &
                                          (!is.na(int_cases) & !is.na(int_sample_size) & !is.na(control_cases) & !is.na(control_sample_size))]
hyg_dia[(is.na(mean) | is.na(lower)) &
          (!is.na(int_cases) & !is.na(int_sample_size) & !is.na(control_cases) & !is.na(control_sample_size)), 
        `:=` (mean = calc_rr(calc_rr_cumulative_incidence, type = "cumulative incidence"),
              lower = calc_uncertainty(calc_rr_cumulative_incidence, type = "cumulative incidence")[[1]],
              upper = calc_uncertainty(calc_rr_cumulative_incidence, type = "cumulative incidence")[[2]])]
# invert RRs reported as risk of getting outcome if person did NOT wash hands
hyg_dia[inverse_risk == 1, `:=` (mean = invert_risk(mean, lower, upper)$mean,
                                 lower = invert_risk(mean, lower, upper)$lower,
                                 upper = invert_risk(mean, lower, upper)$upper)]

# calc se
hyg_dia[, standard_error := log_to_linear(array(log(mean)), array((log(upper)-log(lower))/(2*qnorm(0.975))))[[2]]]

# transform to log space
hyg_dia[, log_rr := linear_to_log(array(mean), array(standard_error))[[1]]]
hyg_dia[, log_se := linear_to_log(array(mean), array(standard_error))[[2]]]

# change categorical vars to binary
hyg_dia[, cv_confounding_uncontrolled_1 := ifelse(cv_confounding_uncontrolled == 1, 1, 0)]
hyg_dia[, cv_confounding_uncontrolled_2 := ifelse(cv_confounding_uncontrolled == 2, 1, 0)]
hyg_dia[, cv_selection_bias_1 := ifelse(cv_selection_bias == 1, 1, 0)]
hyg_dia[, cv_selection_bias_2 := ifelse(cv_selection_bias == 2, 1, 0)]
hyg_dia[, c("cv_confounding_uncontrolled","cv_selection_bias") := NULL]

# subset to only covs that have variation (i.e. >1 unique value)
cv_cols <- names(hyg_dia)[names(hyg_dia) %like% "cv_"]
cols_delete <- c()
for (col in cv_cols) {
  if (length(hyg_dia[, unique(get(col))]) == 1) {
    print(col)
    cols_delete <- append(cols_delete, col)
  }
}
hyg_dia <- hyg_dia[, -cols_delete, with = F]

# save
write.csv(hyg_dia, "FILEPATH", row.names = F)

# run model without covariates
hyg_dia_data_basic <- MRData()
hyg_dia_data_basic$load_df(
  data = hyg_dia, col_obs = "log_rr", col_obs_se = "log_se",
  col_covs = list(), col_study_id = "nid"
)

hyg_dia_model_basic <- MRBRT(
  data = hyg_dia_data_basic,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE)),
  inliner_pct = 0.9
)

hyg_dia_model_basic$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

# predict
hyg_dia_pred_df_basic <- data.frame(intercept = 1)

hyg_dia_pred_data_basic <- MRData()
hyg_dia_pred_data_basic$load_df(
  data = hyg_dia_pred_df_basic,
  col_covs = list("intercept")
)

hyg_dia_pred_basic <- hyg_dia_model_basic$predict(data = hyg_dia_pred_data_basic)

hyg_dia_samples_basic <- hyg_dia_model_basic$sample_soln(sample_size = 1000L)
hyg_dia_draws_basic <- hyg_dia_model_basic$create_draws(
  data = hyg_dia_pred_data_basic,
  beta_samples = hyg_dia_samples_basic[[1]],
  gamma_samples = hyg_dia_samples_basic[[2]],
  random_study = TRUE
)

hyg_dia_pred_df_basic$pred1 <- hyg_dia_pred_basic %>% exp
hyg_dia_pred_df_basic$pred1_lo <- apply(hyg_dia_draws_basic, 1, function(x) quantile(x, 0.025)) %>% exp
hyg_dia_pred_df_basic$pred1_hi <- apply(hyg_dia_draws_basic, 1, function(x) quantile(x, 0.975)) %>% exp

# select covariates with CovFinder
covs_dia <- names(hyg_dia)[names(hyg_dia) %like% "cv_"]

hyg_dia_covs <- MRData()
hyg_dia_covs$load_df(
  data = hyg_dia, col_obs = "log_rr", col_obs_se = "log_se",
  col_covs = as.list(covs_dia), col_study_id = "nid"
)

hyg_dia_covfinder <- CovFinder(
  data = hyg_dia_covs,
  covs = as.list(covs_dia),
  pre_selected_covs = list("intercept"),
  normalized_covs = FALSE,
  num_samples = 1000L,
  power_range = list(-4,4),
  power_step_size = 1,
  laplace_threshold = 1e-5
)

hyg_dia_covfinder$select_covs(verbose = FALSE)
hyg_dia_covs_final <- hyg_dia_covfinder$selected_covs # intercept, cv_confounding_nonrandom, cv_selection_bias_2, cv_exposure_selfreport

# run model
hyg_dia_data <- MRData()
hyg_dia_data$load_df(
  data = hyg_dia, col_obs = "log_rr", col_obs_se = "log_se",
  col_covs = list("cv_confounding_nonrandom","cv_selection_bias_2"), 
  col_study_id = "nid"
)

hyg_dia_model <- MRBRT(
  data = hyg_dia_data,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE),
    LinearCovModel("cv_confounding_nonrandom"),
    LinearCovModel("cv_selection_bias_2")),
  inlier_pct = 0.9
)

# save model object
py_save_object(object = hyg_dia_model, filename = "FILEPATH", pickle = "dill")
# load model object
hyg_dia_model <- py_load_object(filename = "FILEPATH", pickle = "dill")

hyg_dia_model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

# predict
hyg_dia_pred_df <- expand.grid(intercept = 1,
                               cv_confounding_nonrandom = c(0,1), cv_selection_bias_2 = c(0,1))

hyg_dia_pred_data <- MRData()
hyg_dia_pred_data$load_df(
  data = hyg_dia_pred_df,
  col_covs = list("intercept",
                  "cv_confounding_nonrandom","cv_selection_bias_2")
)

hyg_dia_pred <- hyg_dia_model$predict(data = hyg_dia_pred_data)

hyg_dia_samples <- hyg_dia_model$sample_soln(sample_size = 1000L)
hyg_dia_draws <- hyg_dia_model$create_draws(
  data = hyg_dia_pred_data,
  beta_samples = hyg_dia_samples[[1]],
  gamma_samples = hyg_dia_samples[[2]],
  random_study = TRUE
)

hyg_dia_pred_df$pred1 <- hyg_dia_pred %>% exp
hyg_dia_pred_df$pred1_lo <- apply(hyg_dia_draws, 1, function(x) quantile(x, 0.025)) %>% exp
hyg_dia_pred_df$pred1_hi <- apply(hyg_dia_draws, 1, function(x) quantile(x, 0.975)) %>% exp


##### FORMAT FOR SAVE RESULTS #####
rr_cols <- paste0("rr_", 0:999)

# diarrhea
dia_draws_final <- data.table(hyg_dia_draws)[1]
setnames(dia_draws_final, names(dia_draws_final), rr_cols)
dia_draws_final[, (rr_cols) := lapply(.SD, function(x) 1/exp(x)), .SDcols = rr_cols]
dia_draws_final[, rr_mean := rowMeans(.SD), .SDcols = rr_cols]
dia_draws_final[, rr_lower := apply(.SD, 1, quantile, 0.025), .SDcols = rr_cols]
dia_draws_final[, rr_upper := apply(.SD, 1, quantile, 0.975), .SDcols = rr_cols]
dia_draws_final[, sd := sd(.SD), .SDcols = rr_cols]
dia_draws_final[, parameter := "cat1"]
dia_draws_final <- rbind(dia_draws_final, data.table(parameter = "cat2", rr_mean = 1, rr_lower = 1, rr_upper = 1, sd = 0), fill = TRUE)
dia_draws_final[parameter == "cat2", (rr_cols) := 1]
# males
dia_df_m <- expand.grid(cause_id = 302, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 1, 
                        mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = c("cat1","cat2"))
dia_df_m <- merge(dia_df_m, dia_draws_final, by = "parameter")
setDT(dia_df_m)
# females
dia_df_f <- expand.grid(cause_id = 302, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 2, 
                        mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = c("cat1","cat2"))
dia_df_f <- merge(dia_df_f, dia_draws_final, by = "parameter")
setDT(dia_df_f)

# lri
lri_draws_final <- data.table(hyg_lri_draws)[1]
setnames(lri_draws_final, names(lri_draws_final), rr_cols)
lri_draws_final[, (rr_cols) := lapply(.SD, function(x) 1/exp(x)), .SDcols = rr_cols]
lri_draws_final[, rr_mean := rowMeans(.SD), .SDcols = rr_cols]
lri_draws_final[, rr_lower := apply(.SD, 1, quantile, 0.025), .SDcols = rr_cols]
lri_draws_final[, rr_upper := apply(.SD, 1, quantile, 0.975), .SDcols = rr_cols]
lri_draws_final[, sd := sd(.SD), .SDcols = rr_cols]
lri_draws_final[, parameter := "cat1"]
lri_draws_final <- rbind(lri_draws_final, data.table(parameter = "cat2", rr_mean = 1, rr_lower = 1, rr_upper = 1, sd = 0), fill = TRUE)
lri_draws_final[parameter == "cat2", (rr_cols) := 1]
# males
lri_df_m <- expand.grid(cause_id = 322, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 1, 
                        mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = c("cat1","cat2"))
lri_df_m <- merge(lri_df_m, lri_draws_final, by = "parameter")
setDT(lri_df_m)
# females
lri_df_f <- expand.grid(cause_id = 322, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 2, 
                        mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = c("cat1","cat2"))
lri_df_f <- merge(lri_df_f, lri_draws_final, by = "parameter")
setDT(lri_df_f)

# combine all
hyg_rr_final_m <- rbind(dia_df_m, lri_df_m)
hyg_rr_final_m <- hyg_rr_final_m[order(year_id, age_group_id)]
setcolorder(hyg_rr_final_m, c("cause_id","location_id","age_group_id","sex_id","year_id","mortality","morbidity","parameter",
                              "rr_mean","rr_lower","rr_upper","sd"))

hyg_rr_final_f <- rbind(dia_df_f, lri_df_f)
hyg_rr_final_f <- hyg_rr_final_f[order(year_id, age_group_id)]
setcolorder(hyg_rr_final_f, c("cause_id","location_id","age_group_id","sex_id","year_id","mortality","morbidity","parameter",
                              "rr_mean","rr_lower","rr_upper","sd"))

# save
write.csv(hyg_rr_final_m, "FILEPATH", row.names = F)
write.csv(hyg_rr_final_f, "FILEPATH", row.names = F)

##### calculate evidence score #####
# need to run 'repl_python()' to open an interactive Python interpreter,
# then immediately type 'exit' to get back to the R interpreter
# -- this helps to load a required Python package
repl_python()
# -- type 'exit' or hit escape
evidence_score <- import("mrtool.evidence_score.scorelator")

## LRI
lri_scorelator <- evidence_score$DichotomousScorelator(
  model = hyg_lri_model # MRBRT model
)
scores <- data.frame(
  score = lri_scorelator$get_score(),
  score_gamma = lri_scorelator$get_score(use_gamma_ub = T), #low score
  gamma = lri_scorelator$gamma,
  gammas_sd = sqrt(lri_scorelator$gamma_var)
)
lri_scorelator$plot_model(folder = "FILEPATH", title = "hygiene_lri")

## diarrhea
dia_scorelator <- evidence_score$DichotomousScorelator(
  model = hyg_dia_model # MRBRT model
)
scores <- data.frame(
  score = dia_scorelator$get_score(),
  score_gamma = dia_scorelator$get_score(use_gamma_ub = T), #low score
  gamma = dia_scorelator$gamma,
  gammas_sd = sqrt(dia_scorelator$gamma_var)
)
dia_scorelator$plot_model(folder = "FILEPATH", title = "hygiene_diarrhea")
## RRs & evidence score for no access to handwashing station
#######################################################################

# CONFIG #############################################################
rm(list = ls()[ls() != "xwalk"])

library(mrbrt002, lib.loc = "/ihme/code/mscm/Rv4/packages/")
library(data.table)
library(openxlsx)
library(epiR)

cycle<-"GBD2020"
gbd_round<-7 #GBD2020
decomp<-"iterative"

# Load in Hygiene RR ######################################################
hyg_rr <- data.table(read.xlsx("FILEPATH/extraction_wash_hygiene_SS_MRBRT_prep.xlsx"))[is_outlier == 0]

#Only keep these columns
hyg_rr <- hyg_rr[, .(nid, field_citation_value, source_type, location_name, location_id, ihme_loc_id, acause, year_start, year_end, age_start, age_end,
                     measure, mean, lower, upper, nonCI_uncertainty_value, int_cases, int_sample_size, control_cases, control_sample_size,
                     int_episodes_person_time, int_total_person_time, control_episodes_person_time, control_total_person_time,
                     cohort_cases_person_years, cohort_total_person_years, cv_exposure_population, cv_exposure_selfreport, cv_exposure_study,
                     cv_outcome_selfreport, cv_outcome_unblinded, cv_reverse_causation, cv_confounding_nonrandom, cv_confounding_uncontrolled,
                     cv_selection_bias, cv_subpopulation, is_outlier, inverse_risk)]

# If cv_selection_bias is undefined or NA, then make it one
# What is cv_selection bias?
hyg_rr[cv_selection_bias == "undefined" | is.na(cv_selection_bias), cv_selection_bias := 1]

#Place specific columns into the variable cols
cols <- c("age_start","age_end","mean","lower","upper","nonCI_uncertainty_value","int_cases","int_sample_size",
          "control_cases","control_sample_size","int_episodes_person_time","int_total_person_time",
          "control_episodes_person_time","control_total_person_time","is_outlier")


# Place the columns that start with cv_ into a variable called cv_cols
cv_cols <- names(hyg_rr)[names(hyg_rr) %like% "cv_"]

#Change all of the cols and cv_cols into numeric
hyg_rr[, c(cols,cv_cols) := lapply(.SD, as.numeric), .SDcols = c(cols,cv_cols)]

# Custom Functions ##################################################################
### some RRs/CIs were not reported - these are functions to calculate them

# calculate RR
#input: dt - extraction sheet
#input: type - either "cumulative incidence" (sample size & cases reported) or "incidence rate" (person-time and episodes per person-time reported)
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

  #Proportion of cases in the test group divided by the proportion of cases in the control group
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

# LRI #################################################
# Lower respiratory infection

hyg_lri <- hyg_rr[cause == "lri"]

## Calculate RR and uncertainty ##################################
# calculate RR and uncertainty for studies w/o RR or uncertainty that report person-time and episodes per person-time
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

## calc se ##########################################################
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

## Subset ##########################################################
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

## save ############################################
write.csv(hyg_lri, paste0("FILEPATH/",cycle,"/hyg_lri_rr.csv"), row.names = F)


## run model w/o covariates ####################################################
# Load data into MR-BRT format
hyg_lri_data_basic <- MRData()

hyg_lri_data_basic$load_df(
  data = hyg_lri, col_obs = "log_rr", col_obs_se = "log_se",
  col_covs = list(), col_study_id = "nid"
)

hyg_lri_model_basic <- MRBRT(
  data = hyg_lri_data_basic,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE)
  )
)

hyg_lri_model_basic$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

## predict ##########################################################
hyg_lri_pred_df_basic <- data.frame(intercept = 1)
# Dataframe that only has a column called intercept, filled in with a 1

# Load data into MR-BRT format
hyg_lri_pred_data_basic <- MRData()

# Load data into MR-BRT model?
hyg_lri_pred_data_basic$load_df(
  data = hyg_lri_pred_df_basic,
  col_covs = list("intercept")
)

# Using MR-BRT predict
hyg_lri_pred_basic <- hyg_lri_model_basic$predict(data = hyg_lri_pred_data_basic)

hyg_lri_samples_basic <- hyg_lri_model_basic$sample_soln(sample_size = 1000L)
hyg_lri_draws_basic <- hyg_lri_model_basic$create_draws(
  data = hyg_lri_pred_data_basic,
  beta_samples = hyg_lri_samples_basic[[1]],
  gamma_samples = hyg_lri_samples_basic[[2]],
  random_study = TRUE
)

hyg_lri_pred_df_basic$pred1 <- hyg_lri_pred_basic %>% exp
hyg_lri_pred_df_basic$pred1_lo <- apply(hyg_lri_draws_basic, 1, function(x) quantile(x, 0.025)) %>% exp
hyg_lri_pred_df_basic$pred1_hi <- apply(hyg_lri_draws_basic, 1, function(x) quantile(x, 0.975)) %>% exp

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
hyg_lri_covs_final <- hyg_lri_covfinder$selected_covs # intercept, cv_outcome_unblinded, cv_confounding_nonrandom, cv_selection_bias_2

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
    # LinearCovModel("cv_outcome_unblinded"),
    LinearCovModel("cv_confounding_nonrandom"),
    LinearCovModel("cv_selection_bias_2")),
  inlier_pct = 0.9
)

# save model object
py_save_object(object = hyg_lri_model, filename = paste0("FILEPATH/",cycle,"/FILEPATH/hyg_lri_model.pkl"), pickle = "dill")


# load model object
hyg_lri_model <- py_load_object(filename = paste0("FILEPATH/",cycle,"/FILEPATH/hyg_lri_model.pkl"), pickle = "dill")

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

# Diarrhea #########################################################################################

hyg_dia <- hyg_rr[acause == "Diarrheal diseases"]

## Calculate RR ##############################################

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

## calc se ################################
hyg_dia[, standard_error := log_to_linear(array(log(mean)), array((log(upper)-log(lower))/(2*qnorm(0.975))))[[2]]]

# transform to log space
hyg_dia[, log_rr := linear_to_log(array(mean), array(standard_error))[[1]]]
hyg_dia[, log_se := linear_to_log(array(mean), array(standard_error))[[2]]]

## Covariates ############################
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

## save #####################################
write.csv(hyg_dia, paste0("FILEPATH/",cycle,"/hyg_dia_rr.csv"))

## MRBRT w/o cov #############################
# run model without covariates

#Prep a variable to be in MRBRT format
hyg_dia_data_basic <- MRData()

#Fill in that variable with your data
hyg_dia_data_basic$load_df(
  data = hyg_dia, 
  col_obs = "log_rr", 
  col_obs_se = "log_se",
  col_covs = list(), 
  col_study_id = "nid"
)

#Prep a model with your customization
hyg_dia_model_basic <- MRBRT(
  data = hyg_dia_data_basic,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE)),
  inlier_pct = 0.9 # sets the portion of the data you want to keep, 0.9 is the original value
)

# Run the model (this will take a number of iterations for the model to fit)
hyg_dia_model_basic$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

### Predict ################
#Make a dataframe with a column called intercept, filled in with 1
hyg_dia_pred_df_basic <- data.frame(intercept = 1)

#Prep a variable to be in MRBRT format
hyg_dia_pred_data_basic <- MRData()

#load your data into that variable
hyg_dia_pred_data_basic$load_df(
  data = hyg_dia_pred_df_basic,
  col_covs = list("intercept")
)

# Predict
hyg_dia_pred_basic <- hyg_dia_model_basic$predict(data = hyg_dia_pred_data_basic)

#Take 1000 samples of your prediction 
hyg_dia_samples_basic <- hyg_dia_model_basic$sample_soln(sample_size = 1000L)

#Using your samples, create draws
hyg_dia_draws_basic <- hyg_dia_model_basic$create_draws(
  data = hyg_dia_pred_data_basic,
  beta_samples = hyg_dia_samples_basic[[1]],
  gamma_samples = hyg_dia_samples_basic[[2]],
  random_study = TRUE
)

### Extract the RR, and CI ##############
hyg_dia_pred_df_basic$pred1 <- hyg_dia_pred_basic %>% exp
hyg_dia_pred_df_basic$pred1_lo <- apply(hyg_dia_draws_basic, 1, function(x) quantile(x, 0.025)) %>% exp
hyg_dia_pred_df_basic$pred1_hi <- apply(hyg_dia_draws_basic, 1, function(x) quantile(x, 0.975)) %>% exp

## CovFinder ##########################
# select covariates with CovFinder

#Grab all of the covariates that had variation
covs_dia <- names(hyg_dia)[names(hyg_dia) %like% "cv_"]

#Prep a variable to be in MRBRT format
hyg_dia_covs <- MRData()

#Load your data into this variable, include all of the covariates
hyg_dia_covs$load_df(
  data = hyg_dia, col_obs = "log_rr", col_obs_se = "log_se",
  col_covs = as.list(covs_dia), col_study_id = "nid"
)

# Use CovFinder to creat a model
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

# Run the model
hyg_dia_covfinder$select_covs(verbose = FALSE)

#Find out what covariates the model suggests using
hyg_dia_covs_final <- hyg_dia_covfinder$selected_covs 

## MRBRT w/ covariates ############################################
# run model with the covariates

#Prep a variable to be in MRBRT format
hyg_dia_data <- MRData()


hyg_dia_data$load_df(
  data = hyg_dia, col_obs = "log_rr", col_obs_se = "log_se",
  col_covs = list("cv_exposure_study", "cv_confounding_uncontrolled_1", "cv_selection_bias_2"),
  col_study_id = "nid"
)


hyg_dia_model <- MRBRT(
  data = hyg_dia_data,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE),
    LinearCovModel("cv_exposure_study"),
    LinearCovModel("cv_confounding_uncontrolled_1"),
    LinearCovModel("cv_selection_bias_2")),
  inlier_pct = 0.9
)

# save model object
py_save_object(object = hyg_dia_model, filename = paste0("FILEPATH/",cycle,"/FILEPATH/hyg_dia_model.pkl"), pickle = "dill")

# load model object
hyg_dia_model <- py_load_object(filename = paste0("FILEPATH/",cycle,"/FILEPATH/hyg_dia_model.pkl"), pickle = "dill")


# Fit the model
hyg_dia_model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

hyg_dia_pred_df <- expand.grid(intercept = 1, cv_exposure_study = c(0,1),
                               cv_confounding_uncontrolled_1 = c(0,1), cv_selection_bias_2 = c(0,1))

# Prep a variable in the MRBRT format
hyg_dia_pred_data <- MRData()

hyg_dia_pred_data$load_df(
  data = hyg_dia_pred_df,
  col_covs = list("intercept","cv_exposure_study",
                  "cv_confounding_uncontrolled_1","cv_selection_bias_2")
)

### Predict ###################
#Use the previous MRBRT model w/ covariates to predict based on the expanded grid covariates
hyg_dia_pred <- hyg_dia_model$predict(data = hyg_dia_pred_data)

#Grab our 1000 samples of the prediction
hyg_dia_samples <- hyg_dia_model$sample_soln(sample_size = 1000L)

#Now use the 1000 samples to get draws
hyg_dia_draws <- hyg_dia_model$create_draws(
  data = hyg_dia_pred_data,
  beta_samples = hyg_dia_samples[[1]],
  gamma_samples = hyg_dia_samples[[2]],
  random_study = TRUE
)

### Extract the RR and CI #####################
#Use the draws and prediction to get the exp of the RR and CI. Later on we will be doing 1/exp(x) in the dia_draws_final table
hyg_dia_pred_df$pred1 <- hyg_dia_pred %>% exp
hyg_dia_pred_df$pred1_lo <- apply(hyg_dia_draws, 1, function(x) quantile(x, 0.025)) %>% exp
hyg_dia_pred_df$pred1_hi <- apply(hyg_dia_draws, 1, function(x) quantile(x, 0.975)) %>% exp


#FORMAT FOR SAVE RESULTS #####
rr_cols <- paste0("rr_", 0:999)

## diarrhea #######
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

## lri ############
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
write.csv(hyg_rr_final_m, paste0("/FILEPATH/",cycle,"/rr_1_1.csv", row.names = F))
write.csv(hyg_rr_final_f, paste0("/FILEPATH/",cycle,"/rr_1_2.csv", row.names = F))

# save results (run in qlogin with 24 threads & 60G mem)
source("FILEPATH/save_results_risk.R")
save_results_risk(input_dir = "FILEPATH", input_file_pattern = "rr_{location_id}_{sex_id}.csv",
                  modelable_entity_id = 9081, description = "added 15 new LRI studies & used new MR-BRT tool", risk_type = "rr",
                  year_id = c(seq(1990,2015,5),2019,2020:2022), gbd_round_id = gbd_round, decomp_step = decomp, mark_best = TRUE)


# calculate evidence score #######################
# need to run 'repl_python()' to open an interactive Python interpreter,
# then immediately type 'exit' to get back to the R interpreter
# -- this helps to load a required Python package
repl_python()
# -- type 'exit' or hit escape
evidence_score <- import("mrtool.evidence_score.scorelator")

## lri ##########
lri_scorelator <- evidence_score$DichotomousScorelator(
  model = hyg_lri_model # MRBRT model
)

scores <- data.frame(
  score = lri_scorelator$get_score(),
  score_gamma = lri_scorelator$get_score(use_gamma_ub = T), #low score
  gamma = lri_scorelator$gamma,
  gammas_sd = sqrt(lri_scorelator$gamma_var)
)

lri_scorelator$plot_model(folder = paste0("FILEPATH/",cycle,"/FILEPATH"), title = "hygiene_lri")

## diarrhea ################

dia_scorelator <- evidence_score$DichotomousScorelator(
  model = hyg_dia_model # MRBRT model
)

scores <- data.frame(
  score = dia_scorelator$get_score(),
  score_gamma = dia_scorelator$get_score(use_gamma_ub = T), #low score
  gamma = dia_scorelator$gamma,
  gammas_sd = sqrt(dia_scorelator$gamma_var)
)

dia_scorelator$plot_model(folder = paste0("FILEPATH/",cycle,"/FILEPATH"), title = "hygiene_diarrhea") 

#--------------------------
## update RRs with Fischer information boost


source("FILEPATH/dichotomous_functions.R")
rr_cols <- paste0("rr_", 0:999)

## diarrhea
hyg_dia_model <- py_load_object(filename = "FILEPATH/hyg_dia_model.pkl", pickle = "dill")
set.seed(22321)
dia_fischer_draws <- get_draws(hyg_dia_model) %>% data.table # generate draws with the Fischer information boost

dia_fischer_draws[, `:=` (draw = 1/exp(draw), cols = rr_cols, pivot = 1)] # transform to normal space
dia_fischer_draws[, cols := factor(cols, levels = rr_cols)] # factor the column names so when dcasting the cols are in order from 0-999
dia_fischer_draws <- dcast(dia_fischer_draws, pivot ~ cols, value.var = "draw")[, pivot := NULL] # reshape wide

dia_fischer_draws[, rr_mean := rowMeans(.SD), .SDcols = rr_cols]
dia_fischer_draws[, rr_lower := apply(.SD, 1, quantile, 0.025), .SDcols = rr_cols]
dia_fischer_draws[, rr_upper := apply(.SD, 1, quantile, 0.975), .SDcols = rr_cols]
dia_fischer_draws[, sd := sd(.SD), .SDcols = rr_cols]
dia_fischer_draws[, parameter := "cat1"]
dia_fischer_draws <- rbind(dia_fischer_draws, data.table(parameter = "cat2", rr_mean = 1, rr_lower = 1, rr_upper = 1, sd = 0), fill = TRUE)
dia_fischer_draws[parameter == "cat2", (rr_cols) := 1]

# males
dia_fischer_m <- expand.grid(cause_id = 302, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 1,
                        mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = c("cat1","cat2"))
dia_fischer_m <- merge(dia_fischer_m, dia_fischer_draws, by = "parameter")
setDT(dia_fischer_m)
# females
dia_fischer_f <- expand.grid(cause_id = 302, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 2,
                        mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = c("cat1","cat2"))
dia_fischer_f <- merge(dia_fischer_f, dia_fischer_draws, by = "parameter")
setDT(dia_fischer_f)

## lri
hyg_lri_model <- py_load_object(filename = "FILEPATH/hyg_lri_model.pkl", pickle = "dill")
set.seed(22321)
lri_fischer_draws <- get_draws(hyg_lri_model) %>% data.table # generate draws with the Fischer information boost

lri_fischer_draws[, `:=` (draw = 1/exp(draw), cols = rr_cols, pivot = 1)] # transform to normal space
lri_fischer_draws[, cols := factor(cols, levels = rr_cols)] # factor the column names so when dcasting the cols are in order from 0-999
lri_fischer_draws <- dcast(lri_fischer_draws, pivot ~ cols, value.var = "draw")[, pivot := NULL] # reshape wide

lri_fischer_draws[, rr_mean := rowMeans(.SD), .SDcols = rr_cols]
lri_fischer_draws[, rr_lower := apply(.SD, 1, quantile, 0.025), .SDcols = rr_cols]
lri_fischer_draws[, rr_upper := apply(.SD, 1, quantile, 0.975), .SDcols = rr_cols]
lri_fischer_draws[, sd := sd(.SD), .SDcols = rr_cols]
lri_fischer_draws[, parameter := "cat1"]
lri_fischer_draws <- rbind(lri_fischer_draws, data.table(parameter = "cat2", rr_mean = 1, rr_lower = 1, rr_upper = 1, sd = 0), fill = TRUE)
lri_fischer_draws[parameter == "cat2", (rr_cols) := 1]

# males
lri_fischer_m <- expand.grid(cause_id = 322, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 1,
                        mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = c("cat1","cat2"))
lri_fischer_m <- merge(lri_fischer_m, lri_fischer_draws, by = "parameter")
setDT(lri_fischer_m)
# females
lri_fischer_f <- expand.grid(cause_id = 322, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 2,
                        mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = c("cat1","cat2"))
lri_fischer_f <- merge(lri_fischer_f, lri_fischer_draws, by = "parameter")
setDT(lri_fischer_f)

# combine all
hyg_fischer_final_m <- rbind(dia_fischer_m, lri_fischer_m)
hyg_fischer_final_m <- hyg_fischer_final_m[order(year_id, age_group_id)]
setcolorder(hyg_fischer_final_m, c("cause_id","location_id","age_group_id","sex_id","year_id","mortality","morbidity","parameter",
                              "rr_mean","rr_lower","rr_upper","sd"))

hyg_fischer_final_f <- rbind(dia_fischer_f, lri_fischer_f)
hyg_fischer_final_f <- hyg_fischer_final_f[order(year_id, age_group_id)]
setcolorder(hyg_fischer_final_f, c("cause_id","location_id","age_group_id","sex_id","year_id","mortality","morbidity","parameter",
                              "rr_mean","rr_lower","rr_upper","sd"))

# save
write.csv(hyg_fischer_final_m, "FILEPATH/rr_1_1.csv", row.names = F)
write.csv(hyg_fischer_final_f, "FILEPATH/rr_1_2.csv", row.names = F)

# save results (run in qlogin with 24 threads & 60G mem)
source("FILEPATH/save_results_risk.R")
save_results_risk(input_dir = "FILEPATH", input_file_pattern = "rr_{location_id}_{sex_id}.csv",
                  modelable_entity_id = 9081, description = "RRs updated with Fischer information boost", risk_type = "rr",
                  year_id = c(seq(1990,2015,5),2019,2020:2022), gbd_round_id = 7, decomp_step = "iterative", mark_best = TRUE)

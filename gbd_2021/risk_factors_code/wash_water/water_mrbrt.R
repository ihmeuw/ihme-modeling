library(crosswalk, lib.loc = "FILEPATH")

# load rr dataset
water_rr <- fread("FILEPATH")[exclude == 0, .(nid, reference, yearstudywasconducted, intervention_clean, intervention_true, 
                                                                                     control_clean, effectsize, lower95confidenceinterval, upper95confidenceinterval)]
setnames(water_rr, 
         c("yearstudywasconducted","intervention_clean","control_clean","effectsize","lower95confidenceinterval","upper95confidenceinterval"),
         c("year","intervention","control","rr","lower","upper"))

# calc se
water_rr[, se := delta_transform(log(rr), (log(upper)-log(lower))/(2*qnorm(0.975)), transformation = "log_to_linear")[, 2]] # crosswalk package
water_rr[lower == 0, se := (upper - rr)/qnorm(0.975)]

# transform to log space
water_rr[, log_rr := delta_transform(rr, se, "linear_to_log")[, 1]] # crosswalk package
water_rr[, log_se := delta_transform(rr, se, "linear_to_log")[, 2]] # crosswalk package

# load covariates
water_rr_covs <- fread("FILEPATH")
cv_cols <- names(water_rr_covs)[names(water_rr_covs) %like% "cv"]
water_rr_covs <- water_rr_covs[, c("nid",cv_cols), with = F] %>% unique

# merge onto main dataset
water_rr <- merge(water_rr, water_rr_covs, by = "nid")

# change categorical vars to binary
water_rr[, cv_confounding_uncontrolled_1 := ifelse(cv_confounding_uncontrolled == 1, 1, 0)]
water_rr[, cv_confounding_uncontrolled_2 := ifelse(cv_confounding_uncontrolled == 2, 1, 0)]
water_rr[, cv_selection_bias_1 := ifelse(cv_selection_bias == 1, 1, 0)]
water_rr[, cv_selection_bias_2 := ifelse(cv_selection_bias == 2, 1, 0)]
water_rr[, c("cv_confounding_uncontrolled","cv_selection_bias") := NULL]

##################

# run model
water_data <- CWData(
  df = water_rr,
  obs = "log_rr",
  obs_se = "log_se",
  alt_dorms = "intervention",
  ref_dorms = "control",
  covs = list("cv_exposure_selfreport"),
  study_id = "nid"
)

water_model <- CWModel(
  cwdata = water_data,
  obs_type = "diff_log",
  cov_models = list(
    CovModel(cov_name = "intercept"),
    CovModel(cov_name = "cv_exposure_selfreport")
  ),
  gold_dorm = "unimproved",
  inlier_pct = 0.9,
  max_iter = 1000L
)

rr_table <- data.table(intervention = water_model$vars,
                       log_rr = c(water_model$fixed_vars[[1]][1],
                                  water_model$fixed_vars[[2]][1],
                                  water_model$fixed_vars[[3]][1],
                                  water_model$fixed_vars[[4]][1],
                                  water_model$fixed_vars[[5]][1],
                                  water_model$fixed_vars[[6]][1]),
                       log_se = c(sqrt(water_model$beta_sd[1]^2+water_model$gamma),
                                  sqrt(water_model$beta_sd[3]^2+water_model$gamma),
                                  sqrt(water_model$beta_sd[5]^2+water_model$gamma),
                                  sqrt(water_model$beta_sd[7]^2+water_model$gamma),
                                  sqrt(water_model$beta_sd[9]^2+water_model$gamma),
                                  sqrt(water_model$beta_sd[11]^2+water_model$gamma)))
rr_table[, log_rr_lower := log_rr - (log_se * qnorm(0.975))]
rr_table[, log_rr_upper := log_rr + (log_se * qnorm(0.975))]
rr_table[, rr := delta_transform(rr_table$log_rr, rr_table$log_se, "log_to_linear")[, 1]]
rr_table[, rr_lower := exp(log_rr_lower)]
rr_table[, rr_upper := exp(log_rr_upper)]

##### generate draws #####
set.seed(825865)
rr_cols <- paste0("rr_",0:999)

gen_rr_draws <- function(type) {
  log_rr <- rr_table[intervention == type, log_rr]
  log_se <- rr_table[intervention == type, log_se]

  rr_draws <- data.table(draws = exp(rnorm(1000, mean = log_rr, sd = log_se)))
  setnames(rr_draws, "draws", type)

  return(rr_draws)
}

rr_draws_temp <- lapply(rr_table$intervention[-6], gen_rr_draws)
rr_draws_all <- cbind(rr_draws_temp[[1]],
                      rr_draws_temp[[2]],
                      rr_draws_temp[[3]],
                      rr_draws_temp[[4]],
                      rr_draws_temp[[5]])

# rescale RRs to match with exposure categories
# reference is hq piped + boil/filter
rr_draws_all[, cat1 := 1/(hq_piped*filter)]
rr_draws_all[, cat2 := solar/(hq_piped*filter)]
rr_draws_all[, cat3 := filter/(hq_piped*filter)]
rr_draws_all[, cat4 := improved/(hq_piped*filter)]
rr_draws_all[, cat5 := (improved*solar)/(hq_piped*filter)]
rr_draws_all[, cat6 := (improved*filter)/(hq_piped*filter)]
rr_draws_all[, cat7 := piped/(hq_piped*filter)]
rr_draws_all[, cat8 := (piped*solar)/(hq_piped*filter)]
rr_draws_all[, cat9 := (piped*filter)/(hq_piped*filter)]
rr_draws_all[, cat10 := hq_piped/(hq_piped*filter)]
rr_draws_all[, cat11 := (hq_piped*solar)/(hq_piped*filter)]
rr_draws_all[, cat12 := (hq_piped*filter)/(hq_piped*filter)]

rr_dcast <- function(col_name) {
  df <- data.table(cols = rr_cols, draws = rr_draws_all[, get(col_name)])
  df <- dcast(df, . ~ cols, value.var = "draws")[, -"."]
  df[, parameter := paste0(col_name)]
  df[, rr_mean := rowMeans(.SD), .SDcols = rr_cols]
  df[, rr_lower := apply(.SD, 1, quantile, 0.025), .SDcols = rr_cols]
  df[, rr_upper := apply(.SD, 1, quantile, 0.975), .SDcols = rr_cols]
  setcolorder(df, c("parameter","rr_mean","rr_lower","rr_upper",rr_cols))

  return(df)
}

water_draws_final <- rbindlist(lapply(paste0("cat",1:12), rr_dcast))

##### FORMAT FOR SAVING #####

# males
water_df_m <- expand.grid(cause_id = 302, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 1, 
                          mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = paste0("cat",1:12))
water_df_m <- merge(water_df_m, water_draws_final, by = "parameter")
setDT(water_df_m)
water_df_m <- water_df_m[order(year_id, age_group_id, parameter)]
setcolorder(water_df_m, c("cause_id","location_id","age_group_id","sex_id","year_id","mortality","morbidity","parameter",
                          "rr_mean","rr_lower","rr_upper",rr_cols))
# females
water_df_f <- expand.grid(cause_id = 302, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 2, 
                          mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = paste0("cat",1:12))
water_df_f <- merge(water_df_f, water_draws_final, by = "parameter")
setDT(water_df_f)
water_df_f <- water_df_f[order(year_id, age_group_id, parameter)]
setcolorder(water_df_f, c("cause_id","location_id","age_group_id","sex_id","year_id","mortality","morbidity","parameter",
                          "rr_mean","rr_lower","rr_upper",rr_cols))

# save
write.csv(water_df_m, "FILEPATH", row.names = F)
write.csv(water_df_f, "FILEPATH", row.names = F)

##### CALCULATE EVIDENCE SCORE #####
# need to run 'repl_python()' to open an interactive Python interpreter,
# then immediately type 'exit' to get back to the R interpreter
# -- this helps to load a required Python package
repl_python()
# -- type 'exit' or hit escape
evidence_score <- import("crosswalk.scorelator")
# run scorelator
scorelator <- evidence_score$Scorelator(water_model, type = "protective", name = "water")
# save plot
scorelator$plot_model(folder = "FILEPATH")
# save scores
scores <- data.table(
  categories = water_model$vars[1:5],
  score = scorelator$get_score() %>% as.numeric,
  low_score = scorelator$get_score(use_gamma_ub = TRUE) %>% as.numeric
)
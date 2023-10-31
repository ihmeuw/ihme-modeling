## evidence score for unsafe sanitation
## 6/16/20
##################################

# load packages
library(crosswalk, lib.loc = "FILEPATH")

# load rr dataset
san_rr <- fread("FILEPATH")[, .(nid, reference, yearstudywasconducted, intervention_clean, control_edit, effectsize, 
                                                                     lower95confidenceinterval,	upper95confidenceinterval, exclude)]
san_rr <- san_rr[exclude == 0][, exclude := NULL]
setnames(san_rr, 
         c("yearstudywasconducted","intervention_clean","control_edit","effectsize","lower95confidenceinterval","upper95confidenceinterval"),
         c("year","intervention","control","rr","lower","upper"))
san_rr[, nid := as.numeric(nid)]

# calc se
san_rr[, se := delta_transform(log(rr), (log(upper)-log(lower))/(2*qnorm(0.975)), transformation = "log_to_linear")[, 2]]

# transform to log space
san_rr[, log_rr := delta_transform(rr, se, "linear_to_log")[, 1]] # log rr
san_rr[, log_se := delta_transform(rr, se, "linear_to_log")[, 2]] # log se

# load covariates
san_rr_covs <- data.table(read.xlsx("FILEPATH"))[-1, -c("study","notes")]
san_rr_covs[, names(san_rr_covs) := lapply(.SD, as.numeric), .SDcols = names(san_rr_covs)]
# subset to only covs that have variation (i.e. >1 unique value)
cv_cols <- names(san_rr_covs)[names(san_rr_covs) %like% "cv"]
cols_delete <- c()
for (col in cv_cols) {
  if (length(san_rr_covs[, unique(get(col))]) == 1) {
    print(col)
    cols_delete <- append(cols_delete, col)
  }
}
san_rr_covs <- san_rr_covs[, -cols_delete, with = F]

# merge onto main dataset
san_rr <- merge(san_rr, san_rr_covs, by = "nid")
# change categorical vars to binary
san_rr[, cv_confounding_uncontrolled_1 := ifelse(cv_confounding_uncontrolled == 1, 1, 0)]
san_rr[, cv_confounding_uncontrolled_2 := ifelse(cv_confounding_uncontrolled == 2, 1, 0)]
san_rr[, cv_confounding_uncontrolled := NULL]

# run model
san_data <- CWData(
  df = san_rr,
  obs = "log_rr",
  obs_se = "log_se",
  alt_dorms = "intervention",
  ref_dorms = "control",
  covs = list("cv_subpopulation"),
  study_id = "nid"
)

san_model <- CWModel(
  cwdata = san_data,
  obs_type = "diff_log",
  cov_models = list(
    CovModel(cov_name = "intercept"),
    CovModel(cov_name = "cv_subpopulation")
  ),
  gold_dorm = "unimproved",
  inlier_pct = 0.9,
  max_iter = 500L, use_random_intercept = TRUE
)

san_rr_final <- data.table(intervention = c(rep(san_model$vars[1],2),
                                            rep(san_model$vars[2],2),
                                            rep(san_model$vars[3],2)), 
                           log_rr = san_model$beta %>% as.numeric, 
                           log_se = sqrt(as.numeric(san_model$beta_sd)^2+as.numeric(san_model$gamma)))
san_rr_final[, log_rr_lower := log_rr - (log_se * qnorm(0.975))]
san_rr_final[, log_rr_upper := log_rr + (log_se * qnorm(0.975))]
san_rr_final[, rr := exp(log_rr)]
san_rr_final[, rr_lower := exp(log_rr_lower)]
san_rr_final[, rr_upper := exp(log_rr_upper)]
san_rr_final <- san_rr_final[c(1,3,5)]

##### FORMAT FOR SAVING #####
rr_cols <- paste0("rr_",0:999)

san_imp_rr <- san_rr_final[intervention == "improved", rr]/san_rr_final[intervention == "sewer", rr]
san_imp_rr_lower <- san_rr_final[intervention == "improved", rr_upper]/san_rr_final[intervention == "sewer", rr_upper]
san_imp_rr_upper <- san_rr_final[intervention == "improved", rr_lower]/san_rr_final[intervention == "sewer", rr_lower]
san_imp_rr_sd <- (log(san_imp_rr_upper) - log(san_imp_rr_lower))/(2*qnorm(0.975))
san_imp_draws <- data.table(parameter = "cat2", cols = rr_cols, draws = exp(rnorm(1000, mean = log(san_imp_rr), sd = san_imp_rr_sd)))
san_imp_draws <- dcast(san_imp_draws, parameter ~ cols, value.var = "draws")
san_imp_draws[, `:=` (rr_mean = san_imp_rr, rr_lower = san_imp_rr_lower, rr_upper = san_imp_rr_upper, sd = san_imp_rr_sd)]

san_unimp_rr <- 1/san_rr_final[intervention == "sewer", rr]
san_unimp_rr_lower <- 1/san_rr_final[intervention == "sewer", rr_upper]
san_unimp_rr_upper <- 1/san_rr_final[intervention == "sewer", rr_lower]
san_unimp_rr_sd <- (log(san_unimp_rr_upper) - log(san_unimp_rr_lower))/(2*qnorm(0.975))
san_unimp_draws <- data.table(parameter = "cat1", cols = rr_cols, draws = exp(rnorm(1000, mean = log(san_unimp_rr), sd = san_unimp_rr_sd)))
san_unimp_draws <- dcast(san_unimp_draws, parameter ~ cols, value.var = "draws")
san_unimp_draws[, `:=` (rr_mean = san_unimp_rr, rr_lower = san_unimp_rr_lower, rr_upper = san_unimp_rr_upper, sd = san_unimp_rr_sd)]

san_draws_final <- rbindlist(list(san_unimp_draws, san_imp_draws,
                                  data.table(parameter = "cat3", rr_mean = 1, rr_lower = 1, rr_upper = 1, sd = 0)), fill = TRUE)
san_draws_final[parameter == "cat3", (rr_cols) := 1]

# males
san_df_m <- expand.grid(cause_id = 302, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 1, 
                        mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = c("cat1","cat2","cat3"))
san_df_m <- merge(san_df_m, san_draws_final, by = "parameter")
setDT(san_df_m)
san_df_m <- san_df_m[order(year_id, age_group_id)]
setcolorder(san_df_m, c("cause_id","location_id","age_group_id","sex_id","year_id","mortality","morbidity","parameter",
                        "rr_mean","rr_lower","rr_upper","sd",rr_cols))
# females
san_df_f <- expand.grid(cause_id = 302, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 2, 
                        mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = c("cat1","cat2","cat3"))
san_df_f <- merge(san_df_f, san_draws_final, by = "parameter")
setDT(san_df_f)
san_df_f <- san_df_f[order(year_id, age_group_id)]
setcolorder(san_df_f, c("cause_id","location_id","age_group_id","sex_id","year_id","mortality","morbidity","parameter",
                        "rr_mean","rr_lower","rr_upper","sd",rr_cols))

# save
write.csv(san_df_m, "FILEPATH", row.names = F)
write.csv(san_df_f, "FILEPATH", row.names = F)

##### CALC EVIDENCE SCORE #####
# need to run 'repl_python()' to open an interactive Python interpreter,
# then immediately type 'exit' to get back to the R interpreter
# -- this helps to load a required Python package
repl_python()
# -- type 'exit' or hit escape
evidence_score <- import("crosswalk.scorelator")
# run scorelator
scorelator <- evidence_score$Scorelator(san_model, type = "protective", name = "sanitation")
# save plot
scorelator$plot_model(folder = "FILEPATH")
# save scores
scores <- data.table(
  categories = san_model$vars[1:5],
  score = scorelator$get_score() %>% as.numeric,
  low_score = scorelator$get_score(use_gamma_ub = TRUE) %>% as.numeric
)
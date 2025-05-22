################################################################
# Run MR-BRT for unsafe water relative risk
##################################################################

# CONFIG ############################################################
rm(list = ls()[ls() != "xwalk"])

library(reticulate)
reticulate::use_python("FILEPATH")
cw <- import("crosswalk")

library(data.table)
library(magrittr)

# Load rr dataset ##############################################################

water_rr <- fread("FILEPATH/water_rr.csv")[meta_analysis == 0, 
                                                                                                                  .(nid, reference, yearstudywasconducted,
                                                                                                                    intervention_clean, intervention_true,
                                                                                     control_clean, effectsize, lower95confidenceinterval, upper95confidenceinterval)]

setnames(water_rr,
         c("yearstudywasconducted","intervention_clean","control_clean","effectsize","lower95confidenceinterval","upper95confidenceinterval"),
         c("year","intervention","control","rr","lower","upper"))

# calc se #####################################################################
water_rr[,se:=cw$utils$log_to_linear(mean = array(log(rr)),sd=array((log(upper)-log(lower))/(2*qnorm(0.975))))[[2]]]
water_rr[lower == 0, se := (upper - rr)/qnorm(0.975)]

# transform to log space
water_rr[,log_rr:=cw$utils$linear_to_log(mean = array(rr), sd=array(se))[[1]]]
water_rr[,log_se:=cw$utils$linear_to_log(mean = array(rr), sd=array(se))[[2]]]


# load covariates ##############################################################
water_rr_covs <- fread("FILEPATH/water_covs.csv")
cv_cols <- names(water_rr_covs)[names(water_rr_covs) %like% "cv"]
water_rr_covs <- water_rr_covs[, c("nid",cv_cols), with = F] %>% unique

# Merge covariates ##########################################################
# merge onto main dataset
water_rr <- merge(water_rr, water_rr_covs, by = "nid")

# change categorical vars to binary
water_rr[, cv_confounding_uncontrolled_1 := ifelse(cv_confounding_uncontrolled == 1, 1, 0)]
water_rr[, cv_confounding_uncontrolled_2 := ifelse(cv_confounding_uncontrolled == 2, 1, 0)]
water_rr[, cv_selection_bias_1 := ifelse(cv_selection_bias == 1, 1, 0)]
water_rr[, cv_selection_bias_2 := ifelse(cv_selection_bias == 2, 1, 0)]
water_rr[, c("cv_confounding_uncontrolled","cv_selection_bias") := NULL]

##################
# loop over covs to see which ones are significant by themselves #####################################
covs <- names(water_rr)[names(water_rr) %like% "cv_"]

for (cv in covs[-c(1:8)]) { 
  message(cv)

  cv_data <- paste0(cv, "_data")
  assign(cv_data, CWData(
    df = water_rr,
    obs = "log_rr",
    obs_se = "log_se",
    alt_dorms = "intervention",
    ref_dorms = "control",
    covs = list(cv),
    study_id = "nid"
  ))

  cv_model <- paste0(cv, "_model")
  assign(cv_model, CWModel(
    cwdata = get(cv_data),
    obs_type = "diff_log",
    cov_models = list(
      CovModel(cov_name = "intercept"),
      CovModel(cov_name = cv)
    ),
    gold_dorm = "unimproved",
    inlier_pct = 0.9,
    max_iter = 1000L
  ))
}


selfreport_data<-CWData(
  df = water_rr,
  obs = "log_rr",
  obs_se = "log_se",
  alt_dorms = "intervention",
  ref_dorms = "control",
  covs = list("cv_exposure_selfreport"),
  study_id = "nid")

selfreport_Model<-CWModel(
  cwdata = selfreport_data,
  obs_type = "diff_log",
  cov_models = list(
    CovModel(cov_name = "intercept"),
    CovModel(cov_name = "cv_exposure_selfreport")
  ),
  gold_dorm = "unimproved",
  inlier_pct = 0.9,
  max_iter = 1000L)



water_data <- cw$CWData(
  df = water_rr,
  obs = "log_rr",
  obs_se = "log_se",
  alt_dorms = "intervention",
  ref_dorms = "control",
  covs = list(),
  study_id = "nid"
)

water_model <- cw$CWModel(
  cwdata = water_data,
  obs_type = "diff_log",
  cov_models = list(
    cw$CovModel(cov_name = "intercept")
  ),
  gold_dorm = "unimproved"
)


# save model object ################################################################
py_save_object(object = water_model, filename = "FILEPATH/water_model.pkl", pickle = "dill")


#plots #############################################
water_fit<-water_model$fit(inlier_pct = 0.9)

#funnel plot
##### don't forget to run repl_python() !
# ... then type 'exit' to get back to the R interpreter
repl_python()

plots<-import("crosswalk.plots")

water_rr[,':='(refvar="unimproved",altvar=intervention)]

plots$funnel_plot(
  cwmodel = water_model,
  cwdata = water_rr,
  continuous_variables = list(),
  obs_method = "filter",
  plot_note = "Water Treatment: Filtered",
  plots_dir = "FILEPATH",
  file_name = "funnel_plot_filter",
  write_file = T
)

# load model object ################################################################
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
rr_table[,rr:=cw$utils$log_to_linear(mean = array(log_rr),sd=array(log_se))[[1]]]
rr_table[, rr_lower := exp(log_rr_lower)]
rr_table[, rr_upper := exp(log_rr_upper)]

# Clean and save rr table ###############################################################
rr_table_clean <- rr_table[, .(intervention, rr, rr_lower, rr_upper)]
rr_table_clean[intervention == "unimproved", `:=` (rr_lower = 1, rr_upper = 1)]

# Generate draws for evidence scoring #############################################################
set.seed(825865)
rr_cols <- paste0("rr_",0:999)

rr_table <- readRDS("FILEPATH/rr_table.RDS")
rr_table_clean <- readRDS("FILEPATH/rr_table_clean.RDS")

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

#Add Categories ###################################################################
# reference is hq piped + boil or filter
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

# rr_dcast ############################################################
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

#Water draws final & save #######################################################
water_draws_final <- rbindlist(lapply(paste0("cat",1:12), rr_dcast))
saveRDS(water_draws_final, "FILEPATH/water_draws_final.RDS")

# calculate evidence score #########################################################################
## Load in files ###########################
library(mrbrt002, lib.loc = "FILEPATH")
scorelator_dir <- "FILEPATH"
rr_cols <- paste0("rr_",0:999)
water_draws_final <- readRDS("FILEPATH/water_draws_final.RDS")

# need to run 'repl_python()' to open an interactive Python interpreter,
# then immediately type 'exit' to get back to the R interpreter
# -- this helps to load a required Python package
repl_python()
evidence_score <- import("mrtool.evidence_score.scorelator")

ev_score <- function(category) {
  assign(category, water_draws_final[parameter == category, rr_cols, with = F])
  get(category)[, (rr_cols) := lapply(.SD, log), .SDcols = rr_cols]
  assign(category, as.matrix(get(category)))

  scorelator_obj <- paste0(category, "_scorelator")
  assign(scorelator_obj, evidence_score$Scorelator(
    ln_rr_draws = t(get(category)),
    exposures = array(1),
    score_type = "point"
  ))

  score <- paste0(category, "_score")
  assign(score, get(scorelator_obj)$get_evidence_score(path_to_diagnostic = paste0(scorelator_dir, category, "_ev_score.pdf")))
}

lapply(unique(water_draws_final$parameter)[-12], ev_score)
cat1_score <- 0.9
cat2_score <- 0.94
cat3_score <- 0.95
cat4_score <- 0.93
cat5_score <- 0.97
cat6_score <- 0.99
cat7_score <- 0.95
cat8_score <- 0.99
cat9_score <- 1.02
cat10_score <- 0.84
cat11_score <- 1.16

categories <- paste0(unique(water_draws_final$parameter)[1:9], "_score")
all_scores <- unlist(lapply(categories, get))
n_cat <- length(all_scores)

df <- data.table(cat = categories, score = all_scores, index = seq(n_cat,1,-1))

overall_scores <- c()
for (n in sort(df$index)) {
  to_sum <- seq(1,n,1)
  score <- df[index %in% to_sum, sum(score)]/n
  overall_scores <- append(overall_scores, score)
}
min(overall_scores) # 0.96

##### calc evidence score on MR-BRT model results rather than combined source-treatment RRs #####
set.seed(825865)
rr_cols <- paste0("rr_",0:999)

rr_table <- readRDS("FILEPATH/rr_table.RDS")

gen_rr_draws <- function(type) {
  log_rr <- rr_table[intervention == type, log_rr]
  log_se <- rr_table[intervention == type, log_se]

  rr_draws <- data.table(draws = rnorm(1000, mean = log_rr, sd = log_se))
  setnames(rr_draws, "draws", type)

  return(rr_draws)
}

rr_draws_temp <- lapply(rr_table$intervention[-6], gen_rr_draws)
rr_draws_all <- cbind(rr_draws_temp[[1]],
                      rr_draws_temp[[2]],
                      rr_draws_temp[[3]],
                      rr_draws_temp[[4]],
                      rr_draws_temp[[5]])

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

water_draws_mrbrt <- rbindlist(lapply(names(rr_draws_all), rr_dcast))
saveRDS(water_draws_mrbrt, "FILEPATH/water_draws_mrbrt.RDS")

water_draws_mrbrt <- readRDS("FILEAPTH/water_draws_mrbrt.RDS")

repl_python()
evidence_score <- import("crosswalk.scorelator")
# load model object
water_model <- py_load_object(filename = "FILEPATH/water_model.pkl", pickle = "dill")
scorelator <- evidence_score$Scorelator(water_model, type = "protective", name = "water")
scorelator$plot_model(folder = "FILEPATH") # file name is water_score.pdf
scores <- data.table(
  categories = water_model$vars[1:5],
  score = scorelator$get_score() %>% as.numeric,
  low_score = scorelator$get_score(use_gamma_ub = TRUE) %>% as.numeric
)

scores[, order := c(2, 1, 5, 3, 4)]
overall_scores_mrbrt <- c()
for (n in sort(scores$order)) {
  to_sum <- seq(1, n, 1)
  score <- scores[order %in% to_sum, sum(score)]/n
  overall_scores_mrbrt <- append(overall_scores_mrbrt, score)
}
min(overall_scores_mrbrt)

##### FORMAT FOR SAVE RESULTS #####

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
write.csv(water_df_m, "FILEPATH/rr_1_1.csv", row.names = F)
write.csv(water_df_f, "FILEPATH/rr_1_2.csv", row.names = F)

# save results (run in qlogin with 24 threads & 60G mem)
source("FILEPATH/save_results_risk.R")
save_results_risk(input_dir = "FILEPATH", input_file_pattern = "rr_{location_id}_{sex_id}.csv",
                  modelable_entity_id = 9017, description = "updated using new MR-BRT tool", risk_type = "rr",
                  year_id = c(seq(1990,2015,5),2019,2020:2022), gbd_round_id = 7, decomp_step = "iterative", mark_best = TRUE)

##### RRs WITH FISCHER INFORMATION #####
rr_cols <- paste0("rr_", 0:999)

water_fischer_draws <- fread("FILEPATH/water_fischer_draws.csv")[, -"V1"]

# reshape long
water_fischer_draws <- melt(water_fischer_draws, id.vars = "parameter")

for (x in unique(water_fischer_draws$parameter)) {
  assign(x, water_fischer_draws[parameter == x])
  setnames(get(x), "value", x)
  get(x)[, c("parameter", "variable") := NULL]
}

water_fischer_draws <- cbind(filter, hq_piped, improved, piped, solar)

# transform to normal space
water_fischer_draws[, names(water_fischer_draws) := lapply(.SD, exp), .SDcols = names(water_fischer_draws)]

# reference is hq piped + boil or filter
water_fischer_draws[, cat1 := 1/(hq_piped*filter)]
water_fischer_draws[, cat2 := solar/(hq_piped*filter)]
water_fischer_draws[, cat3 := filter/(hq_piped*filter)]
water_fischer_draws[, cat4 := improved/(hq_piped*filter)]
water_fischer_draws[, cat5 := (improved*solar)/(hq_piped*filter)]
water_fischer_draws[, cat6 := (improved*filter)/(hq_piped*filter)]
water_fischer_draws[, cat7 := piped/(hq_piped*filter)]
water_fischer_draws[, cat8 := (piped*solar)/(hq_piped*filter)]
water_fischer_draws[, cat9 := (piped*filter)/(hq_piped*filter)]
water_fischer_draws[, cat10 := hq_piped/(hq_piped*filter)]
water_fischer_draws[, cat11 := (hq_piped*solar)/(hq_piped*filter)]
water_fischer_draws[, cat12 := (hq_piped*filter)/(hq_piped*filter)]

rr_dcast <- function(col_name) {
  df <- data.table(cols = rr_cols, draws = water_fischer_draws[, get(col_name)])
  df <- dcast(df, . ~ cols, value.var = "draws")[, -"."]
  df[, parameter := paste0(col_name)]
  df[, rr_mean := rowMeans(.SD), .SDcols = rr_cols]
  df[, rr_lower := apply(.SD, 1, quantile, 0.025), .SDcols = rr_cols]
  df[, rr_upper := apply(.SD, 1, quantile, 0.975), .SDcols = rr_cols]
  setcolorder(df, c("parameter","rr_mean","rr_lower","rr_upper",rr_cols))

  return(df)
}

water_fischer_final <- rbindlist(lapply(paste0("cat",1:12), rr_dcast))

# males
water_fischer_m <- expand.grid(cause_id = 302, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 1,
                               mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = paste0("cat",1:12))
water_fischer_m <- merge(water_fischer_m, water_fischer_final, by = "parameter")
setDT(water_fischer_m)
water_fischer_m <- water_fischer_m[order(year_id, age_group_id, parameter)]
setcolorder(water_fischer_m, c("cause_id","location_id","age_group_id","sex_id","year_id","mortality","morbidity","parameter",
                               "rr_mean","rr_lower","rr_upper",rr_cols))
# females
water_fischer_f <- expand.grid(cause_id = 302, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 2,
                               mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = paste0("cat",1:12))
water_fischer_f <- merge(water_fischer_f, water_fischer_final, by = "parameter")
setDT(water_fischer_f)
water_fischer_f <- water_fischer_f[order(year_id, age_group_id, parameter)]
setcolorder(water_fischer_f, c("cause_id","location_id","age_group_id","sex_id","year_id","mortality","morbidity","parameter",
                               "rr_mean","rr_lower","rr_upper",rr_cols))

# save
write.csv(water_fischer_m, "FILEPATH/rr_1_1.csv", row.names = F)
write.csv(water_fischer_f, "FILEPATH/rr_1_2.csv", row.names = F)

# save results (run in qlogin with 24 threads & 60G mem)
source("FILEPATH/save_results_risk.R")
save_results_risk(input_dir = "FILEPATH", input_file_pattern = "rr_{location_id}_{sex_id}.csv",
                  modelable_entity_id = 9017, description = "RRs updated with Fischer information boost", risk_type = "rr",
                  year_id = c(seq(1990,2015,5),2019,2020:2022), gbd_round_id = 7, decomp_step = "iterative", mark_best = TRUE)

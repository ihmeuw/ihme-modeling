## evidence score for unsafe sanitation

##################################

# CONFIG ##################################################
rm(list = ls()[ls() != "xwalk"])

# load packages
library(crosswalk002, lib.loc = "FILEPATH")

#Libraries
library(data.table) 
library(openxlsx)
library(magrittr)

# load rr dataset
san_rr <- fread("FILEPATH/sanitation_rr.csv")[, .(nid, reference, yearstudywasconducted, intervention_clean, control_edit, effectsize,
                                                                     lower95confidenceinterval,	upper95confidenceinterval, exclude)]


"%unlike%" <- Negate("%like%")
"%ni%" <- Negate("%in%")

cycle<-"GBD2020"
gbd_round<-7 #GBD2020
decomp<-"iterative"

# Edit sanitation rr ###############################################
# Only keep the rows where exclude=0 and delete the exclude column
san_rr <- san_rr[exclude == 0][, exclude := NULL]

# Change column names
setnames(san_rr,
         c("yearstudywasconducted","intervention_clean","control_edit","effectsize","lower95confidenceinterval","upper95confidenceinterval"),
         c("year","intervention","control","rr","lower","upper"))

# Change the NID column to be numeric
san_rr[, nid := as.numeric(nid)]

# Calc se #########################################################
# Calculate the standard error of the rr
san_rr[, se := delta_transform(log(rr), (log(upper)-log(lower))/(2*qnorm(0.975)), transformation = "log_to_linear")[, 2]]

# add columnd that are in log space
san_rr[, log_rr := delta_transform(rr, se, "linear_to_log")[, 1]] # log rr
san_rr[, log_se := delta_transform(rr, se, "linear_to_log")[, 2]] # log se

# Load covariates #######################################################
san_rr_covs <- data.table(read.xlsx("FILEPATH/sanitation_rr_covariates.xlsx"))[-1, -c("study","notes")]

# Change all of the columns values to be numeric
san_rr_covs[, names(san_rr_covs) := lapply(.SD, as.numeric), .SDcols = names(san_rr_covs)]

# subset to only covs that have variation ###################################
# i.e. >1 unique value in the column

#Make a variable that are all of the cv column names listed
cv_cols <- names(san_rr_covs)[names(san_rr_covs) %like% "cv"]

#Make an empty list
cols_delete <- c()

# If a column only has 1 unqiue value put it into the to delete list
for (col in cv_cols) {
  if (length(san_rr_covs[, unique(get(col))]) == 1) {
    print(col)
    cols_delete <- append(cols_delete, col)
  }
}

# Remove all of the columns that are in the to delete list
san_rr_covs <- san_rr_covs[, -cols_delete, with = F]

# merge covs onto main dataset ###########################################
san_rr <- merge(san_rr, san_rr_covs, by = "nid")

# change categorical vars to binary
san_rr[, cv_confounding_uncontrolled_1 := ifelse(cv_confounding_uncontrolled == 1, 1, 0)]
san_rr[, cv_confounding_uncontrolled_2 := ifelse(cv_confounding_uncontrolled == 2, 1, 0)]
san_rr[, cv_confounding_uncontrolled := NULL]

# save san_rr ##############################################
write.csv(san_rr, paste0("FILEPATH/",cycle,"/san_rr.csv", row.names = F))

# Basic model, no covariates ##############################################################
# Prep model
san_data_basic <- CWData(
  df = san_rr,
  obs = "log_rr",
  obs_se = "log_se",
  alt_dorms = "intervention",
  ref_dorms = "control",
  study_id = "nid"
)

#Run model
san_model_basic <- CWModel(
  cwdata = san_data_basic,
  obs_type = "diff_log",
  cov_models = list(
    CovModel(cov_name = "intercept")),
  gold_dorm = "unimproved",
  inlier_pct = 0.9,
  max_iter = 500L, use_random_intercept = TRUE
)

# Use outputs of model to fill in san_rr_basic table
san_rr_basic <- data.table(intervention = san_model_basic$vars, log_rr = san_model_basic$beta %>% as.numeric,
                           log_se = sqrt(as.numeric(san_model_basic$beta_sd)^2+as.numeric(san_model_basic$gamma)))

#Calculate rr and upper/lower quantiles
san_rr_basic[, log_rr_lower := log_rr - (log_se * qnorm(0.975))]
san_rr_basic[, log_rr_upper := log_rr + (log_se * qnorm(0.975))]
san_rr_basic[, rr := exp(log_rr)]
san_rr_basic[, rr_lower := exp(log_rr_lower)]
san_rr_basic[, rr_upper := exp(log_rr_upper)]

# add in bias covariate(s) #####################################################
# loop over covs to see which ones are significant by themselves
covs <- names(san_rr)[names(san_rr) %like% "cv_"]


for (cv in covs[c(1,2,5,6)]) {
  message(cv)

  cv_data <- paste0(cv, "_data")
  assign(cv_data, CWData(
    df = san_rr,
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
    max_iter = 500L, use_random_intercept = TRUE
  ))
}


# P-Value Function ######################################################
get_pval <- function(beta, beta_sd, one_sided = FALSE) {
  zscore <- abs(beta/beta_sd)
  if (one_sided) 1 - pnorm(zscore)
  if (!one_sided) (1 - pnorm(zscore))*2
}

# Find stats of for each cv ##########################################################
for (cv in paste0(covs[covs %unlike% "cv_exposure_study|cv_confounding_nonrandom|cv_confounding_uncontrolled_2"], "_model")) {
  rr_table_name <- gsub("model","rr",cv)
  assign(rr_table_name, data.table(intervention = c(rep(get(cv)$vars[1],2),
                                                    rep(get(cv)$vars[2],2),
                                                    rep(get(cv)$vars[3],2)),
                                   covariate = gsub("_model","",cv),
                                   beta = get(cv)$beta %>% as.numeric,
                                   beta_sd = sqrt(as.numeric(get(cv)$beta_sd)^2+as.numeric(get(cv)$gamma))))
                                   # beta_sd = get(cv)$beta_sd %>% as.numeric))
  get(rr_table_name)[, beta_lower := beta - (beta_sd * qnorm(0.975))]
  get(rr_table_name)[, beta_upper := beta + (beta_sd * qnorm(0.975))]
  get(rr_table_name)[, coef := delta_transform(get(rr_table_name)$beta, get(rr_table_name)$beta_sd, "log_to_linear")[, 1]]
  get(rr_table_name)[, lower := exp(beta_lower)]
  get(rr_table_name)[, upper := exp(beta_upper)]
  get(rr_table_name)[, p_value := get_pval(beta, beta_sd)]

  get(rr_table_name)[, c("beta","beta_sd","beta_lower","beta_upper") := NULL]
}

# List out the stats
for (x in ls()[ls() %like% "cv_" & ls() %like% "_rr"]) {
  message(gsub("_rr","",x))
  print(get(x)[seq(1,5,2)])
}

# run model w/ covariates #########################################################################
# run model with the covariates
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

# save model object
py_save_object(object = san_model, filename = paste0("FILEPATH/",cycle,"/FILEPATH/san_model.pkl"), pickle = "dill")

# load model object ############################################################
san_model <- py_load_object(filename = paste0("FILEPATH/",cycle,"/FILEPATH/san_model.pkl"), pickle = "dill")

#san_rr_final ####################################################################
san_rr_final <- data.table(intervention = c(rep(san_model$vars[1],2),
                                            rep(san_model$vars[2],2),
                                            rep(san_model$vars[3],2)),
                           log_rr = san_model$beta %>% as.numeric, # log of the rr is beta in the model
                           log_se = sqrt(as.numeric(san_model$beta_sd)^2+as.numeric(san_model$gamma)))

san_rr_final[, log_rr_lower := log_rr - (log_se * qnorm(0.975))]
san_rr_final[, log_rr_upper := log_rr + (log_se * qnorm(0.975))]
san_rr_final[, rr := exp(log_rr)]
san_rr_final[, rr_lower := exp(log_rr_lower)]
san_rr_final[, rr_upper := exp(log_rr_upper)]
san_rr_final <- san_rr_final[c(1,3,5)]

saveRDS(san_rr_final, "FILEPATH/san_rr_final.RDS")


# calc evidence score #############################################################
set.seed(825865) 
rr_cols <- paste0("rr_",0:999)

san_rr_final <- readRDS("FILEPATH/san_rr_final.RDS")

gen_rr_draws <- function(type) {
  log_rr <- san_rr_final[intervention == type, log_rr]
  log_se <- san_rr_final[intervention == type, log_se]

  rr_draws <- data.table(draws = rnorm(1000, mean = log_rr, sd = log_se))
  setnames(rr_draws, "draws", type)

  return(rr_draws)
}

rr_draws_temp <- lapply(san_rr_final$intervention[-3], gen_rr_draws)
rr_draws_all <- cbind(rr_draws_temp[[1]],
                      rr_draws_temp[[2]])

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

san_draws_mrbrt <- rbindlist(lapply(names(rr_draws_all), rr_dcast))
saveRDS(san_draws_mrbrt, paste0("FILEPATH/",cycle,"FILEPATH/san_draws_mrbrt.RDS"))


repl_python()
evidence_score <- import("crosswalk.scorelator")
scorelator <- evidence_score$Scorelator(san_model, type = "protective", name = "sanitation")
scorelator$plot_model(folder = "FILEAPTH") 
score <- scorelator$get_score()
low_score <- scorelator$get_score(use_gamma_ub = TRUE)

##### FORMAT FOR SAVE RESULTS #####
rr_cols <- paste0("rr_",0:999)
san_rr_final <- readRDS("FILEPATH/san_rr_final.RDS")

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
write.csv(san_df_m,paste0("FILEPATH/",cycle,"/rr_1_1.csv", row.names = F))
write.csv(san_df_f, paste0("FILEPATH/",cycle,"/rr_1_2.csv", row.names = F))

# save results (run in qlogin with 24 threads & 60G mem)
source("FILEPATH/save_results_risk.R")
save_results_risk(input_dir = paste0("FILEPATH/",cycle), input_file_pattern = "rr_{location_id}_{sex_id}.csv",
                  modelable_entity_id = 9018, description = "updated using new MR-BRT tool", risk_type = "rr",
                  year_id = c(seq(1990,2015,5),2019,2020:2022), gbd_round_id = gbd_round, decomp_step = decomp, mark_best = TRUE)

# RRs WITH FISCHER INFORMATION #####

## Import Fischer ################
san_fischer_draws <- fread("FILEPATH/sanitation_fischer_draws.csv")[, -"V1"]

# reshape long
san_fischer_draws <- melt(san_fischer_draws, id.vars = "parameter")

## Seperate improved and sewer ##############
#make seperate data tables for improved and sewer sanitation, this will just be a datatable with one column and all of the values
for (x in unique(san_fischer_draws$parameter)) {
  assign(x, san_fischer_draws[parameter == x])
  setnames(get(x), "value", x)
  get(x)[, c("parameter", "variable") := NULL]
}

## Combine improved and sewer ###########################
#Combine the improved and sewer data tables made in the previous line
san_fischer_draws <- cbind(improved, sewer)

#not sure what this is doing
san_fischer_draws[, names(san_fischer_draws) := lapply(.SD, exp), .SDcols = names(san_fischer_draws)]

## Calculate improved and unimproved #######################
#Make new calculated columns for improved and unimproved
san_fischer_draws[, imp := improved/sewer]
san_fischer_draws[, unimp := 1/sewer]

#delete the improved and sewer columns, since they are no longer needed
san_fischer_draws[, c("improved", "sewer") := NULL]

## Improved data table ######################
#Make a separate data table that is just the improved column
san_imp_fischer_draws <- san_fischer_draws[, .(imp)]

#Add a parameter column that is just filled in with cat2, and make another column that counts the number of rows with rr_#
san_imp_fischer_draws[, `:=` (parameter = "cat2", cols = rr_cols)]

#Make the table wide, from long
san_imp_fischer_draws <- dcast(san_imp_fischer_draws, parameter ~ cols, value.var = "imp")

#Add 3 columns that calculates the mean, upper, and lower quantiles of all of the data
san_imp_fischer_draws[, `:=` (rr_mean = rowMeans(.SD),
                              rr_lower = apply(.SD, 1, quantile, 0.025),
                              rr_upper = apply(.SD, 1, quantile, 0.975)), .SDcols = rr_cols]

## Unimproved data table #####################
#Make a separate data table that is just the unimproved column
san_unimp_fischer_draws <- san_fischer_draws[, .(unimp)]

#Add a parameter column that is just filled in with cat1, and make another column that counts the number of rows with rr_#
san_unimp_fischer_draws[, `:=` (parameter = "cat1", cols = rr_cols)]

#Make the table wide, from long
san_unimp_fischer_draws <- dcast(san_unimp_fischer_draws, parameter ~ cols, value.var = "unimp")

#Add 3 columns that calculates the mean, upper, and lower quantiles of all of the data
san_unimp_fischer_draws[, `:=` (rr_mean = rowMeans(.SD),
                                rr_lower = apply(.SD, 1, quantile, 0.025),
                                rr_upper = apply(.SD, 1, quantile, 0.975)), .SDcols = rr_cols]

## Fischer Final ##########################
#Combine improved and unimproved by row (only 1 row in each). And add in another 1 row data table that has the parameter as cat3, and mean, upper,
# and lower are all =1
san_fischer_final <- rbindlist(list(san_unimp_fischer_draws, san_imp_fischer_draws,
                                    data.table(parameter = "cat3", rr_mean = 1, rr_lower = 1, rr_upper = 1)), fill = TRUE)

# Fill all of the rr columns with 1, for the cat3 row. 
san_fischer_final[parameter == "cat3", (rr_cols) := 1]

## Males #####################
# Make a table by hand that includes all of the below info, and has a unique combination for each parameter
san_fischer_m <- expand.grid(cause_id = 302, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 1,
                             mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = c("cat1","cat2","cat3"))

# Merge fischer final with the made male table by the parameter column
# This adds the rr columns to the male table. The same rr values are repeated for each unique combo and for each specific rr column
san_fischer_m <- merge(san_fischer_m, san_fischer_final, by = "parameter")

# set it to be a data table
setDT(san_fischer_m)

#Order it by year and age group
san_fischer_m <- san_fischer_m[order(year_id, age_group_id)]

#Reorder the columns
setcolorder(san_fischer_m, c("cause_id","location_id","age_group_id","sex_id","year_id","mortality","morbidity","parameter",
                             "rr_mean","rr_lower","rr_upper",rr_cols))
## females #####################
# Make a table by hand that includes all of the below info, and has a unique combination for each parameter
san_fischer_f <- expand.grid(cause_id = 302, location_id = 1, age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), sex_id = 2,
                             mortality = 1, morbidity = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), parameter = c("cat1","cat2","cat3"))

# Merge fischer final with the made male table by the parameter column
# This adds the rr columns to the male table. The same rr values are repeated for each unique combo and for each specific rr column
san_fischer_f <- merge(san_fischer_f, san_fischer_final, by = "parameter")

# set it to be a data table
setDT(san_fischer_f)

#Order it by year and age group
san_fischer_f <- san_fischer_f[order(year_id, age_group_id)]

#Reorder the columns
setcolorder(san_fischer_f, c("cause_id","location_id","age_group_id","sex_id","year_id","mortality","morbidity","parameter",
                             "rr_mean","rr_lower","rr_upper",rr_cols))

## save Male and Female #########################
write.csv(san_fischer_m, paste0("FILEPATH/",cycle,"/rr_1_1.csv", row.names = F))
write.csv(san_fischer_f, paste0("FILEPATH/",cycle,"/rr_1_2.csv", row.names = F))

#Again not really sure why we are doiing all of this, and not sure why we separated 

## Upload to risk db ############################
#(run in qlogin with 24 threads & 60G mem)
source("FILEPATH/save_results_risk.R")
save_results_risk(input_dir = paste0("FILEPATH/",cycle), input_file_pattern = "rr_{location_id}_{sex_id}.csv",
                  modelable_entity_id = 9018, description = "RRs updated with Fischer information boost", risk_type = "rr",
                  year_id = c(seq(1990,2015,5),2019,2020:2022), gbd_round_id = gbd_round, decomp_step = decomp, mark_best = TRUE)

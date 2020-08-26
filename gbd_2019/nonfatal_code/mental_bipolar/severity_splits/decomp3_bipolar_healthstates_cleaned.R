########

rm(list=ls())

library(openxlsx)
library(data.table)
library(msm)
source("FILEPATH.R")
rlogit <- function(x){exp(x)/(1+exp(x))}

locations <- get_location_metadata(location_set_id=35)

## Load dataset

dataset_filepath <- "FILEPATH.xlsm"
data <-  data.table(read.xlsx(dataset_filepath))

#cv_ population type
data[, `:=` (cv_rep = 0, cv_inp = 0, cv_out = 0, cv_inpandout = 0, cv_gold = 0 , cv_bp1 = 0 , cv_income = 0)]
data[Population.type == "representative", cv_rep := 1]
data[Population.type == "inpatients ", cv_inp := 1]
data[Population.type == "outpatients", cv_out := 1]
data[Population.type == "inpatients and outpatients", cv_inpandout := 1]

#cv_bipolar 1
data[nid %in% c(5,6,14,15,16,19) , cv_bp1 := 1]

#cv_superregion
data[super_region == "High-income" , cv_income := 1]
data[cv_rep == 1 | cv_inpandout == 1, cv_gold := 1]
data <- data[recall_type!="Lifetime"]

## step 1, divide bipolar health states into two groups: residual and the rest
#This is to estimate the proportion of people on both groups before dealing with maci, depressive and mixed

#### RESIDUAL MODEL ---------------------------------------------------------------

data_resid <- data[case_definition == "residual", ]
data_resid[, `:=` (mean_resid = cases / sample_size)] 
data_resid[, `:=` (mean_resid_logit = log(mean_resid / (1-mean_resid)), se_resid_logit = sqrt(1/cases + 1/(sample_size - cases)))]

## estimate the proportion in each group

##Load needed functions ##

repo_dir <- "FILEPATH"
source(paste0(repo_dir, "FILEPATH.R"))
source(paste0(repo_dir, "FILEPATH.R"))
source("FILEPATH.R")

## run mr_brt with remove.intercept = F to get "true betas" and define significance of covariates

fit1 <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = "FILEPATH",
  data = data_resid,
  mean_var = "mean_resid_logit",
  se_var = "se_resid_logit",
  method = "trim_maxl",
  trim_pct = 0.1,
  study_id = "nid",
  covs = list(
    cov_info("cv_gold", "X"),
    cov_info("cv_inp", "X"),
    cov_info("cv_out", "X"),
    cov_info("cv_bp1", "X")),
  remove_x_intercept = T,
  lasso = FALSE,
  overwrite_previous = TRUE
)

## Load Mrbrt outputs- creates an R object containing a model's raw outputs
results  <- data.table(load_mr_brt_outputs(fit1)$model_coef)
results[, `:=` (lower = beta_soln - sqrt(beta_var)*qnorm(0.975, 0, 1), upper = beta_soln + sqrt(beta_var)*qnorm(0.975, 0, 1))]
results[, `:=` (sig = ifelse(lower * upper > 0, "Yes", "No"))]
results[,.(x_cov, beta = round(beta_soln, 2), lower = round(lower, 2), upper = round(upper, 2), sig)]

## make predictions from a Mrbrt model

df_pred <- data.table(cv_gold = c(1, 0, 0, 0), cv_inp = c(0,1,0,0), cv_out = c(0,0,1,0), cv_bp1= c(0,0,0,1))
pred1 <- predict_mr_brt(fit1, newdata = df_pred, write_draws = T)
summaries <- data.table(pred1$model_summaries)
summaries[,.(X_cv_gold, X_cv_inp, X_cv_out, X_cv_bp1, prop = round(rlogit(Y_mean), 2), lower = round(rlogit(Y_mean_lo), 2), upper= round(rlogit(Y_mean_hi), 2))]

resid_draws <- data.table(pred1$model_draws)
names(resid_draws) <- gsub("X_cv_", "cv_", names(resid_draws))

resid_draws <- resid_draws[cv_gold == 1, ]
resid_draws[, `:=` (cause = "bipolar", cv_gold = NULL, cv_inp = NULL, cv_out = NULL, cv_bp1 = NULL , Z_intercept = NULL)]
resid_draws <- melt.data.table(resid_draws,  id.vars = "cause", value.name = "resid_prop", variable.name = "draw")
resid_draws[, resid_prop := rlogit(resid_prop)]
resid_draws[, remaining_prop := 1-resid_prop]

#### REMAINING MODEL ---------------------------------------------------------------

data_manicdep <- data[case_definition == "manic" | case_definition == "depressive",]
data_manicdep <- data_manicdep[nid != 1, ]
data_manicdep[, sample_size := sum(cases), by = "nid"]
data_manic<-  data_manicdep[ case_definition == "manic" ]
data_manic[, `:=` (mean_manic= cases/ sample_size)]
data_manic[, `:=` (mean_manic_logit = log(mean_manic / (1-mean_manic)), se_manic_logit = sqrt(1/cases + 1/(sample_size - cases)))]

## run mr_brt

fit2 <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = "FILEPATH",
  data = data_manic,
  mean_var = "mean_manic_logit",
  se_var = "se_manic_logit",
  method = "trim_maxl",
  trim_pct = 0.1,
  study_id = "nid",
  covs = list(
    cov_info("cv_gold", "X"),
    cov_info("cv_inp", "X"),
    cov_info("cv_out", "X"),
    cov_info("cv_bp1", "X")),
  remove_x_intercept = T,
  lasso = FALSE,
  overwrite_previous = TRUE
)


results  <- data.table(load_mr_brt_outputs(fit2)$model_coef)
results[, `:=` (lower = beta_soln - sqrt(beta_var)*qnorm(0.975, 0, 1), upper = beta_soln + sqrt(beta_var)*qnorm(0.975, 0, 1))]
results[, `:=` (sig = ifelse(lower * upper > 0, "Yes", "No"))]
results[,.(x_cov, beta = round(beta_soln, 2), lower = round(lower, 2), upper = round(upper, 2), sig)]

df_pred <- data.table(cv_gold = c(1, 0, 0 , 0), cv_inp = c(0,1,0, 0), cv_out = c(0,0,1, 0), cv_bp1= c(0,0,0,1))
pred2 <- predict_mr_brt(fit2, newdata = df_pred, write_draws = T)
summaries <- data.table(pred2$model_summaries)
summaries[,.(X_cv_gold, X_cv_inp, X_cv_out, X_cv_bp1, prop = round(rlogit(Y_mean), 2), lower = round(rlogit(Y_mean_lo), 2), upper= round(rlogit(Y_mean_hi), 2))]

manic_draws <- data.table(pred2$model_draws)
names(manic_draws) <- gsub("X_cv_", "cv_", names(manic_draws))
manic_draws <- manic_draws[cv_gold == 1, ]
manic_draws[, `:=` (cause = "bipolar", cv_gold = NULL, cv_inp = NULL, cv_out = NULL, cv_bp1 = NULL,  Z_intercept = NULL)]
manic_draws <- melt.data.table(manic_draws,  id.vars = "cause", value.name = "manic_prop", variable.name = "draw")
manic_draws[, manic_prop := rlogit(manic_prop)]

#### ESTIMATING FINAL PROPORTIONS ---------------------------------------------------------------

final_draws <- merge(resid_draws, manic_draws , by= c("cause", "draw"))
final_draws[, manic_prop := manic_prop * remaining_prop]
final_draws[, `:=` (remaining_prop = NULL, depres_prop = 1- resid_prop - manic_prop)]

final_proportions <- data.table(health_state = c("Residual", "Manic", "Depressive"),
                                mean = c(final_draws[, mean(resid_prop)], final_draws[, mean(manic_prop)], final_draws[, mean(depres_prop)]),
                                lower = c(final_draws[, quantile(resid_prop, 0.025)], final_draws[, quantile(manic_prop, 0.025)], final_draws[, quantile(depres_prop, 0.025)]),
                                upper = c(final_draws[, quantile(resid_prop, 0.975)], final_draws[, quantile(manic_prop, 0.975)], final_draws[, quantile(depres_prop, 0.975)]))

write.csv(final_draws, "FILEPATH.csv", row.names=F)
write.csv(final_proportions, "FILEPATH.csv", row.names=F)


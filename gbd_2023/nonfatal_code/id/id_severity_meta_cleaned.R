################################################################################
### Purpose: ID severity estimation and calculation of disability weights    ###

rm(list=ls())

library(data.table)
library(openxlsx)
library(msm)
library(metafor)
library(ggplot2)
library(gridExtra)
library(mrbrt003, lib.loc = "/FILEPATH/")

invisible(sapply(list.files("/FILEPATH/", full.names = T), source))

rlogit <- function(x){exp(x)/(1+exp(x))}
logit <- function(x){log(x/(1-x))}

get_beta_vcov <- function(model){
  model_specs <- mrbrt003::core$other_sampling$extract_simple_lme_specs(model)
  beta_hessian <- mrbrt003::core$other_sampling$extract_simple_lme_hessian(model_specs)
  solve(beta_hessian) 
}

get_beta_sd <- function(model){
  beta_sd <- sqrt(diag(get_beta_vcov(model)))
  names(beta_sd) <- model$cov_names
  return(beta_sd)
}

input_folder <- "/FILEPATH/id_severity_splits/"

##################################
### Estimate proportions of ID ###

meta <- as.data.table(read.xlsx(paste0(input_folder, "ID_severity_data_final.xlsx"), sheet="clean"))

meta <- meta[!(exclude_both_sex == 1),]

meta[,short_cite := substr(field_citation_value, 0,15)]
meta <- meta[,.(nid, short_cite, location_id, location_name, sex, year_start, year_end, age_start, age_end, cases, sample_size, c_cases,
                prop, IQ_min, IQ_max, cv_mid_age, cv_mid_year, cv_sex)]

meta[,`:=` (row_id = 1:nrow(meta), intercept = 0)]

meta[, cv_mid_age_log := log(cv_mid_age)]
meta[, cv_mid_age_sqrt := sqrt(cv_mid_age)]

meta[, cv_mid_year := round(cv_mid_year)]
meta[, std_mid_age_sqrt := cv_mid_age_sqrt - mean(cv_mid_age_sqrt)]
meta[, std_mid_age_log := cv_mid_age_log - mean(cv_mid_age_log)]
meta[, std_mid_age := cv_mid_age - mean(cv_mid_age)]
meta[, std_mid_year := cv_mid_year - mean(cv_mid_year)]
meta[, std_age_year_intx := std_mid_age_sqrt*std_mid_year]

meta[, unique_study_id := paste0(nid, "_", location_id)]

##########################################
### Calculate total cases of ID by study #

meta[IQ_max <71,`:=` (id_total = sum(c_cases)), by= .(nid, cv_mid_year, cv_mid_age, location_id, sex)] #total ID
meta[IQ_max <50 ,`:=` (mod_lower_total = sum(c_cases)), by= .(nid, cv_mid_year, cv_mid_age, location_id, sex)] #total mod or lower
meta[IQ_max <35 ,`:=` (sev_lower_total = sum(c_cases)), by= .(nid, cv_mid_year, cv_mid_age, location_id, sex)] #total sev or lower

meta[IQ_min==50 & IQ_max==70, prop_mild := (c_cases)/id_total, by= .(nid, cv_mid_year, cv_mid_age, location_id, sex)] #prop mild out of all ID
meta[IQ_min==35 & IQ_max==49, prop_mod := (c_cases)/mod_lower_total, by= .(nid, cv_mid_year, cv_mid_age, location_id, sex)] #prop mod out of total mod or lower
meta[IQ_min==20 & IQ_max==34, prop_sev := (c_cases)/sev_lower_total, by= .(nid, cv_mid_year, cv_mid_age, location_id, sex)] #prop sev out of total sev or lower

meta[IQ_min>49 & IQ_max<86, prop_border := (.SD[IQ_min==71 & IQ_max==85,c_cases])/.SD[IQ_min==50 & IQ_max==70, id_total], by= .(nid, cv_mid_year, cv_mid_age, location_id, sex)] 
meta[IQ_min>49 & IQ_max<86, id_total := ifelse(IQ_min > 70, .SD[IQ_min==50 & IQ_max==70, id_total], id_total), by= .(nid, cv_mid_year, cv_mid_age, location_id, sex)] 


meta_mild <- data.table(escalc(measure="PLO", data = meta[IQ_min==50 & IQ_max==70,], xi=c_cases, ni=id_total))
meta_mild[, `:=` (logit_se = sqrt(vi), logit_prop = yi)]

meta_mod <- data.table(escalc(measure="PLO", data = meta[IQ_min==35 & IQ_max==49,], xi=c_cases, ni=mod_lower_total))
meta_mod[, `:=` (logit_se = sqrt(vi), logit_prop = yi)]

meta_sev <- data.table(escalc(measure="PLO", data = meta[IQ_min==20 & IQ_max==34,], xi=c_cases, ni=sev_lower_total))
meta_sev[, `:=` (logit_se = sqrt(vi), logit_prop = yi)]

meta_bor_pre <- meta[IQ_min > 70 & IQ_max < 86,]
meta_bor_pre[, `:=` (no_id = as.numeric(sample_size)-as.numeric(cases))]

meta_bor <- data.table(escalc(measure="RR", data = meta_bor_pre[IQ_min > 70 & IQ_max < 86,], ai=c_cases, bi=no_id, ci= id_total, di=no_id))
meta_bor[, `:=` (log_se = sqrt(vi), log_ratio = yi)]


###########################
### mrBrt Meta-analysis ###

## Borderline ID split
trimming <- 1 # too few studies to trim
cvs_bor <- c()

selected_covs_bor <- cvs_bor
cov_list <- list(LinearCovModel('intercept', use_re = T))

mr_dataset <- MRData()
mr_dataset$load_df(
  data = meta_bor,
  col_obs = "log_ratio", col_obs_se = "log_se",
  col_covs = as.list(c(cvs_bor, "row_id")), col_study_id = "unique_study_id")

for(c in selected_covs_bor){cov_list <- c(cov_list, list(LinearCovModel(c, use_re = F)))}
model_bor <- MRBRT(data = mr_dataset, cov_models = cov_list, inlier_pct = trimming)
model_bor$fit_model(inner_print_level = 5L, inner_max_iter = 1000L, inner_acceptable_tol=1e-3)

betas <- data.table(cov = model_bor$cov_names, coef = as.numeric(model_bor$beta_soln), se = get_beta_sd(model_bor))
betas[, `:=` (lower = coef-(qnorm(0.975)*se), upper = coef+(qnorm(0.975)*se), z = abs(coef/se), p = (1 - pnorm(abs(coef/se)))*2)]
betas[p == betas[cov != "intercept", max(p) ] & cov != "intercept", .(cov, p)]
betasbr <- betas[,.(model = "Model 1", cov, or = round(exp(coef), 3), beta = round(coef,3), se = round(se,3), lower = round(lower,3), upper = round(upper,3), z = round(z, 3), p = round(p,4))]
betasbr


## Mild ID split
trimming <- .95
cvs_mild <- c()

selected_covs_mild <- cvs_mild
cov_list <- list(LinearCovModel('intercept', use_re = T))

mr_dataset <- MRData()
mr_dataset$load_df(
  data = meta_mild,
  col_obs = "logit_prop", col_obs_se = "logit_se",
  col_covs = as.list(c(cvs_mild, "row_id")), col_study_id = "unique_study_id")

for(c in selected_covs_mild){cov_list <- c(cov_list, list(LinearCovModel(c, use_re = F)))}
model_mild <- MRBRT(data = mr_dataset, cov_models = cov_list, inlier_pct = trimming)
model_mild$fit_model(inner_print_level = 5L, inner_max_iter = 1000L, inner_acceptable_tol=1e-3)

betas <- data.table(cov = model_mild$cov_names, coef = as.numeric(model_mild$beta_soln), se = get_beta_sd(model_mild))
betas[, `:=` (lower = coef-(qnorm(0.975)*se), upper = coef+(qnorm(0.975)*se), z = abs(coef/se), p = (1 - pnorm(abs(coef/se)))*2)]
betas[p == betas[cov != "intercept", max(p) ] & cov != "intercept", .(cov, p)]
betas1 <- betas[,.(model = "Model 1", cov, or = round(exp(coef), 3), beta = round(coef,3), se = round(se,3), lower = round(lower,3), upper = round(upper,3), z = round(z, 3), p = round(p,4))]
betas1 


## Moderate ID split
cvs_mod <- c()

selected_covs_mod <- cvs_mod
cov_list <- list(LinearCovModel('intercept', use_re = T))

mr_dataset <- MRData()
mr_dataset$load_df(
  data = meta_mod,
  col_obs = "logit_prop", col_obs_se = "logit_se",
  col_covs = as.list(c(cvs_mod, "row_id")), col_study_id = "unique_study_id")

for(c in selected_covs_mod){cov_list <- c(cov_list, list(LinearCovModel(c, use_re = F)))}
model_mod <- MRBRT(data = mr_dataset, cov_models = cov_list, inlier_pct = trimming)
model_mod$fit_model(inner_print_level = 5L, inner_max_iter = 1000L, inner_acceptable_tol=1e-3)

betas <- data.table(cov = model_mod$cov_names, coef = as.numeric(model_mod$beta_soln), se = get_beta_sd(model_mod))
betas[, `:=` (lower = coef-(qnorm(0.975)*se), upper = coef+(qnorm(0.975)*se), z = abs(coef/se), p = (1 - pnorm(abs(coef/se)))*2)]
betas[p == betas[cov != "intercept", max(p) ] & cov != "intercept", .(cov, p)]
betas2 <- betas[,.(model = "Model 1", cov, or = round(exp(coef), 3), beta = round(coef,3), se = round(se,3), lower = round(lower,3), upper = round(upper,3), z = round(z, 3), p = round(p,4))]
betas2 


## Severe ID split
cvs_sev <- c()

selected_covs_sev <- cvs_sev
cov_list <- list(LinearCovModel('intercept', use_re = T))

mr_dataset <- MRData()
mr_dataset$load_df(
  data = meta_sev,
  col_obs = "logit_prop", col_obs_se = "logit_se",
  col_covs = as.list(c(cvs_sev, "row_id")), col_study_id = "unique_study_id")

for(c in selected_covs_sev){cov_list <- c(cov_list, list(LinearCovModel(c, use_re = F)))}
model_sev <- MRBRT(data = mr_dataset, cov_models = cov_list, inlier_pct = trimming)
model_sev$fit_model(inner_print_level = 5L, inner_max_iter = 1000L, inner_acceptable_tol=1e-3)

betas <- data.table(cov = model_sev$cov_names, coef = as.numeric(model_sev$beta_soln), se = get_beta_sd(model_sev))
betas[, `:=` (lower = coef-(qnorm(0.975)*se), upper = coef+(qnorm(0.975)*se), z = abs(coef/se), p = (1 - pnorm(abs(coef/se)))*2)]
betas[p == betas[cov != "intercept", max(p) ] & cov != "intercept", .(cov, p)]
betas3 <- betas[,.(model = "Model 1", cov, or = round(exp(coef), 3), beta = round(coef,3), se = round(se,3), lower = round(lower,3), upper = round(upper,3), z = round(z, 3), p = round(p,4))]
betas3 

#############################
## Generate severity draws ##

## borderline draws
selected_covs_bor <- c(cvs_bor, "intercept")
pred_matrix <- data.table(matrix(0, nrow = 1, ncol = length(selected_covs_bor)))
setnames(pred_matrix, selected_covs_bor)

pred_matrix_data <- MRData()
pred_matrix_data$load_df(data = pred_matrix, col_covs=as.list(model_bor$cov_names))
pred_beta_samples <- mrbrt003::core$other_sampling$sample_simple_lme_beta(1000L, model_bor)
gamma_outer_samples <- matrix(rep(model_bor$gamma_soln, each = 1000L), nrow = 1000L)

gamma_outer_samples[,1] <- 0 # remove intercept gamma
pred_draws <- model_bor$create_draws(pred_matrix_data, beta_samples = pred_beta_samples, gamma_samples = gamma_outer_samples, random_study = F)

pred_draws <- data.table(pred_draws)
names(pred_draws) <- paste0("draw_", as.numeric(gsub("V", "", names(pred_draws)))-1)
pred_draws_out_bor <- melt.data.table(pred_draws , id.vars = names(pred_draws)[!(names(pred_draws) %like% "draw")], value.name="ratio", variable.name="draw")
pred_draws_out_bor[, ratio := exp(ratio)]


## Mild draws
selected_covs_mild <- c(cvs_mild, "intercept")
pred_matrix <- data.table(matrix(0, nrow = 1, ncol = length(selected_covs_mild)))
setnames(pred_matrix, selected_covs_mild)

pred_matrix_data <- MRData()
pred_matrix_data$load_df(data = pred_matrix, col_covs=as.list(model_mild$cov_names))
pred_beta_samples <- mrbrt003::core$other_sampling$sample_simple_lme_beta(1000L, model_mild)
gamma_outer_samples <- matrix(rep(model_mild$gamma_soln, each = 1000L), nrow = 1000L) 

gamma_outer_samples[,1] <- 0 # remove intercept gamma
pred_draws <- model_mild$create_draws(pred_matrix_data, beta_samples = pred_beta_samples, gamma_samples = gamma_outer_samples, random_study = F)

pred_draws <- data.table(pred_draws)
names(pred_draws) <- paste0("draw_", as.numeric(gsub("V", "", names(pred_draws)))-1)
pred_draws_out_mild <- melt.data.table(pred_draws , id.vars = names(pred_draws)[!(names(pred_draws) %like% "draw")], value.name="prop", variable.name="draw")
pred_draws_out_mild[,prop := rlogit(prop)]


## Moderate draws
selected_covs_mod <- c(selected_covs_mod, "intercept")
pred_matrix <- data.table(matrix(0, nrow = 1, ncol = length(selected_covs_mod)))
setnames(pred_matrix, selected_covs_mod)

pred_matrix_data <- MRData()
pred_matrix_data$load_df(data = pred_matrix, col_covs=as.list(model_mod$cov_names))
pred_beta_samples <- mrbrt003::core$other_sampling$sample_simple_lme_beta(1000L, model_mod)
gamma_outer_samples <- matrix(rep(model_mod$gamma_soln, each = 1000L), nrow = 1000L) 

gamma_outer_samples[,1] <- 0 # remove intercept gamma
pred_draws <- model_mod$create_draws(pred_matrix_data, beta_samples = pred_beta_samples, gamma_samples = gamma_outer_samples, random_study = F)

pred_draws <- data.table(pred_draws)
names(pred_draws) <- paste0("draw_", as.numeric(gsub("V", "", names(pred_draws)))-1)
pred_draws_out_mod <- melt.data.table(pred_draws , id.vars = names(pred_draws)[!(names(pred_draws) %like% "draw")], value.name="prop", variable.name="draw")
pred_draws_out_mod[,prop := rlogit(prop)]


## Severe draws
selected_covs_sev <- c(selected_covs_sev, "intercept")
pred_matrix <- data.table(matrix(0, nrow = 1, ncol = length(selected_covs_sev)))
setnames(pred_matrix, selected_covs_sev)

pred_matrix_data <- MRData()
pred_matrix_data$load_df(data = pred_matrix, col_covs=as.list(model_sev$cov_names))
pred_beta_samples <- mrbrt003::core$other_sampling$sample_simple_lme_beta(1000L, model_sev)
gamma_outer_samples <- matrix(rep(model_sev$gamma_soln, each = 1000L), nrow = 1000L) 

gamma_outer_samples[,1] <- 0 # remove intercept gamma
pred_draws <- model_sev$create_draws(pred_matrix_data, beta_samples = pred_beta_samples, gamma_samples = gamma_outer_samples, random_study = F)

pred_draws <- data.table(pred_draws)
names(pred_draws) <- paste0("draw_", as.numeric(gsub("V", "", names(pred_draws)))-1)
pred_draws_out_sev <- melt.data.table(pred_draws , id.vars = names(pred_draws)[!(names(pred_draws) %like% "draw")], value.name="prop", variable.name="draw")
pred_draws_out_sev[,prop := rlogit(prop)]

##############################
## Combine draws, est props ##

severity_draws_a <- merge(pred_draws_out_mild[,.(draw, mild_prop = prop)], pred_draws_out_mod[,.(draw, mod_prop = prop)], by = "draw")
severity_draws_b <- merge(severity_draws_a, pred_draws_out_sev[,.(draw, sev_prop = prop)], by = "draw")
severity_draws <- merge(severity_draws_b, pred_draws_out_bor[,.(draw, bor_prop = ratio)], by = "draw")

corr_severity_draws <- copy(severity_draws)

corr_severity_draws[,`:=` (id_bord = bor_prop)]
corr_severity_draws[,`:=` (id_mild = mild_prop)]
corr_severity_draws[,`:=` (id_mod = (1-id_mild)*mod_prop)]
corr_severity_draws[,`:=` (id_sev = (1-id_mild-id_mod)*sev_prop)]
corr_severity_draws[,`:=` (id_prof = (1-id_mild-id_mod-id_sev))]

corr_severity_draws[, prop_check := sum(id_mild, id_mod, id_sev, id_prof), by = 1:nrow(corr_severity_draws)]

corr_severity_draws_l <- melt.data.table(corr_severity_draws, id.vars = "draw", value.name = "severity_prop", variable.name = "healthstate")

prob_draws <- corr_severity_draws_l[(healthstate  %in% c("id_mild","id_mod","id_sev","id_prof","id_bord")),]

## Summaries
# corr_severity_draws_l[, `:=` (mean = mean(severity_prop), lower_prop = quantile(severity_prop, 0.025), upper_prop = quantile(severity_prop, 0.975) ), by = "healthstate"]
# corr_severity_out <-unique(corr_severity_draws_l[,.(healthstate , mean, lower_prop, upper_prop)])
# corr_severity_out

write.xlsx(prob_draws,"/FILEPATH/sev_props.xlsx", rowNames=F)


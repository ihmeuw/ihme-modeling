rm(list=ls()) # Clear memory
detach(package:crosswalk002, unload = T) # Ignore errors
detach(package:mrbrt003, unload = T) # Ignore errors

## sources needed
source("/FILEPATH/get_bundle_version.R")
source("/FILEPATH/save_bundle_version.R")
library(mrbrt003, lib.loc = "/FILEPATH/")
library(openxlsx)
library(msm)

acause <- "mental_pdd" # Specify the acause of your disorder

##### Auto-complete cause meta-data for data prep ------------------------------------------------------------------------

cause_meta_data <- rbind(data.table(acause_label = 'mental_schizo', bundle_id = 152, age_pattern_me = 24004, sex_ratio_by_age = F, covariates = ""),
                         data.table(acause_label = 'mental_unipolar_mdd', bundle_id = 159, age_pattern_me = 23997, sex_ratio_by_age = T, covariates = "cv_recall_1yr, cv_symptom_scales, cv_whs, cv_lay_interviewer, cv_dsm_5"),
                         data.table(acause_label = 'mental_unipolar_dys', bundle_id = 160, age_pattern_me = 23998, sex_ratio_by_age = T, covariates = "cv_recall_1yr, cv_symptom_scales, cv_whs, cv_lay_interviewer"), 
                         data.table(acause_label = 'mental_bipolar', bundle_id = 161, age_pattern_me = 23999, sex_ratio_by_age = F, covariates = "cv_recall_lifetime, cv_bipolar_recall_point"),
                         data.table(acause_label = 'mental_eating_bulimia', bundle_id = 164, age_pattern_me = 24002, sex_ratio_by_age = F, covariates = "cv_recall_1yr, cv_old_dx_criteria"),
                         data.table(acause_label = 'mental_eating_anorexia', bundle_id = 163, age_pattern_me = 24003, sex_ratio_by_age = T, covariates = ""),
                         data.table(acause_label = 'mental_conduct', bundle_id = 168, age_pattern_me = NA, sex_ratio_by_age = F, covariates = ""),
                         data.table(acause_label = 'mental_adhd', bundle_id = 167, age_pattern_me = NA, sex_ratio_by_age = F, covariates = "cv_recall_1yr, cv_no_impairment"),
                         data.table(acause_label = 'mental_other', bundle_id = 757, age_pattern_me = NA, sex_ratio_by_age = F, covariates = "cv_nesarc"),
                         data.table(acause_label = 'mental_pdd', bundle_id = 3071, age_pattern_me = 24001, sex_ratio_by_age = F, covariates = "cv_autism, cv_survey"),
                         data.table(acause_label = 'bullying', bundle_id = 3122, age_pattern_me = NA, sex_ratio_by_age = F, covariates = "cv_low_bullying_freq, cv_no_bullying_def_presented, cv_recall_1yr"),
                         data.table(acause_label = 'mental_anxiety', bundle_id = 162, age_pattern_me = 24000, sex_ratio_by_age = F, covariates = "cv_recall_1yr"))

cause_meta_data <- cause_meta_data[acause_label == acause,]
bundle_id <- cause_meta_data$bundle_id

bundle_metadata <- fread("/FILEPATH/bundle_metadata.csv")

if(cause_meta_data$covariates != ""){crosswalk_pairs <- paste0("/FILEPATH/gbd2021_crosswalk_pairs_", acause, ".csv")}
covariates <- gsub(" ", "", unlist(strsplit(cause_meta_data$covariates, ",")))
sex_ratio_by_age <- cause_meta_data$sex_ratio_by_age

v_id <- bundle_metadata[cause == acause, bundle_version]

##### Estimate and save sex-ratios -----------------------------------------------------------------------
## Reload bundle version
review_sheet <- get_bundle_version(v_id, fetch = 'all')

review_sheet <- review_sheet[measure != 'mtspecific',]

## If Bipolar Disorder, remove Marketscan data
if(acause == "mental_bipolar"){review_sheet <- review_sheet[clinical_data_type == "",]}

## Remove excluded estimates ##
review_sheet[is.na(group_review), group_review := 1]
review_sheet <- review_sheet[group_review == 1, ]
review_sheet[, study_covariate := "ref"]

review_sheet[is.na(standard_error) & !is.na(lower), standard_error := (upper - lower) / (qnorm(0.975,0,1)*2)]
review_sheet[is.na(standard_error) & measure %in% c("prevalence", "proportion"), standard_error := sqrt(1/sample_size * mean * (1-mean) + 1/(4*sample_size^2)*qnorm(0.975,0,1)^2)]
review_sheet[is.na(standard_error) & measure %in% c("incidence", "remission", "mtexcess"), standard_error :=  ifelse(mean*sample_size <= 5, ((5-mean*sample_size) / sample_size + mean * sample_size * sqrt(5/sample_size^2))/5, ((mean*sample_size)^0.5)/sample_size)]

## Create paired dataset where each row is a sex pair ##
match_columns <- c("nid", "age_start", "age_end", "location_id", "site_memo", "year_start", "year_end", "measure")
males <- review_sheet[sex == "Male" & is_outlier == 0, c(match_columns, "mean", "standard_error"), with = F]
females <- review_sheet[sex == "Female" & is_outlier == 0, c(match_columns, "mean", "standard_error"), with = F]
setnames(males, "mean", "mean_m")
setnames(males, "standard_error", "se_m")
setnames(females, "mean", "mean_f")
setnames(females, "standard_error", "se_f")
sex_ratios <- merge(males, females, by = match_columns)

sex_ratios<- sex_ratios[!(measure %in% c("mtspecific")), ]
sex_ratios[, `:=` (ratio = mean_m / mean_f, se = sqrt(((mean_m^2 / mean_f^2) * ((se_m^2) / (mean_m^2) + (se_f^2) / (mean_f^2)))),
                   mid_age = (age_start + age_end) / 2, mid_year = (year_start + year_end) / 2)]
sex_ratios[, log_ratio := log(ratio)]
sex_ratios[, log_ratio_se := deltamethod(~log(x1), ratio, se^2), by = c("ratio", "se")]

table(sex_ratios[!is.na(ratio) & ratio != 0, measure])

# Create measure CVs
measures <- unique(sex_ratios$measure)

for(m in measures){
  sex_ratios[, paste0("cv_", m) := ifelse(measure == m, 1, 0)]
}

# Create covlist
measure_cvs <- paste0("cv_", measures)

for(c in measure_cvs){
  cov <- list(LinearCovModel(c))
  if(c == paste0("cv_", measures)[1]){
    cov_list <- cov
  } else {
    cov_list <- c(cov_list, cov)
  }
}

sex_ratios <- sex_ratios[!is.na(ratio) & ratio != 0 & ratio != Inf,]

mean_mid_age <- sex_ratios[measure == 'prevalence', mean(mid_age)]
sex_ratios[measure == 'prevalence', mc_mid_age := mid_age - mean_mid_age]
sex_ratios[, prev_by_mid_age := ifelse(measure == 'prevalence', cv_prevalence * mc_mid_age, 0)]
if(sex_ratio_by_age == T){
    cov_list <- c(cov_list, list(LinearCovModel("prev_by_mid_age")))
}

#sex_ratios[, `:=` (sex_r = "Male", sex_a = "Female")]

sex_ratio_filepath <- paste0("/FILEPATH/", acause, "/FILEPATH/")

dir.create(file.path(sex_ratio_filepath), recursive = T, showWarnings = FALSE)

## Run MR-BRT ##
sex_data_mrbrt <- MRData()
sex_data_mrbrt$load_df(data = sex_ratios, col_obs = "log_ratio", col_obs_se = "log_ratio_se", col_covs = c(as.list(measure_cvs), list("prev_by_mid_age")), col_study_id = "nid")
model <- MRBRT(data = sex_data_mrbrt, cov_models =cov_list, inlier_pct =0.9)
model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L, inner_acceptable_tol=1e-3)
py_save_object(object = model, filename = paste0(sex_ratio_filepath, "/sex_ratio_model.pkl"), pickle = "dill")

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

sex_coefs <- data.table(cov = model$cov_names, ratio = as.numeric(model$beta_soln), se = get_beta_sd(model))
sex_coefs <- sex_coefs[abs((se^2) / ratio) < 100,]
sex_coefs[, `:=` (lower = ratio-(qnorm(0.975)*se), upper = ratio+(qnorm(0.975)*se), z = abs(ratio/se), p = (1 - pnorm(abs(ratio/se)))*2)]
sex_coefs_raw <- copy(sex_coefs)
sex_coefs[, `:=` (ratio =  exp(ratio), lower = exp(lower), upper = exp(upper), se = deltamethod(~exp(x1), ratio, se^2)), by = "cov"]

write.csv(sex_coefs, paste0(sex_ratio_filepath, "/sex_ratios.csv"),row.names=F)

if(sex_ratio_by_age == F){
  eval(parse(text = paste0("sex_ratio <- expand.grid(", paste0(paste0("cv_", measures), "=c(0, 1)", collapse = ", "), ")")))
} else {
  eval(parse(text = paste0("sex_ratio <- expand.grid(", paste0(paste0("cv_", measures), "=c(0, 1)", collapse = ", "), ", prev_by_mid_age = c((0-mean_mid_age):(100-mean_mid_age), 0))")))
}

sex_ratio_data <- MRData()
sex_ratio_data$load_df(data = sex_ratio, col_covs=as.list(model$cov_names))
sex_beta_samples <- mrbrt003::core$other_sampling$sample_simple_lme_beta(1000L, model)
gamma_outer_samples <- matrix(rep(model$gamma_soln, each = 1000L), nrow = 1000L)
gamma_outer_samples[,1] <- 0 # remove intercept gamma
sex_draws <- model$create_draws(sex_ratio_data, beta_samples = sex_beta_samples, gamma_samples = gamma_outer_samples, random_study = F)
sex_ratio$ratio <- exp(model$predict(data = sex_ratio_data))
sex_ratio$pred_lo <- apply(exp(sex_draws), 1, function(x) quantile(x, 0.025))
sex_ratio$pred_hi <- apply(exp(sex_draws), 1, function(x) quantile(x, 0.975))

sex_ratio <- data.table(sex_ratio)

sex_ratio[, measure := ""]
for(m in names(sex_ratio)[names(sex_ratio) %like% "cv_"]){
  sex_ratio[get(m) == 1, measure := ifelse(measure != "", paste0(measure, ", "), m)]
}
sex_ratio[, `:=` (measure = gsub("cv_", "", measure), ratio_se = (pred_hi - pred_lo) / (qnorm(0.975,0,1)*2))]
sex_ratio <- sex_ratio[measure %in% measures,]
sex_ratio[, (c(paste0("cv_", measures), "pred_hi", "pred_lo")) := NULL]

if(sex_ratio_by_age == T){
  sex_ratio <- sex_ratio[measure == 'prevalence' | (measure != 'prevalence' & prev_by_mid_age == 0),]
  sex_ratio[, `:=` (mid_age = prev_by_mid_age+mean_mid_age, prev_by_mid_age = NULL)]
  sex_ratio[, mid_age := round(mid_age)]
  sex_ratio <- unique(sex_ratio, by = c("measure", "mid_age"))
  for(m in measures[measures != "prevalence"]){
    sex_ratio <- rbind(sex_ratio[measure != m, ], data.table(measure = m, ratio = sex_ratio[measure == m, ratio], ratio_se = sex_ratio[measure == m, ratio_se], mid_age = c(0:100)))
  }
}

write.csv(sex_ratio, paste0(sex_ratio_filepath, "/sex_matrix.csv"),row.names=F)

cov_metadata <- fread("/FILEPATH/cov_metadata.csv")
cov_metadata[, date_run := paste0(date_run)]
write.csv(cov_metadata, paste0("/FILEPATH/cov_metadata_backup_", gsub(":", "-", gsub(" ", "-", Sys.time())), ".csv"), row.names = F)

cov_metadata_rows <- data.table(gbd_round = 2021, date_run = Sys.Date(), cause = acause,	parameter	= gsub("cv_", "", model$cov_model_names), covariate = "sex", gamma= model$gamma_soln,	log_or_logit= "log")
cov_metadata_rows <- merge(cov_metadata_rows, sex_coefs_raw[,.(parameter = gsub("cv_", "", cov), beta = ratio, beta_se = se, beta_lower = lower, beta_upper = upper, beta_p = p)], by = 'parameter')

used_sex_data <- data.table(cbind(model$data$to_df(), data.frame(w = model$w_soln)))
used_sex_data <- melt.data.table(used_sex_data, id.vars = names(used_sex_data)[!(names(used_sex_data) %like% "cv_")], value.name="count", variable.name="parameter")
used_sex_data <- used_sex_data[count > 0,]
used_sex_data <- data.table(table(used_sex_data[,.(parameter, w)]))
used_sex_data[, parameter := gsub("cv_", "", parameter)]
used_sex_data[, `:=` (used_pairs = N * as.numeric(w), trimmed_pairs = N * (1-as.numeric(w)))]
used_sex_data[, `:=` (total_pairs = sum(N), within_pairs = sum(N), between_pairs = 0, used_pairs = sum(used_pairs), trimmed_pairs = sum(trimmed_pairs)), by = "parameter"]
cov_metadata_rows <- merge(cov_metadata_rows, unique(used_sex_data[,.(parameter, total_pairs, within_pairs, between_pairs, used_pairs, trimmed_pairs)]), by = 'parameter')

cov_metadata_rows[, `:=` (beta_gamma_se = sqrt(beta_se^2 + gamma), date_run = as.character(date_run))]
cov_metadata_rows[, `:=` (beta_gamma_lower = beta-(qnorm(0.975)*beta_gamma_se), beta_gamma_upper = beta+(qnorm(0.975)*beta_gamma_se))]
cov_metadata_rows[, `:=` (beta_exp = exp(beta), beta_exp_lower = exp(beta_lower), beta_exp_upper = exp(beta_upper), beta_exp_gamma_lower = exp(beta_gamma_lower), beta_exp_gamma_upper = exp(beta_gamma_upper))]

setcolorder(cov_metadata_rows, c("gbd_round", "date_run", "cause", "parameter", "covariate", "total_pairs", "between_pairs", "within_pairs", "trimmed_pairs", "used_pairs", "beta", "beta_se", "beta_p", "beta_lower",
                                 "beta_upper", "gamma", "beta_gamma_se", "beta_gamma_lower", "beta_gamma_upper", "log_or_logit", "beta_exp", "beta_exp_lower", "beta_exp_upper", "beta_exp_gamma_lower", "beta_exp_gamma_upper"))

cov_metadata <- rbind(cov_metadata[!(paste0(gbd_round, cause, parameter, covariate) %in% cov_metadata_rows[,(paste0(gbd_round, cause, parameter, covariate))])], cov_metadata_rows)

write.csv(cov_metadata, "/FILEPATH/cov_metadata.csv", row.names = F)

##### Estimate and save crosswalks -----------------------------------------------------------------------
detach(package:mrbrt003, unload = T) # Have to detach mrbrt001 package for crosswalk package to work (they have conflicting functions)
library(crosswalk002)

if(length(covariates) > 0){
  crosswalk_pairs_data <- fread(crosswalk_pairs)
  crosswalk_pairs_data <- crosswalk_pairs_data[exclude == ""]
  suppressWarnings(crosswalk_pairs_data[, covariate := NULL])

  covariates <- gsub("cv_", "d_", covariates)

  if(!("r_mid_year" %in% names(crosswalk_pairs_data)) & ("mid_year" %in% names(crosswalk_pairs_data))){crosswalk_pairs_data[,`:=`(r_mid_year=mid_year, a_mid_year=mid_year)]}

  suppressWarnings(crosswalk_pairs_data[is.na(r_year_start), (c("r_year_start", "r_year_end")) := r_mid_year])
  suppressWarnings(crosswalk_pairs_data[is.na(a_year_start), (c("a_year_start", "a_year_end")) := a_mid_year])
  suppressWarnings(crosswalk_pairs_data[is.na(r_age_start), (c("r_age_start", "r_age_end")) := mid_age])
  suppressWarnings(crosswalk_pairs_data[is.na(a_age_start), (c("a_age_start", "a_age_end")) := mid_age])
  suppressWarnings(crosswalk_pairs_data[, `:=` (cv_male = NULL, cv_female = NULL)])

  if("id" %in% names(crosswalk_pairs_data)){
    suppressWarnings(crosswalk_pairs_data[, id := gsub("id_", "", id)])
    suppressWarnings(crosswalk_pairs_data[is.na(r_nid), r_nid := substring(id, 1, nchar(id)/2)])
    suppressWarnings(crosswalk_pairs_data[is.na(a_nid), a_nid := substring(id, nchar(id)/2+1)])
  }

  crosswalk_pairs_data[r_nid >= a_nid, `:=` (unique_label = paste0(r_nid, a_nid, r_year_start, a_year_start, r_year_end, a_year_end, r_age_start, a_age_start, r_age_end, a_age_end, sex, location_id, r_mean, a_mean),
                                             sex_label = paste0(r_nid, a_nid, r_year_start, a_year_start, r_year_end, a_year_end, r_age_start, a_age_start, r_age_end, a_age_end, location_id))]
  crosswalk_pairs_data[r_nid < a_nid, `:=` (unique_label = paste0(a_nid, r_nid, a_year_start, r_year_start, a_year_end, r_year_end, a_age_start, r_age_start, a_age_end, r_age_end, sex, location_id, a_mean, r_mean),
                                            sex_label = paste0(a_nid, r_nid, a_year_start, r_year_start, a_year_end, r_year_end, a_age_start, r_age_start, a_age_end, r_age_end, location_id))]


  for(c in covariates){
    crosswalk_pairs_data[r_nid >= a_nid & get(c) == 1, `:=` (unique_label = paste0(unique_label, 1), sex_label = paste0(sex_label, 1))]
    crosswalk_pairs_data[r_nid >= a_nid & get(c) == 0, `:=` (unique_label = paste0(unique_label, 0), sex_label = paste0(sex_label, 0))]
    crosswalk_pairs_data[r_nid >= a_nid & get(c) == -1, `:=` (unique_label = paste0(unique_label, -1), sex_label = paste0(sex_label, -1))]

    crosswalk_pairs_data[r_nid < a_nid & get(c) == 1, `:=` (unique_label = paste0(unique_label, -1), sex_label = paste0(sex_label, -1))]
    crosswalk_pairs_data[r_nid < a_nid & get(c) == 0, `:=` (unique_label = paste0(unique_label, 0), sex_label = paste0(sex_label, 0))]
    crosswalk_pairs_data[r_nid < a_nid & get(c) == -1, `:=` (unique_label = paste0(unique_label, 1), sex_label = paste0(sex_label, 1))]
  }

  crosswalk_pairs_data <- unique(crosswalk_pairs_data, by = "unique_label")

  ## Remove both-sex estiamte if sex-specific is available or vice versa
  both_labels <- crosswalk_pairs_data[sex == "Both", sex_label]
  sex_labels <- crosswalk_pairs_data[sex != "Both", sex_label]
  
  bothsex <- T
  if(bothsex == T){
    sex_labels <- sex_labels[sex_labels %in% both_labels]
    crosswalk_pairs_data <- crosswalk_pairs_data[!(sex != "Both" & sex_label %in% sex_labels),]
  } else {
    both_labels <- both_labels[both_labels %in% sex_labels]
    crosswalk_pairs_data <- crosswalk_pairs_data[!(sex == "Both" & sex_label %in% both_labels),]
  }

  ## Subset to only within-study pairs for some disorders
  if(acause == "mental_pdd"){
    crosswalk_pairs_data <- crosswalk_pairs_data[cv_between == 0,]
  }
  
  ## Remove male estimates from DSM-IV to DSM-5 crosswalk for anorexia nervosa (as criteria change mostly impacts females)
  if(acause == "mental_eating_anorexia"){
    crosswalk_pairs_data <- crosswalk_pairs_data[!(sex == "Male" & d_icd_dms4 != 0),]
  }

  crosswalk_pairs_data[, `:=` (ref = "gold", alt = "remove")]
  for(c in covariates){
    c_simple <- substring(c, 3)
    crosswalk_pairs_data[get(c) == 1, alt := paste0(alt, "-", c_simple)]
    crosswalk_pairs_data[get(c) == -1, ref := paste0(ref, "-", c_simple)]
  }
  crosswalk_pairs_data[, `:=` (ref = gsub("gold-", "", ref), alt = gsub("remove-", "", alt))]
  crosswalk_pairs_data[alt == "remove", alt := "gold"]

  crosswalk_pairs_data <-crosswalk_pairs_data[r_mean != 0 & a_mean != 0,]

  crosswalk_pairs_data[, row_id := seq_len(.N)]
  crosswalk_pairs_data[, `:=` (logit_alt = delta_transform(mean = a_mean, sd = a_se, transformation = "linear_to_logit")[1],
                               logit_alt_se = delta_transform(mean = a_mean, sd = a_se, transformation = "linear_to_logit")[2],
                               logit_ref = delta_transform(mean = r_mean, sd = r_se, transformation = "linear_to_logit")[1],
                               logit_ref_se = delta_transform(mean = r_mean, sd = r_se, transformation = "linear_to_logit")[2]), by = "row_id"]
  crosswalk_pairs_data[, `:=` (logit_diff = logit_alt - logit_ref, logit_diff_se = sqrt(logit_alt_se^2 + logit_ref_se^2))]

  if(acause %in% c('mental_unipolar_mdd', 'mental_unipolar_dys')){
    crosswalk_pairs_data_symptoms <- crosswalk_pairs_data[d_symptom_scales != 0, ]
    crosswalk_pairs_data <- crosswalk_pairs_data[d_symptom_scales == 0, ]
    covariates <- covariates[covariates != 'd_symptom_scales']
  }

  for(c in covariates){
    cov <- CovModel(c)
    if(c == covariates[1]){
      cov_list <- list(cov)
    } else {
      cov_list <- c(cov_list, list(cov))
    }
  }

  crosswalk_pairs_data[r_nid >= a_nid, id := paste0(r_nid, a_nid)]
  crosswalk_pairs_data[r_nid < a_nid, id := paste0(a_nid, r_nid)]

  ## Remove rows representing unused crosswalks that do not inform the network meta-analysis
  crosswalk_pairs_data <- crosswalk_pairs_data[!(ref == 'gold' & alt == "remove"),]

crosswalk_filepath <- paste0("/FILEPATH/", acause, "/FILEPATH/")

dir.create(file.path(crosswalk_filepath), recursive = T, showWarnings = FALSE)

crosswalk_data_mrbrt <- CWData(df = crosswalk_pairs_data, obs = "logit_diff", obs_se = "logit_diff_se", dorm_separator = "-", alt_dorms = "alt", ref_dorms = "ref", covs = list(), study_id = "id")
crosswalk_fit <- CWModel(cwdata = crosswalk_data_mrbrt, obs_type = "diff_logit", cov_models = list(CovModel("intercept")), gold_dorm = "gold", max_iter = 500L, inlier_pct = .9)

summaries <- data.table(crosswalk_fit$create_result_df()[,1:5])
summaries[, `:=` (lower = beta-(qnorm(0.975)*beta_sd), upper = beta+(qnorm(0.975)*beta_sd), p = (1 - pnorm(abs(beta/beta_sd)))*2)]
summaries[, `:=` (odds =  exp(beta), odds_lower = exp(lower), odds_upper = exp(upper), odds_se = deltamethod(~exp(x1), beta, beta_sd^2)), by = "dorms"]

if(acause %in% c('mental_unipolar_mdd')){ # Estimate network without symptom scales first to ensure symptom scales (the noisy crosswalks) does not impact other crosswalks
  for(c in gsub("d_", "", covariates)){
    crosswalk_pairs_data_symptoms[grepl(c, ref), `:=` (logit_ref = logit_ref - summaries[dorms == c, beta], logit_ref_se = sqrt(logit_ref_se^2 + summaries[dorms == c, beta_sd^2]), add_gamma_ref = 1)]
    crosswalk_pairs_data_symptoms[grepl(c, alt), `:=` (logit_alt = logit_alt - summaries[dorms == c, beta], logit_alt_se = sqrt(logit_alt_se^2 + summaries[dorms == c, beta_sd^2]), add_gamma_alt = 1)]
  }
  gamma <- as.numeric(crosswalk_fit$gamma)
  crosswalk_pairs_data_symptoms[add_gamma_ref == 1, logit_ref_se := sqrt(logit_ref_se^2 + gamma)]
  crosswalk_pairs_data_symptoms[add_gamma_alt == 1, logit_alt_se := sqrt(logit_alt_se^2 + gamma)]
  crosswalk_pairs_data_symptoms[, `:=` (logit_diff = logit_alt - logit_ref, logit_diff_se = sqrt(logit_alt_se^2 + logit_ref_se^2), ref = 'gold', alt = 'gold')]
  crosswalk_pairs_data_symptoms[d_symptom_scales == 1, alt := "symptom_scales"]
  crosswalk_pairs_data_symptoms[d_symptom_scales == -1, ref := "symptom_scales"]

  crosswalk_data_mrbrt_symptoms <- CWData(df = crosswalk_pairs_data_symptoms, obs = "logit_diff", obs_se = "logit_diff_se", dorm_separator = "-", alt_dorms = "alt", ref_dorms = "ref", covs = list(), study_id = "id")
  crosswalk_fit_symptoms <- CWModel(cwdata = crosswalk_data_mrbrt_symptoms, obs_type = "diff_logit", cov_models = list(CovModel("intercept")), gold_dorm = "gold", max_iter = 500L, inlier_pct = .9)
  summaries_symptoms <- data.table(crosswalk_fit_symptoms$create_result_df()[,1:5])
  summaries_symptoms[, `:=` (lower = beta-(qnorm(0.975)*beta_sd), upper = beta+(qnorm(0.975)*beta_sd), p = (1 - pnorm(abs(beta/beta_sd)))*2)]
  summaries_symptoms[, `:=` (odds =  exp(beta), odds_lower = exp(lower), odds_upper = exp(upper), odds_se = deltamethod(~exp(x1), beta, beta_sd^2)), by = "dorms"]
  py_save_object(object = crosswalk_fit_symptoms, filename = paste0(crosswalk_filepath, "/crosswalk_model_symptom_scales.pkl"), pickle = "dill")
  write.csv(summaries_symptoms, paste0(crosswalk_filepath, "/crosswalk_summaries_symptom_scales.csv"),row.names=F)
}

if(acause == "mental_unipolar_dys"){
  summaries <- summaries[dorms %in% c('gold', 'lay_interviewer')]
}

py_save_object(object = crosswalk_fit, filename = paste0(crosswalk_filepath, "/crosswalk_model.pkl"), pickle = "dill")
write.csv(summaries, paste0(crosswalk_filepath, "/crosswalk_summaries.csv"),row.names=F)

## Update cov metadata
cov_gamma <- summaries[1,gamma]
summaries <- summaries[dorms != 'gold',]
cov_metadata_rows <- data.table(gbd_round = 2021, date_run = Sys.Date(), cause = acause,	parameter	= "prevalence", covariate = summaries$dorms, gamma = cov_gamma,	log_or_logit = "logit")
cov_metadata_rows <- merge(cov_metadata_rows, summaries[,.(covariate = dorms, beta, beta_se = beta_sd, beta_lower = lower, beta_upper = upper, beta_p = p)], by = 'covariate')

used_cw_data <- data.table(crosswalk_fit$cwdata$df, w = crosswalk_fit$w)
setnames(used_cw_data, "cv_between", "between_data")
for(d in covariates){used_cw_data[, paste0("cv_", substring(d, 3)) := abs(get(d))]}
used_cw_data <- melt.data.table(used_cw_data, id.vars = names(used_cw_data)[!(names(used_cw_data) %like% "cv_")], value.name="count", variable.name="covariate")
used_cw_data <- used_cw_data[count > 0,]
if(dim(used_cw_data[between_data == 1,.(covariate, w)])[1] > 0){
    used_cw_data_between <- data.table(table(used_cw_data[between_data == 1,.(covariate, w)]), between_data = 1)
} else {
    used_cw_data_between <- data.table()
}
if(dim(used_cw_data[between_data == 0,.(covariate, w)])[1] > 0){
  used_cw_data_within <- data.table(table(used_cw_data[between_data == 0,.(covariate, w)]), between_data = 0)
} else {
  used_cw_data_within <- data.table()
}



used_cw_data <- rbind(used_cw_data_between, used_cw_data_within)
used_cw_data[, covariate := gsub("cv_", "", covariate)]

used_cw_data[, `:=` (used_pairs = N * as.numeric(w), trimmed_pairs = N * (1-as.numeric(w)), between_pairs = N * between_data, within_pairs = N * (1-between_data))]
used_cw_data[, `:=` (total_pairs = sum(N), within_pairs = sum(within_pairs), between_pairs = sum(between_pairs), used_pairs = sum(used_pairs), trimmed_pairs = sum(trimmed_pairs)), by = "covariate"]
cov_metadata_rows <- merge(cov_metadata_rows, unique(used_cw_data[,.(covariate, total_pairs, within_pairs, between_pairs, used_pairs, trimmed_pairs)]), by = 'covariate')

if(acause == "mental_unipolar_mdd"){
  cov_gamma_symptoms <- summaries_symptoms[1,gamma]
  summaries_symptoms <- summaries_symptoms[dorms != 'gold',]
  cov_metadata_rows_symptoms <- data.table(gbd_round = 2021, date_run = Sys.Date(), cause = acause,	parameter	= "prevalence", covariate = summaries_symptoms$dorms, gamma = cov_gamma_symptoms,	log_or_logit = "logit")
  cov_metadata_rows_symptoms <- merge(cov_metadata_rows_symptoms, summaries_symptoms[,.(covariate = dorms, beta, beta_se = beta_sd, beta_lower = lower, beta_upper = upper, beta_p = p)], by = 'covariate')

  used_cw_data <- data.table(crosswalk_fit_symptoms$cwdata$df, w = crosswalk_fit_symptoms$w)
  setnames(used_cw_data, "cv_between", "between_data")
  used_cw_data[, cv_symptom_scales := d_symptom_scales]
  used_cw_data <- melt.data.table(used_cw_data, id.vars = names(used_cw_data)[!(names(used_cw_data) %like% "cv_")], value.name="count", variable.name="covariate")
  used_cw_data <- used_cw_data[count > 0,]
  used_cw_data_between <- data.table(table(used_cw_data[between_data == 1,.(covariate, w)]), between_data = 1)
  used_cw_data_within <- data.table(table(used_cw_data[between_data == 0,.(covariate, w)]), between_data = 0)
  used_cw_data <- rbind(used_cw_data_between, used_cw_data_within)
  used_cw_data[, covariate := gsub("cv_", "", covariate)]

  used_cw_data[, `:=` (used_pairs = N * as.numeric(w), trimmed_pairs = N * (1-as.numeric(w)), between_pairs = N * between_data, within_pairs = N * (1-between_data))]
  used_cw_data[, `:=` (total_pairs = sum(N), within_pairs = sum(within_pairs), between_pairs = sum(between_pairs), used_pairs = sum(used_pairs), trimmed_pairs = sum(trimmed_pairs)), by = "covariate"]

  cov_metadata_rows_symptoms <- merge(cov_metadata_rows_symptoms, unique(used_cw_data[,.(covariate, total_pairs, within_pairs, between_pairs, used_pairs, trimmed_pairs)]), by = 'covariate')
  cov_metadata_rows <- rbind(cov_metadata_rows, cov_metadata_rows_symptoms)
}

cov_metadata_rows[, `:=` (beta_gamma_se = sqrt(beta_se^2 + gamma), date_run = as.character(date_run))]
cov_metadata_rows[, `:=` (beta_gamma_lower = beta-(qnorm(0.975)*beta_gamma_se), beta_gamma_upper = beta+(qnorm(0.975)*beta_gamma_se))]
cov_metadata_rows[, `:=` (beta_exp = exp(beta), beta_exp_lower = exp(beta_lower), beta_exp_upper = exp(beta_upper), beta_exp_gamma_lower = exp(beta_gamma_lower), beta_exp_gamma_upper = exp(beta_gamma_upper))]

setcolorder(cov_metadata_rows, c("gbd_round", "date_run", "cause", "parameter", "covariate", "total_pairs", "between_pairs", "within_pairs", "trimmed_pairs", "used_pairs", "beta", "beta_se", "beta_p", "beta_lower",
                                 "beta_upper", "gamma", "beta_gamma_se", "beta_gamma_lower", "beta_gamma_upper", "log_or_logit", "beta_exp", "beta_exp_lower", "beta_exp_upper", "beta_exp_gamma_lower", "beta_exp_gamma_upper"))

cov_metadata <- rbind(cov_metadata[!(paste0(gbd_round, cause, parameter, covariate) %in% cov_metadata_rows[,(paste0(gbd_round, cause, parameter, covariate))])], cov_metadata_rows)

write.csv(cov_metadata, "/FILEPATH/cov_metadata.csv", row.names = F)

}




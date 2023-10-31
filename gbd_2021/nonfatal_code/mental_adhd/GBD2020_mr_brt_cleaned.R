#############################################################################
## GBD 2020 iterative
############################################################################

rm(list=ls()) # Clear memory

## sources needed
source("/FILEPATH/get_bundle_version.R")
source("/FILEPATH/save_bundle_version.R")
source("/FILEPATH//mr_brt_functions.R")
library(openxlsx)
library(msm)

acause <- "" # Specify the acause of your disorder
need_to_save_bundle_version <- F # T = Need to save a bundle version, F = bundle version already in bundle_metadata.csv

##### Auto-complete cause meta-data for data prep ------------------------------------------------------------------------

cause_meta_data <- rbind(data.table(acause_label = 'mental_schizo', bundle_id = 152, age_pattern_me = 24004, sex_ratio_by_age = F, covariates = ""),
                         data.table(acause_label = 'mental_unipolar_mdd', bundle_id = 159, age_pattern_me = 23997, sex_ratio_by_age = T, covariates = "cv_recall_1yr, cv_symptom_scales, cv_whs, cv_lay_interviewer"),
                         data.table(acause_label = 'mental_unipolar_dys', bundle_id = 160, age_pattern_me = 23998, sex_ratio_by_age = T, covariates = "cv_recall_1yr, cv_symptom_scales, cv_whs, cv_lay_interviewer"), # actually only cv_lay_interviewer but is part of overall Depressive disorder (MDD + Dysthymia) network meta-analysis including MDD's crosswalks
                         data.table(acause_label = 'mental_bipolar', bundle_id = 161, age_pattern_me = 23999, sex_ratio_by_age = F, covariates = "cv_recall_lifetime, cv_bipolar_recall_point"),
                         data.table(acause_label = 'mental_eating_bulimia', bundle_id = 164, age_pattern_me = 24002, sex_ratio_by_age = F, covariates = ""),
                         data.table(acause_label = 'mental_eating_anorexia', bundle_id = 163, age_pattern_me = 24003, sex_ratio_by_age = F, covariates = ""),
                         data.table(acause_label = 'mental_conduct', bundle_id = 168, age_pattern_me = NA, sex_ratio_by_age = F, covariates = ""),
                         data.table(acause_label = 'mental_adhd', bundle_id = 167, age_pattern_me = NA, sex_ratio_by_age = F, covariates = ""),
                         data.table(acause_label = 'mental_other', bundle_id = 757, age_pattern_me = NA, sex_ratio_by_age = F, covariates = "cv_nesarc"),
                         data.table(acause_label = 'mental_pdd', bundle_id = 3071, age_pattern_me = 24001, sex_ratio_by_age = F, covariates = "cv_autism, cv_survey"),
                         data.table(acause_label = 'bullying', bundle_id = 3122, age_pattern_me = NA, sex_ratio_by_age = F, covariates = "cv_low_bullying_freq, cv_no_bullying_def_presented, cv_recall_1yr"),
                         data.table(acause_label = 'mental_anxiety', bundle_id = 162, age_pattern_me = 24000, sex_ratio_by_age = F, covariates = "cv_recall_1yr"))

cause_meta_data <- cause_meta_data[acause_label == acause,]
bundle_id <- cause_meta_data$bundle_id

bundle_metadata <- fread("/FILEPATH/bundle_metadata.csv")

if(cause_meta_data$covariates != ""){crosswalk_pairs <- paste0("/FILEPATH/crosswalk_pairs_", acause, ".csv")}
covariates <- gsub(" ", "", unlist(strsplit(cause_meta_data$covariates, ",")))
sex_ratio_by_age <- cause_meta_data$sex_ratio_by_age

##### Save bundle version if required ------------------------------------------------------------------------------------
if(need_to_save_bundle_version == T){
  write.csv(bundle_metadata, "/FILEPATH/bundle_metadata_backup.csv", row.names = F, na = '')
  if(acause == "mental_bipolar"){
    v_id <- save_bundle_version(bundle_id = bundle_id, decomp_step = 'iterative', gbd_round_id = 7, include_clinical = "claims")
  } else {
    v_id <- save_bundle_version(bundle_id = bundle_id, decomp_step = 'iterative', gbd_round_id = 7)
  }
  v_id <- v_id$bundle_version_id
  bundle_metadata[cause == acause, `:=` (bundle_version = v_id)]
  write.csv(bundle_metadata, "/FILEPATH/bundle_metadata.csv", row.names = F, na = '')
  print("Once a bundle version is saved, do not forget to set 'need_to_save_bundle_version' to FALSE on line 15")
}

v_id <- bundle_metadata[cause == acause, bundle_version]

##### Estimate and save sex-ratios -----------------------------------------------------------------------
## Reload bundle version
review_sheet <- get_bundle_version(v_id, fetch = 'all')
review_sheet <- review_sheet[measure != 'mtspecific',] # Get rid of CSMR data if present (relevant to MDD)

## If Bipolar Disorder, remove Marketscan data
if(acause == "mental_bipolar"){review_sheet <- review_sheet[clinical_data_type == "",]}

## Remove excluded estimates ##
review_sheet[is.na(group_review), group_review := 1]
review_sheet <- review_sheet[group_review == 1, ]
review_sheet[, study_covariate := "ref"]

review_sheet[is.na(standard_error) & !is.na(lower), standard_error := (upper - lower) / (qnorm(0.975,0,1)*2)]
review_sheet[is.na(standard_error) & measure %in% c("prevalence", "proportion"), standard_error := sqrt(1/sample_size * mean * (1-mean) + 1/(4*sample_size^2)*qnorm(0.975,0,1)^2)]
review_sheet[is.na(standard_error) & measure %in% c("incidence", "remission"), standard_error :=  ifelse(mean*sample_size <= 5, ((5-mean*sample_size) / sample_size + mean * sample_size * sqrt(5/sample_size^2))/5, ((mean*sample_size)^0.5)/sample_size)]

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
for(c in paste0("cv_", measures)){
  cov <- cov_info(c, "X")
  if(c == paste0("cv_", measures)[1]){
    cov_list <- list(cov)
  } else {
    cov_list <- c(cov_list, list(cov))
  }
}

if(sex_ratio_by_age == T){
  mean_mid_age <- sex_ratios[measure == 'prevalence', mean(mid_age)]
  sex_ratios[measure == 'prevalence', mc_mid_age := mid_age - mean_mid_age]
  sex_ratios[, prev_by_mid_age := ifelse(measure == 'prevalence', cv_prevalence * mc_mid_age, 0)]
  cov_list <- c(cov_list, list(cov_info("prev_by_mid_age", "X")))
}

sex_ratio_filepath <- paste0("/FILEPATH/", acause, "/sex_ratio/")

dir.create(file.path(sex_ratio_filepath), recursive = T, showWarnings = FALSE)

## Run MR-BRT ##
model <- run_mr_brt(
  output_dir = sex_ratio_filepath,
  model_label = "sex",
  data = sex_ratios[!is.na(ratio) & ratio != 0 & ratio != Inf,],
  mean_var = "log_ratio",
  se_var = "log_ratio_se",
  covs = cov_list,
  remove_x_intercept = T,
  method = "trim_maxL",
  trim_pct = 0.1,
  study_id = "nid",
  overwrite_previous = TRUE,
  lasso = F)

saveRDS(object = model, file = paste0(sex_ratio_filepath, "model_object.rds"))

sex_coefs  <- data.table(load_mr_brt_outputs(model)$model_coef)
sex_coefs[, `:=` (lower = beta_soln - sqrt(beta_var)*qnorm(0.975, 0, 1), upper = beta_soln + sqrt(beta_var)*qnorm(0.975, 0, 1))]
sex_coefs[, `:=` (sig = ifelse(lower * upper > 0, "Yes", "No"))]
sex_coefs <- sex_coefs[abs(beta_var / beta_soln) < 100,] # get rid of estimates with extremely high RSE
sex_coefs

if(sex_ratio_by_age == F){
  eval(parse(text = paste0("sex_ratio <- expand.grid(", paste0(paste0("cv_", measures), "=c(0, 1)", collapse = ", "), ")")))
} else {
  eval(parse(text = paste0("sex_ratio <- expand.grid(", paste0(paste0("cv_", measures), "=c(0, 1)", collapse = ", "), ", prev_by_mid_age = c((0-mean_mid_age):(100-mean_mid_age), 0))")))
}

sex_ratio <- as.data.table(predict_mr_brt(model, newdata = sex_ratio)["model_summaries"])
names(sex_ratio) <- gsub("model_summaries.", "", names(sex_ratio))
names(sex_ratio) <- gsub("X_", "", names(sex_ratio))

sex_ratio[, measure := ""]
for(m in names(sex_ratio)[names(sex_ratio) %like% "cv_"]){
  sex_ratio[get(m) == 1, measure := ifelse(measure != "", paste0(measure, ", "), m)]
}
sex_ratio[, measure := gsub("cv_", "", measure)]
sex_ratio <- sex_ratio[measure %in% measures,]

sex_ratio[, `:=` (ratio = exp(Y_mean), ratio_se = (exp(Y_mean_hi) - exp(Y_mean_lo))/(2*qnorm(0.975,0,1)))]
sex_ratio[, (c(paste0("cv_", measures), "Y_mean", "Z_intercept", "Y_negp", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe")) := NULL]

if(sex_ratio_by_age == T){
  sex_ratio <- sex_ratio[measure == 'prevalence' | (measure != 'prevalence' & prev_by_mid_age == 0),]
  sex_ratio[, `:=` (mid_age = prev_by_mid_age+mean_mid_age, prev_by_mid_age = NULL)]
  sex_ratio[, mid_age := round(mid_age)]
  sex_ratio <- unique(sex_ratio, by = c("measure", "mid_age"))
  for(m in measures[measures != "prevalence"]){
    sex_ratio <- rbind(sex_ratio[measure != m, ], data.table(measure = m, ratio = sex_ratio[measure == m, ratio], ratio_se = sex_ratio[measure == m, ratio_se], mid_age = c(0:100)))
  }
}

write.csv(sex_ratio, paste0(sex_ratio_filepath, "/final_sex_ratios.csv"),row.names=F)

##### Estimate and save crosswalks -----------------------------------------------------------------------
if(length(covariates) > 0){
covariates <- gsub("cv_", "d_", covariates)
for(c in covariates){
  cov <- cov_info(c, "X")
  if(c == covariates[1]){
    cov_list <- list(cov)
  } else {
    cov_list <- c(cov_list, list(cov))
  }
}

crosswalk_filepath <- paste0("/FILEPATH/", acause, "/crosswalks/")

dir.create(file.path(crosswalk_filepath), recursive = T, showWarnings = FALSE)

crosswalk_fit <- run_mr_brt(
  output_dir = crosswalk_filepath,
  model_label = "crosswalk",
  data = crosswalk_pairs,
  mean_var = "log_effect_size",
  se_var = "log_effect_size_se",
  covs = cov_list,
  remove_x_intercept = TRUE,
  method = "trim_maxL",
  trim_pct = 0.1,
  study_id = "id",
  overwrite_previous = TRUE,
  lasso = FALSE)

saveRDS(object = crosswalk_fit, file = paste0(crosswalk_filepath, "model_object.rds"))

eval(parse(text = paste0("predicted <- expand.grid(", paste0(covariates, "=c(0, 1)", collapse = ", "), ")")))
predicted <- as.data.table(predict_mr_brt(crosswalk_fit, newdata = predicted)["model_summaries"])
names(predicted) <- gsub("model_summaries.", "", names(predicted))
names(predicted) <- gsub("X_d_", "cv_", names(predicted))
predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]

predicted[, covariate := ""]
for(c in names(predicted)[names(predicted) %like% "cv_"]){
  predicted[get(c) == 1, covariate := ifelse(covariate != "", paste0(covariate, ", "), c)]
}
predicted[, covariate := gsub("cv_", "", covariate)]
predicted <- predicted[covariate %in% substring(covariates, 3, nchar(covariates)),]

predicted <- predicted[,.(covariate, beta = Y_mean, beta_low = Y_mean_lo, beta_high = Y_mean_hi,
                                              exp_beta = exp(Y_mean), exp_beta_low = exp(Y_mean_lo), exp_beta_high = exp(Y_mean_hi))]
if(acause == "mental_unipolar_dys"){
  predicted <- predicted[covariate == 'lay_interviewer']
}
write.csv(predicted, paste0("/FILEPATH/", acause, "/crosswalks/final_crosswalks.csv"),row.names=F)
}


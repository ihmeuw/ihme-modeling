rm(list=ls()) # Clear memory
detach(package:crosswalk002, unload = T) # Ignore errors
detach(package:mrbrt003, unload = T) # Ignore errors

## sources needed
source("/FILEPATH/upload_bundle_data.R")
source("/FILEPATH/get_bundle_data.R")
source("/FILEPATH/get_bundle_version.R")
source("/FILEPATH/get_crosswalk_version.R")
source("/FILEPATH/save_bundle_version.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/save_crosswalk_version.R")
source("/FILEPATH/get_draws.R")
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_ids.R")

library(crosswalk002)

library(openxlsx)
library(msm)
rlogit <- function(x){exp(x)/(1+exp(x))}
logit <- function(x){log(x/(1-x))}

acause <- "mental_pdd" # Specify the acause of your disorder
crosswalk_description <- "Crosswalk version name"

##### Auto-complete cause meta-data for data prep ------------------------------------------------------------------------

cause_meta_data <- rbind(data.table(acause_label = 'mental_schizo', bundle_id = 152, age_pattern_me = 24004, age_pattern_location = "super_region", sex_ratio_by_age = F, covariates = ""),
                         data.table(acause_label = 'mental_unipolar_mdd', bundle_id = 159, age_pattern_me = 23997, age_pattern_location = "super_region", sex_ratio_by_age = T, covariates = "cv_recall_1yr, cv_symptom_scales, cv_whs, cv_lay_interviewer"),
                         data.table(acause_label = 'mental_unipolar_dys', bundle_id = 160, age_pattern_me = 23998, age_pattern_location = "super_region", sex_ratio_by_age = T, covariates = "cv_recall_1yr, cv_symptom_scales, cv_whs, cv_lay_interviewer"), 
                         data.table(acause_label = 'mental_bipolar', bundle_id = 161, age_pattern_me = 23999, age_pattern_location = "super_region", sex_ratio_by_age = F, covariates = "cv_recall_lifetime, cv_bipolar_recall_point"),
                         data.table(acause_label = 'mental_eating_bulimia', bundle_id = 164, age_pattern_me = 24002, age_pattern_location = "super_region", sex_ratio_by_age = F, covariates = ""),
                         data.table(acause_label = 'mental_eating_anorexia', bundle_id = 163, age_pattern_me = 24003, age_pattern_location = "super_region", sex_ratio_by_age = T, covariates = ""),
                         data.table(acause_label = 'mental_conduct', bundle_id = 168, age_pattern_me = NA, age_pattern_location = "super_region", sex_ratio_by_age = F, covariates = ""),
                         data.table(acause_label = 'mental_adhd', bundle_id = 167, age_pattern_me = NA, age_pattern_location = "super_region", sex_ratio_by_age = F, covariates = ""),
                         data.table(acause_label = 'mental_other', bundle_id = 757, age_pattern_me = NA, age_pattern_location = "super_region", sex_ratio_by_age = F, covariates = "cv_nesarc"),
                         data.table(acause_label = 'mental_pdd', bundle_id = 3071, age_pattern_me = 24001, age_pattern_location = "super_region", sex_ratio_by_age = F, covariates = "cv_autism, cv_survey"),
                         data.table(acause_label = 'bullying', bundle_id = 3122, age_pattern_me = NA, age_pattern_location = "super_region", sex_ratio_by_age = F, covariates = "cv_low_bullying_freq, cv_no_bullying_def_presented, cv_recall_1yr"),
                         data.table(acause_label = 'mental_anxiety', bundle_id = 162, age_pattern_me = 24000, age_pattern_location = "super_region", sex_ratio_by_age = F, covariates = "cv_recall_1yr"))

cause_meta_data <- cause_meta_data[acause_label == acause,]
bundle_id <- cause_meta_data$bundle_id
bundle_metadata <- fread("/FILEPATH/bundle_metadata.csv")
v_id <- bundle_metadata[cause == acause, bundle_version]
covariates <- gsub(" ", "", unlist(strsplit(cause_meta_data$covariates, ",")))
age_pattern_me <- cause_meta_data$age_pattern_me
age_pattern_location <- cause_meta_data$age_pattern_location
sex_ratio_by_age <- cause_meta_data$sex_ratio_by_age

## Reload bundle version
review_sheet <- get_bundle_version(v_id, fetch = 'all')

## Separate Market Scan if Bipolar Disorder
if(acause == "mental_bipolar"){
  review_sheet_mscan <- review_sheet[clinical_data_type == "claims" & grepl("Truven Health Analytics", field_citation_value),]
  review_sheet <- review_sheet[clinical_data_type == "",]
}

## Estimate Nesarc to AUS crosswalk for Other Mental Disorders before relevant data gets removed
if(acause == "mental_other"){
  aus_data <- review_sheet[cv_nesarc == 0 & age_start == 18 & age_end == 99 & sex == "Both", .(mean_aus = mean, se_aus = standard_error)]
  nesarc_data <- review_sheet[cv_nesarc == 1, ]
  nesarc_data[, `:=` (cases = sum(cases), sample_size = sum(sample_size))]
  nesarc_data[, `:=` (mean = cases / sample_size)]
  nesarc_data[, `:=` (standard_error = sqrt(1/sample_size*mean*(1-mean)+1/(4*sample_size^2)*1.96^2))]
  nesarc_data <- unique(nesarc_data[,.(mean_nesarc = mean, se_nesarc = standard_error)])
  x_walk <- data.table(aus_data, nesarc_data)
  x_walk[, `:=` (x_walk = mean_aus / mean_nesarc, x_walk_se = sqrt(((mean_aus^2) / (mean_nesarc^2)) * (((se_aus^2) / (mean_aus^2) + (se_nesarc^2) / (mean_nesarc^2)))))]
}

## Separate csmr if present
if("mtspecific" %in% review_sheet[,measure]){
  review_sheet_csmr <- review_sheet[measure == 'mtspecific',]
  review_sheet <- review_sheet[measure != 'mtspecific',]
}

## Remove excluded estimates
review_sheet[is.na(group_review), group_review := 1]
review_sheet <- review_sheet[group_review == 1, ]
review_sheet[, study_covariate := "ref"]

## Fill SE where missing for crosswalks
review_sheet[is.na(standard_error) & !is.na(lower), standard_error := (upper - lower) / (qnorm(0.975,0,1)*2)]
review_sheet[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(1/sample_size * mean * (1-mean) + 1/(4*sample_size^2)*qnorm(0.975,0,1)^2)]
review_sheet[is.na(standard_error) & measure %in% c("incidence", "remission", 'mtexcess'), standard_error :=  ifelse(mean*sample_size <= 5, ((5-mean*sample_size) / sample_size + mean * sample_size * sqrt(5/sample_size^2))/5, ((mean*sample_size)^0.5)/sample_size)]
review_sheet[,imputed_sample_size := 0]
review_sheet[is.na(sample_size) & is.na(effective_sample_size), imputed_sample_size := 1]
review_sheet[measure == "prevalence" & is.na(sample_size) & !is.na(effective_sample_size), sample_size := effective_sample_size]
review_sheet[measure == "prevalence" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean*(1-mean)/standard_error^2]
review_sheet[measure == "incidence" & is.na(sample_size) & !is.na(effective_sample_size), sample_size := effective_sample_size]
review_sheet[measure == "incidence" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean/standard_error^2]
review_sheet[is.na(cases), cases := sample_size * mean]

##### Apply sex-ratios -----------------------------------------------------------------------
if(acause != "mental_other"){   ## No need for sex crosswalks for Other Mental Disorders where all data is sex-specific
  sex_ratio_filepath <- paste0("/FILEPATH/", acause, "/FILEPATH/")
  sex_ratio <- fread(paste0(sex_ratio_filepath, "/sex_matrix.csv"))

  if(!('incidence' %in% sex_ratio$measure)){
    sex_ratio_inc <- sex_ratio[measure == "prevalence", ]
    sex_ratio <- rbind(sex_ratio, sex_ratio_inc[, measure := "incidence"])
  }

  if(!('mtstandard' %in% sex_ratio$measure) & ('relrisk' %in% sex_ratio$measure)){
    sex_ratio_smr <- sex_ratio[measure == "relrisk", ]
    sex_ratio <- rbind(sex_ratio, sex_ratio_smr[, measure := "mtstandard"])
  }

  if(!('relrisk' %in% sex_ratio$measure) & ('mtstandard' %in% sex_ratio$measure)){
    sex_ratio_rr <- sex_ratio[measure == "mtstandard", ]
    sex_ratio <- rbind(sex_ratio, sex_ratio_rr[, measure := "relrisk"])
  }

  ## Age-sex-split estimates where appropriate
  review_sheet[, unique_study_label := paste0(nid, location_id, site_memo, year_start, year_end, urbanicity_type, gsub(" ", "", case_definition), case_diagnostics)]
  for(c in names(review_sheet)[names(review_sheet) %like% "cv_"]){
    review_sheet[, unique_study_label := paste0(unique_study_label, get(c))]
  }

  review_sheet[, `:=` (min_age = min(age_start), max_age = max(age_end)), by = "unique_study_label"]
  review_sheet[, `:=` (widest_age = ifelse(min_age == age_start & max_age == age_end, 1, 0))]
  review_sheet_bysex_allage <- review_sheet[sex != "Both" & widest_age == 1,]
  review_sheet_bothsex_byage <- review_sheet[sex == "Both" & widest_age == 0 & (unique_study_label %in% review_sheet_bysex_allage$unique_study_label),]

  age_sex_split <- function(x){
    if(!(review_sheet_bysex_allage[unique_study_label == x, unique(measure)] %in% c("mtstandard", "relrisk"))){
      cases_male <- review_sheet_bysex_allage[unique_study_label == x & sex == "Male", cases] / (review_sheet_bysex_allage[unique_study_label == x & sex != "Both", sum(cases)])
      cases_female <- review_sheet_bysex_allage[unique_study_label == x & sex == "Female", cases] / (review_sheet_bysex_allage[unique_study_label == x & sex != "Both", sum(cases)])
      cases_male_se <- sqrt(cases_male*(1-cases_male)/(review_sheet_bysex_allage[unique_study_label == x & sex != "Both", sum(cases)]))
      cases_female_se <- sqrt(cases_female*(1-cases_female)/(review_sheet_bysex_allage[unique_study_label == x & sex != "Both", sum(cases)]))
      sample_male <- review_sheet_bysex_allage[unique_study_label == x & sex == "Male", sample_size] / (review_sheet_bysex_allage[unique_study_label == x & sex != "Both", sum(sample_size)])
      sample_female <- review_sheet_bysex_allage[unique_study_label == x & sex == "Female", sample_size] / (review_sheet_bysex_allage[unique_study_label == x & sex != "Both", sum(sample_size)])
      sample_male_se <- sqrt(sample_male*(1-sample_male)/(review_sheet_bysex_allage[unique_study_label == x & sex != "Both", sum(sample_size)]))
      sample_female_se <- sqrt(sample_female*(1-sample_female)/(review_sheet_bysex_allage[unique_study_label == x & sex != "Both", sum(sample_size)]))
      ratio_male <- cases_male/sample_male
      ratio_female <- cases_female/sample_female
      ratio_male_se <- sqrt((cases_male^2/sample_male^2)*(cases_male_se^2/cases_male^2 + sample_male_se^2/sample_male^2))
      ratio_female_se <- sqrt((cases_female^2/sample_female^2)*(cases_female_se^2/cases_female^2 + sample_female_se^2/sample_female^2))
    } else { 
      ratio_male_num <- review_sheet_bysex_allage[unique_study_label == x & sex == "Male", mean]
      ratio_male_den <-  mean(c(review_sheet_bysex_allage[unique_study_label == x & sex == "Male", mean], review_sheet_bysex_allage[unique_study_label == x & sex == "Female", mean]))
      ratio_male <- ratio_male_num/ratio_male_den
      ratio_female_num <- review_sheet_bysex_allage[unique_study_label == x & sex == "Female", mean]
      ratio_female_den <-  mean(c(review_sheet_bysex_allage[unique_study_label == x & sex == "Male", mean], review_sheet_bysex_allage[unique_study_label == x & sex == "Female", mean]))
      ratio_female <- ratio_female_num/ratio_female_den
      ratio_male_se_num <- review_sheet_bysex_allage[unique_study_label == x & sex == "Male", standard_error]
      ratio_male_se_den <- sqrt(review_sheet_bysex_allage[unique_study_label == x & sex == "Male", standard_error^2] + review_sheet_bysex_allage[unique_study_label == x & sex == "Female", standard_error^2])*(0.5^2)
      ratio_male_se <- sqrt(((ratio_male_num^2)/(ratio_male_den^2))*((ratio_male_se_num^2)/(ratio_male_num^2)+(ratio_male_se_den^2)/(ratio_male_den^2)))
      ratio_female_se_num <- review_sheet_bysex_allage[unique_study_label == x & sex == "Female", standard_error]
      ratio_female_se_den <- sqrt(review_sheet_bysex_allage[unique_study_label == x & sex == "Male", standard_error^2] + review_sheet_bysex_allage[unique_study_label == x & sex == "Female", standard_error^2])*(0.5^2)
      ratio_female_se <- sqrt(((ratio_female_num^2)/(ratio_female_den^2))*((ratio_female_se_num^2)/(ratio_female_num^2)+(ratio_female_se_den^2)/(ratio_female_den^2)))
    }
    male_by_age <- copy(review_sheet_bothsex_byage[unique_study_label == x])
    female_by_age <- copy(review_sheet_bothsex_byage[unique_study_label == x])
    male_by_age[,`:=`(mean=mean*ratio_male, standard_error=sqrt(standard_error^2*ratio_male_se^2 + standard_error^2*ratio_male^2 + ratio_male_se^2*mean^2), sex = "Male")]
    female_by_age[,`:=`(mean=mean*ratio_female, standard_error=sqrt(standard_error^2*ratio_female_se^2 + standard_error^2*ratio_female^2 + ratio_female_se^2*mean^2), sex = "Female")]
    by_age <- rbind(male_by_age, female_by_age)
    rm(male_by_age, female_by_age)
    by_age[, `:=` (study_covariate = "sex", crosswalk_parent_seq = seq, seq = NA, sample_size = NA, cases = NA)]
    return(by_age)
  }

  age_sex_split_list <- lapply(unique(review_sheet_bothsex_byage$unique_study_label), age_sex_split)
  age_sex_split_list <- Reduce(function(x, y){rbind(x, y)}, age_sex_split_list)

  ## Crosswalk both-sex data ##
  review_sheet_both <- review_sheet[sex == "Both" & !(unique_study_label %in% age_sex_split_list$unique_study_label), ]

  review_sheet_both[, `:=` (crosswalk_parent_seq = NA)]

  review_sheet_both[, mid_age := round((age_start + age_end) / 2)]

  population <- get_population(location_id = unique(review_sheet_both$location_id), release_id = 13, age_group_id = c(1, 6:20, 30:32, 235), sex_id = c(1, 2), year_id = seq(min(review_sheet_both$year_start), max(review_sheet_both$year_end)))
  age_ids <- get_ids('age_group')[age_group_id %in% c(1, 6:20, 30:32, 235),]
  suppressWarnings(age_ids[, `:=` (age_start = as.numeric(unlist(strsplit(age_group_name, " "))[1]), age_end = as.numeric(unlist(strsplit(age_group_name, " "))[3])), by = "age_group_id"])
  age_ids[age_group_id == 1, `:=` (age_start = 0, age_end = 4)]
  age_ids[age_group_id == 235, `:=` (age_end = 99)]
  population <- merge(population, age_ids, by = "age_group_id")

  pop_agg <- function(l, a_s, a_e, y_s, y_e, s){
    a_ids <- age_ids[age_start %in% c(a_s:a_e-4) & age_end %in% c(a_s+4:a_e), age_group_id]
    pop <- population[location_id == l & age_group_id %in% a_ids & sex_id == s & year_id %in% c(y_s:y_e),sum(population)]
    return(pop)
  }

  if(sex_ratio_by_age == T){
    review_sheet_both <- merge(review_sheet_both, sex_ratio, by = c("measure", "mid_age"), all.x = T)
  } else {
    review_sheet_both <- merge(review_sheet_both, sex_ratio, by = "measure", all.x = T) 
  }

  review_sheet_both[, `:=` (mid_age = (age_start + age_end) / 2, age_start_r = round(age_start/5)*5, age_end_r = round(age_end/5)*5)]
  review_sheet_both[age_start_r == age_end_r & mid_age < age_start_r, age_start_r := age_start_r - 5]
  review_sheet_both[age_start_r == age_end_r & mid_age >= age_start_r, age_end_r := age_end_r + 5]
  review_sheet_both[, age_end_r := age_end_r - 1]
  review_sheet_both[, pop_m := pop_agg(location_id, age_start_r, age_end_r, year_start, year_end, s = 1), by = "seq"]
  review_sheet_both[, pop_f := pop_agg(location_id, age_start_r, age_end_r, year_start, year_end, s = 2), by = "seq"]
  review_sheet_both[, pop_b := pop_m + pop_f]

  review_sheet_female <- copy(review_sheet_both)
  review_sheet_female[, `:=` (sex = "Female", mean_n = mean * (pop_b), mean_d =(pop_f + ratio * pop_m),
                              var_n = (standard_error^2 * pop_b^2), var_d = ratio_se^2 * pop_m^2)]
  review_sheet_female[!is.na(ratio) & mean > 0, `:=` (mean = mean_n / mean_d, standard_error = sqrt(((mean_n^2) / (mean_d^2)) * (var_n / (mean_n^2) + var_d / (mean_d^2))))]
  review_sheet_female[, `:=` (study_covariate = "sex", crosswalk_parent_seq = seq, seq = NA, sample_size = NA, cases = NA)]

  review_sheet_male <- copy(review_sheet_both)
  review_sheet_male[, `:=` (sex = "Male", mean_n = mean * (pop_b), mean_d =(pop_m + (1/ratio) * pop_f),
                            var_n = (standard_error^2 * pop_b^2), var_d = ratio_se^2 * pop_f^2)]
  review_sheet_male[!is.na(ratio) & mean > 0, `:=` (mean = mean_n / mean_d, standard_error = sqrt(((mean_n^2) / (mean_d^2)) * (var_n / (mean_n^2) + var_d / (mean_d^2))))]
  review_sheet_male[, `:=` (study_covariate = "sex", crosswalk_parent_seq = seq, seq = NA, sample_size = NA, cases = NA)]

  review_sheet_both <- rbind(review_sheet_male, review_sheet_female)

  ## Re-add estimates that are age-sex split using the study sex-ratio
  review_sheet_final <- rbind(review_sheet_both, age_sex_split_list, review_sheet[sex != "Both" & !(unique_study_label %in% age_sex_split_list$unique_study_label), ], fill = T)
  rm(review_sheet_both, review_sheet_male, review_sheet_female)

  col_remove <- c("mid_age", "age_start_r", "age_end_r", "pop_m", "pop_f", "pop_b", "mean_n", "mean_d", "var_n", "var_d", "ratio", "ratio_se")
  review_sheet_final[, (col_remove) := NULL]

  if(length(review_sheet_final$unique_study_label[!(review_sheet_final$unique_study_label %in% review_sheet$unique_study_label)])>0){
    print(paste(unique(review_sheet_final$unique_study_label[!(review_sheet_final$unique_study_label %in% review_sheet$unique_study_label)])))
    stop("STOP! The age-sex-splitting process has accidently dropped the above estimates")
  }

  review_sheet_final[imputed_sample_size == 1, `:=` (cases = NA, sample_size = NA)]
  col_del <- c("imputed_sample_size", "unique_study_label", "min_age", "max_age", "widest_age")
  review_sheet_final[, (col_del) := NULL]

} else {
  review_sheet_final <- review_sheet ## No need for sex crosswalks for Other Mental Disorders where all data is sex-specific
}

##### Loan in and apply study-level covariates -----------------------------------------------------------------------
if(length(covariates) > 0 & acause != "mental_other"){

  crosswalk_model <- py_load_object(filename = paste0("/FILEPATH/", acause, "/FILEPATH/crosswalk_model.pkl"), pickle = "dill")

  if(acause == "mental_unipolar_mdd"){crosswalk_model_symptoms <- py_load_object(filename = paste0("/FILEPATH/", acause, "/FILEPATH/crosswalk_model_symptom_scales.pkl"), pickle = "dill")}

  if(acause == "mental_unipolar_dys"){covariates <- 'cv_lay_interviewer'}

  review_sheet_final[, crosswalk_applied := 'gold']

  for(c in covariates[covariates != "cv_symptom_scales"]){
    c_simple <- gsub("cv_", "", c)
    review_sheet_final[get(c) == 1 & measure == "prevalence", crosswalk_applied := paste0(crosswalk_applied, "-", c_simple)]
  }
  review_sheet_final[crosswalk_applied != "gold", `:=` (crosswalk_applied = gsub("gold-", "", crosswalk_applied))]

  review_sheet_final[, fake_non_0 := 0]
  review_sheet_final[mean == 0, `:=` (mean = 0.001, fake_non_0 = 1)] # Just to include in prediction matrix below (estimate returns to 0 later)

  review_sheet_final[, data_id := seq_len(.N)]

  review_sheet_xwalked <- data.table(adjust_orig_vals(fit_object = crosswalk_model, df = review_sheet_final[crosswalk_applied != "gold" & measure == "prevalence" & mean > 0,],
                                           orig_dorms = "crosswalk_applied", orig_vals_mean = "mean", orig_vals_se = "standard_error", data_id = "data_id"))
  review_sheet_xwalked[, gamma := as.numeric(crosswalk_model$gamma)]

  if(acause == "mental_unipolar_mdd"){
    review_sheet_final[, crosswalk_applied_symptoms := 'gold']
    review_sheet_final[cv_symptom_scales == 1 & measure == "prevalence", crosswalk_applied_symptoms := "symptom_scales"]
    review_sheet_xwalked_symptoms <- data.table(adjust_orig_vals(fit_object = crosswalk_model_symptoms, df = review_sheet_final[crosswalk_applied_symptoms != "gold" & measure == "prevalence" & mean > 0,],
                                                         orig_dorms = "crosswalk_applied_symptoms", orig_vals_mean = "mean", orig_vals_se = "standard_error", data_id = "data_id"))
    review_sheet_xwalked_symptoms[, gamma_symptoms := as.numeric(crosswalk_model_symptoms$gamma)]
    review_sheet_xwalked <- merge(review_sheet_xwalked, review_sheet_xwalked_symptoms[,.(data_id, pred_diff_mean_symptoms = pred_diff_mean, pred_diff_sd_symptoms = pred_diff_sd, gamma_symptoms)], by = 'data_id', all = T)
    review_sheet_xwalked[is.na(pred_diff_mean), `:=` (pred_diff_mean = 0, pred_diff_sd = 0, gamma = 0)]
    review_sheet_xwalked[is.na(pred_diff_mean_symptoms), `:=` (pred_diff_mean_symptoms = 0, pred_diff_sd_symptoms = 0, gamma_symptoms = 0)]
    review_sheet_xwalked <- review_sheet_xwalked[, .(data_id, pred_diff_mean = pred_diff_mean + pred_diff_mean_symptoms, pred_diff_sd = sqrt(pred_diff_sd^2 + gamma + pred_diff_sd_symptoms^2 + gamma_symptoms))]
  }

  review_sheet_final <- merge(review_sheet_final, review_sheet_xwalked, by = "data_id", all = T)

  if(acause == "mental_unipolar_mdd"){
    review_sheet_final[crosswalk_applied == "gold" & crosswalk_applied_symptoms == "symptom_scales", crosswalk_applied := "symptom_scales"]
    review_sheet_final[crosswalk_applied != "gold" & crosswalk_applied_symptoms == "symptom_scales", crosswalk_applied := paste0(crosswalk_applied, "-symptom_scales")]
    review_sheet_final[, crosswalk_applied_symptoms := NULL]
  }

  # Exponentiate the logit difference to estimate the relative odds and its uncertainty, then use this uncertainty as a close proxy for the logit crosswalk SE
  review_sheet_final[, `:=` (odds = exp(-pred_diff_mean), odds_se = deltamethod(~exp(x1), pred_diff_mean, pred_diff_sd^2)), by = c("pred_diff_mean", "pred_diff_sd")]
  review_sheet_final[crosswalk_applied != "gold" & measure == "prevalence" & fake_non_0 == 1, `:=` (standard_error = sqrt((standard_error^2)*(odds_se^2) +  + (standard_error^2)*(odds^2) + (odds_se^2)*(mean^2)))]

  # Crosswalk other estimates normally
  review_sheet_final[crosswalk_applied != "gold" & measure == "prevalence" & fake_non_0 == 0, `:=` (mean_logit = logit(mean), se_logit = deltamethod(~log(x1/(1-x1)), mean, standard_error^2)), by = c("mean", "standard_error")]
  review_sheet_final[crosswalk_applied != "gold" & measure == "prevalence" & fake_non_0 == 0, `:=` (mean_logit = mean_logit-pred_diff_mean, se_logit = sqrt(se_logit^2 + pred_diff_sd^2))]
  review_sheet_final[crosswalk_applied != "gold" & measure == "prevalence" & fake_non_0 == 0, `:=` (mean = rlogit(mean_logit), standard_error = deltamethod(~exp(x1)/(1+exp(x1)), mean_logit, se_logit^2)), by = c("mean_logit", "se_logit")]

  # Return fake non-zero estimates to equal 0
  review_sheet_final[fake_non_0 == 1, mean := 0]

  # Remove other traces of uncertainty from adjusted estimates
  review_sheet_final[crosswalk_applied != 'gold', `:=` (cases = NA, lower = NA, upper = NA)]

  # Assign crosswalk_parent_seqs
  review_sheet_final[crosswalk_applied != 'gold' & is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq)] 

  for(c in covariates){
    review_sheet_final[get(c) == 1, study_covariate := ifelse(is.na(study_covariate) | study_covariate == "ref", gsub("cv_", "", c), paste0(study_covariate, ", ", gsub("cv_", "", c)))]
  }

  review_sheet_final[, (c("crosswalk_applied", "fake_non_0", "pred_diff_mean", "pred_diff_sd", "odds", "odds_se", "mean_logit", "se_logit")) := NULL]

  ## Adjust and bring back Market Scan data if Bipolar Disorder
  if(acause == "mental_bipolar"){
    ncs_data <- data.table(sex=c("Male", "Female"), mean=c(0.007422402, 0.0078528621616), lower=c(0.00529012136501, 0.00572678678931), upper=c(0.0104051476613, 0.0107597035283), se=c(0.00130600507242, 0.00128495017743), cases=c(33, 38), sample_size=c(4446, 4839))
    review_sheet_mscan[, `:=` (cases_total = sum(cases), sample_total = sum(sample_size)), by = c("nid", "sex")]
    review_sheet_mscan[, `:=` (mean_total = cases_total / sample_total)]
    review_sheet_mscan[, `:=` (se_total = sqrt(1/sample_total*mean_total*(1-mean_total)+1/(4*sample_total^2)*1.96^2))]
    review_sheet_mscan <- merge(review_sheet_mscan, ncs_data[,.(sex, mean_ncs = mean, se_ncs = se)], by = "sex")
    review_sheet_mscan[, `:=` (x_walk = mean_ncs / mean_total, x_walk_se = sqrt(((mean_ncs^2) / (mean_total^2)) * (((se_ncs^2) / (mean_ncs^2) + (se_total^2) / (mean_total^2)))))]
    review_sheet_mscan[, `:=` (mean = mean * x_walk, standard_error = sqrt((standard_error^2) * (x_walk_se^2) + (standard_error^2) * (x_walk^2) + (x_walk_se^2) * (mean^2)))]
    review_sheet_mscan[, `:=` (crosswalk_parent_seq = seq, uncertainty_type_value = NA, lower = NA, upper = NA, cases = NA, study_covariate = "marketscan")]
    review_sheet_mscan[, `:=` (seq = NA, cases_total = NULL, sample_total = NULL, mean_total = NULL, se_total = NULL, mean_ncs = NULL, se_ncs = NULL, x_walk = NULL, x_walk_se = NULL)]
    review_sheet_mscan[, `:=` (group_review = 1, group = nid, specificity = "age, sex")]
    covariate_columns <- names(review_sheet_final)[names(review_sheet_final) %like% "cv_"]
    review_sheet_mscan[, (covariate_columns) := 0]
    review_sheet_final[, `:=` (age_sex_split = NULL, seq_parent = NULL)]
    review_sheet_final <- rbind(review_sheet_final, review_sheet_mscan)
  }
} else if(acause == "mental_other") {
  review_sheet_final[cv_nesarc == 1, `:=` (mean = mean*x_walk$x_walk, standard_error = sqrt((standard_error^2) * (x_walk$x_walk_se^2) + (standard_error^2) * (x_walk$x_walk^2) + (x_walk$x_walk_se^2) * (mean^2)))]
  review_sheet_final[cv_nesarc == 1, `:=` (cases = NA, lower = NA, upper = NA)]
  review_sheet_final[cv_nesarc == 1, `:=` (crosswalk_parent_seq = seq, seq = NA)]
  for(c in covariates){
    c <- gsub("d_", "cv_", c)
    review_sheet_final[get(c) == 1, study_covariate := ifelse(is.na(study_covariate) | study_covariate == "ref", gsub("cv_", "", c), paste0(study_covariate, ", ", gsub("cv_", "", c)))]
  }
}

# Bring back csmr data if separated earlier
if(exists("review_sheet_csmr") == T){
  review_sheet_final <- rbind(review_sheet_final, review_sheet_csmr[group_review == 1,], fill = T)
  review_sheet_final[measure == "mtspecific", study_covariate := "ref"]
}

## Corrections / edits to counter upload validation issues ##
review_sheet_final[study_covariate != "ref", `:=` (lower = NA, upper = NA, cases = NA, sample_size = NA)]
review_sheet_final[is.na(lower), uncertainty_type_value := NA]
review_sheet_final[is.na(group), group := nid]
review_sheet_final[measure == "prevalence" & standard_error >= 1, standard_error := NA]

review_sheet_final[is.na(unit_value_as_published), unit_value_as_published := 1]
review_sheet_final[is.na(specificity), specificity := "Unspecified"]
review_sheet_final[specificity == "", specificity := "Unspecified"]

review_sheet_final[year_start < 1950, year_start := 1950]

duplicate_columns <- data.table(table(tolower(names(review_sheet_final))))[N>1, V1]
if(length(duplicate_columns) > 0){review_sheet_final[,(duplicate_columns) := NULL]} # duplicate colums can exist in database for some bundles and causes upload issues

# Get rid of special characters that seem to appear sometimes from the database
non_proper_char <- c("Ã", "ƒ", "Æ", "’", "‚", "Â", "¢", "â", "¬", "Å", "¡", "¾", "†", "€", "™", "„", "š", "ž", "¦", "…", "œ")
for(n in non_proper_char){
  review_sheet_final[, `:=` (site_memo = gsub(n, "", site_memo))]
  for(note in names(review_sheet_final)[names(review_sheet_final) %like% "note_"]){
    review_sheet_final[, paste0(note) := gsub(n, "", get(note))]
    review_sheet_final[nchar(get(note)) > 1999, paste0(note) := substring(get(note), 1, 1999)]
  }
}

crosswalk_save_folder <- paste0("/FILEPATH/", acause, "/", bundle_id, "/FILEPATH/crosswalk_uploads/")
dir.create(file.path(crosswalk_save_folder), showWarnings = FALSE)
crosswalk_save_file <- paste0(crosswalk_save_folder, "crosswalk_", gsub(":", "-", gsub(" ", "-", Sys.time())), ".xlsx")
write.xlsx(review_sheet_final, crosswalk_save_file, sheetName = "extraction")

##### Upload crosswalked dataset to database -----------------------------------------------------------------------

bundle_metadata <- fread("/FILEPATH/bundle_metadata.csv")
write.csv(bundle_metadata, paste0("/FILEPATH/bundle_metadata_backup_", gsub(":", "-", gsub(" ", "-", Sys.time())), ".csv"), row.names = F, na = '')
save_results <- save_crosswalk_version(v_id, crosswalk_save_file, description = crosswalk_description)
cw_version <- save_results$crosswalk_version_id
bundle_metadata[cause == acause, `:=` (bundle_version = v_id, crosswalk_version = cw_version)]
write.csv(bundle_metadata, "/FILEPATH/bundle_metadata.csv", row.names = F, na = '')


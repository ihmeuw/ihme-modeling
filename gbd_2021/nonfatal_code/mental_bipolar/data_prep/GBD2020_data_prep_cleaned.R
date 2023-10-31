#############################################################################
## GBD 2020 iterative
############################################################################

rm(list=ls()) # Clear memory

## sources needed
source("/FILEPATH/upload_bundle_data.R")
source("/FILEPATH/get_bundle_data.R")
source("/FILEPATH/get_bundle_version.R")
source("/FILEPATH/get_crosswalk_version.R")
source("/FILEPATH/save_bundle_version.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/save_crosswalk_version.R")
source("/FILEPATH/mr_brt_functions.R")
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_ids.R")
library(RMySQL)
odbc <- ini::read.ini('/FILEPATH/.odbc.ini')
con_def <- 'ADDRESS'
get_bun_vers_from_XW <- function(XW_version){
  myconn <- RMySQL::dbConnect(RMySQL::MySQL(),
                              host = odbc[[con_def]]$SERVER,
                              username = odbc[[con_def]]$USER,
                              password = odbc[[con_def]]$PASSWORD)
  df <- dbGetQuery(myconn, sprintf(paste0("SELECT * from crosswalk_version.crosswalk_version cv where cv.crosswalk_version_id = ", XW_version)))
  bun_version <- df$bundle_version_id
  dbDisconnect(myconn)
  return(bun_version)
}

library(openxlsx)
library(msm)

acause <- "" # Specify the acause of your disorder
crosswalk_description <- ""

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
v_id <- bundle_metadata[cause == acause, bundle_version]
age_sex_split_estimates <- paste0("/FILEPATH/age_sex_split_", acause, ".xlsx")
covariates <- gsub(" ", "", unlist(strsplit(cause_meta_data$covariates, ",")))
age_pattern_me <- cause_meta_data$age_pattern_me
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
review_sheet[is.na(standard_error) & measure %in% c("incidence", "remission"), standard_error :=  ifelse(mean*sample_size <= 5, ((5-mean*sample_size) / sample_size + mean * sample_size * sqrt(5/sample_size^2))/5, ((mean*sample_size)^0.5)/sample_size)]

##### Apply sex-ratios -----------------------------------------------------------------------
if(acause != "mental_other"){   ## No need for sex crosswalks for Other Mental Disorders where all data is sex-specific
  sex_ratio_filepath <- paste0("/FILEPATH/", acause, "/sex_ratio/")
  sex_ratio <- fread(paste0(sex_ratio_filepath, "/final_sex_ratios.csv"))

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

  ## Load in estimates that are age-sex split using the study sex-ratio
  age_sex_split <- data.table(read.xlsx(age_sex_split_estimates))
  suppressWarnings(age_sex_split[, `:=` (seq = NULL, age_parent = NULL, sex_parent = NULL, age_parent_seq = NULL)])
  age_sex_split <- age_sex_split[group_review == 1,]
  age_sex_split <- merge(age_sex_split, review_sheet[, .(nid, age_parent_seq = seq, age_start, age_end, year_start, year_end, location_id, site_memo, uq_row_id)],
                        by = c("nid", "age_start", "age_end", "location_id", "site_memo", "year_start", "year_end"), all.x = T)
  if(dim(age_sex_split[is.na(age_parent_seq),.(nid, age_start, age_end, location_id, site_memo, year_start, year_end)])[1] == 0){
    print("All age-sex-split estimates merged correctly, please proceed")
  } else {
    print("Some age-sex-split estimates did not merge correctly, please investigate below")
    age_sex_split[is.na(age_parent_seq),.(nid, age_start, age_end, location_id, site_memo, year_start, year_end)]
  }

  outlier_agesexsplit <- review_sheet[seq %in% age_sex_split$age_parent_seq & is_outlier == 1, seq]
  age_sex_split[age_parent_seq %in% outlier_agesexsplit, is_outlier := 1]

  ## Crosswalk both-sex data ##
  if(age_sex_split_estimates != ""){
    review_sheet_both <- review_sheet[sex == "Both" & !(seq %in% age_sex_split$age_parent_seq), ]
  } else {
    review_sheet_both <- review_sheet[sex == "Both", ]
  }
  review_sheet_both[, `:=` (crosswalk_parent_seq = NA)]

  review_sheet_both[, mid_age := round((age_start + age_end) / 2)]

  population <- get_population(location_id = unique(review_sheet_both$location_id), gbd_round_id = 7, decomp_step = 'iterative', age_group_id = c(1, 6:20, 30:32, 235), sex_id = c(1, 2), year_id = seq(min(review_sheet_both$year_start), max(review_sheet_both$year_end)))
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
  review_sheet_female[!is.na(ratio), `:=` (mean = mean_n / mean_d, standard_error = sqrt(((mean_n^2) / (mean_d^2)) * (var_n / (mean_n^2) + var_d / (mean_d^2))))]
  review_sheet_female[, `:=` (study_covariate = "sex", crosswalk_parent_seq = seq, seq = NA, sample_size = NA, cases = NA)]

  review_sheet_male <- copy(review_sheet_both)
  review_sheet_male[, `:=` (sex = "Male", mean_n = mean * (pop_b), mean_d =(pop_m + (1/ratio) * pop_f),
                            var_n = (standard_error^2 * pop_b^2), var_d = ratio_se^2 * pop_f^2)]
  review_sheet_male[!is.na(ratio), `:=` (mean = mean_n / mean_d, standard_error = sqrt(((mean_n^2) / (mean_d^2)) * (var_n / (mean_n^2) + var_d / (mean_d^2))))]
  review_sheet_male[, `:=` (study_covariate = "sex", crosswalk_parent_seq = seq, seq = NA, sample_size = NA, cases = NA)]

  review_sheet_final <- rbind(review_sheet_male, review_sheet_female, review_sheet[sex != "Both",], fill = T)
  col_remove <- c("mid_age", "age_start_r", "age_end_r", "pop_m", "pop_f", "pop_b", "mean_n", "mean_d", "var_n", "var_d", "ratio", "ratio_se")
  review_sheet_final[, (col_remove) := NULL]

  ## Re-add estimates that are age-sex split using the study sex-ratio
  if(age_sex_split_estimates != ""){
    setnames(age_sex_split, "age_parent_seq", "crosswalk_parent_seq")
    age_sex_split[, `:=` (study_covariate = "sex")]
    review_sheet_final <- review_sheet_final[!(nid %in% age_sex_split$nid),] # remove all-age sex-specific estimates
    review_sheet_final <- rbind(review_sheet_final, age_sex_split, fill = T) # add in age-sex split estimates
  }

  review_sheet_final[is.na(standard_error), standard_error := (upper-lower) / 3.92]

} else {
  review_sheet_final <- review_sheet ## No need for sex crosswalks for Other Mental Disorders where all data is sex-specific
}
##### Loan in and apply study-level covariates -----------------------------------------------------------------------
if(length(covariates) > 0 & acause != "mental_other"){
  covariates <- gsub("cv_", "d_", covariates)
  crosswalks_filepath <- paste0("/FILEPATH/", acause, "/crosswalks/")
  crosswalk_fit <- readRDS(file = paste0(crosswalks_filepath, "model_object.rds"))

  eval(parse(text = paste0("predicted <- expand.grid(", paste0(covariates, "=c(0, 1)", collapse = ", "), ")")))
  predicted <- as.data.table(predict_mr_brt(crosswalk_fit, newdata = predicted)["model_summaries"])
  names(predicted) <- gsub("model_summaries.", "", names(predicted))
  names(predicted) <- gsub("X_d_", "cv_", names(predicted))
  predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
  predicted[, (c("Z_intercept", "Y_negp", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe")) := NULL]
  if(acause == "mental_unipolar_dys"){
    predicted <- predicted[cv_symptom_scales == 0 & cv_whs == 0 & cv_recall_1yr == 0, ]
    predicted[, `:=` (cv_symptom_scales = NULL, cv_whs = NULL, cv_recall_1yr = NULL)]
    covariates <- 'cv_lay_interviewer'
  }

  review_sheet_final <- merge(review_sheet_final, predicted, by=gsub("d_", "cv_", covariates))
  review_sheet_final[, `:=` (log_mean = log(mean), log_se = deltamethod(~log(x1), mean, standard_error^2)), by = c("mean", "standard_error")]

  # estimate inverse of the exponentiated crosswalk to carry over the crosswalk uncertainty to 0 estimates
  review_sheet_final[, `:=` (Y_inv_exp = 1/exp(Y_mean), Y_inv_exp_se = deltamethod(~1/exp(x1), Y_mean, Y_se^2)), by = c("Y_mean", "Y_se")]

  # Crosswalk non 0 estimates using standard method
  review_sheet_final[Y_mean != predicted[1,Y_mean] & mean != 0, `:=` (log_mean = log_mean - Y_mean, log_se = sqrt(log_se^2 + Y_se^2))]
  review_sheet_final[Y_mean != predicted[1,Y_mean] & mean != 0, `:=` (mean = exp(log_mean), standard_error = deltamethod(~exp(x1), log_mean, log_se^2)), by = c("log_mean", "log_se")]

  # Adjust uncertainty of crosswalked 0 estimates by inversing the exponentiated crosswalk and then multiplying the estimate by the inversed exponentiated crosswalk
  review_sheet_final[Y_mean != predicted[1,Y_mean] & mean == 0, `:=` (standard_error = sqrt((standard_error^2)*(Y_inv_exp_se^2) + (standard_error^2)*(Y_inv_exp^2) + (Y_inv_exp_se^2)*(mean^2)))]

  # Remove other traces of uncertainty from adjusted estimates
  review_sheet_final[Y_mean != predicted[1,Y_mean], `:=` (cases = NA, lower = NA, upper = NA)]

  # Assign crosswalk_parent_seqs
  review_sheet_final[Y_mean != predicted[1,Y_mean] & is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq, seq = NA)]

  for(c in covariates){
    c <- gsub("d_", "cv_", c)
    review_sheet_final[get(c) == 1, study_covariate := ifelse(is.na(study_covariate) | study_covariate == "ref", gsub("cv_", "", c), paste0(study_covariate, ", ", gsub("cv_", "", c)))]
  }

  review_sheet_final[, (c("Y_mean", "Y_se", "log_mean", "log_se", "Y_inv_exp", "Y_inv_exp_se")) := NULL]

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
} else {
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
  review_sheet_final <- rbind(review_sheet_final, review_sheet_csmr, fill = T)
  review_sheet_final[measure == "mtspecific", study_covariate := "ref"]
}

## Corrections / edits to counter upload validation issues ##
review_sheet_final[study_covariate != "ref", `:=` (lower = NA, upper = NA, cases = NA, sample_size = NA)]
review_sheet_final[is.na(lower), uncertainty_type_value := NA]
review_sheet_final[is.na(group), group := nid]
review_sheet_final[measure == "prevalence" & standard_error >= 1, standard_error := NA]

review_sheet_final <- review_sheet_final[group_review == 1, ]
review_sheet_final[is.na(unit_value_as_published), unit_value_as_published := 1]
review_sheet_final[is.na(specificity), specificity := "Unspecified"]
review_sheet_final[specificity == "", specificity := "Unspecified"]

duplicate_columns <- data.table(table(tolower(names(review_sheet_final))))[N>1, V1]
if(length(duplicate_columns) > 0){review_sheet_final[,(duplicate_columns) := NULL]}

# Get rid of special characters that seem to appear sometimes from the database
non_proper_char <- c("Ã", "ƒ", "Æ", "’", "‚", "Â", "¢", "â", "¬", "Å", "¡", "¾", "†", "€", "™", "„", "š", "ž", "¦", "…", "œ")
for(n in non_proper_char){
  review_sheet_final[, `:=` (site_memo = gsub(n, "", site_memo))]
  for(note in names(review_sheet_final)[names(review_sheet_final) %like% "note_"]){
    review_sheet_final[, paste0(note) := gsub(n, "", get(note))]
    review_sheet_final[nchar(get(note)) > 1999, paste0(note) := substring(get(note), 1, 1999)]
  }
}

crosswalk_save_folder <- paste0("/FILEPATH/", acause, "/", bundle_id, "/FILEPATH/")
dir.create(file.path(crosswalk_save_folder), showWarnings = FALSE)
crosswalk_save_file <- paste0(crosswalk_save_folder, "crosswalk_", Sys.Date(), ".xlsx")
write.xlsx(review_sheet_final, crosswalk_save_file, sheetName = "extraction")

##### Upload crosswalked dataset to database -----------------------------------------------------------------------

bundle_metadata <- fread("/FILEPATH/bundle_metadata.csv")
write.csv(bundle_metadata, "/FILEPATH/bundle_metadata_backup.csv", row.names = F, na = '')
save_results <- save_crosswalk_version(v_id, crosswalk_save_file, description = crosswalk_description)
cw_version <- save_results$crosswalk_version_id
bundle_metadata[cause == acause, `:=` (bundle_version = v_id, crosswalk_version = cw_version)]
write.csv(bundle_metadata, "/FILEPATH/bundle_metadata.csv", row.names = F, na = '')

#### Pull crosswalk version and age-split it ---------------------------
if(dim(review_sheet_final[(age_end - age_start) > 25 & study_covariate != 'marketscan' & (measure %in% c("prevalence", "incidence")),])[1]>0 & !is.na(age_pattern_me)){
  library(tidyr)
  ##functions
  functions <- paste0("/FILEPATH/")
  source(paste0(functions, "get_ids.R"))
  source(paste0(functions, "get_demographics.R"))

  ##Create merging file
  ages <- get_ids(table = "age_group")
  dems <- get_demographics(gbd_team = "epi")$age_group_id

  ages <- ages[age_group_id %in% dems & !age_group_id %in% c(2, 3, 4), ]
  neonates <- data.table(age_group_name = c("0 to 0.01", "0.01 to 0.1", "0.1 to 0.997"),
                         age_group_id = c(2, 3, 4))

  ages <- rbind(ages, neonates, use.names = TRUE)
  ages <- separate(ages, age_group_name, into = c("age_start", "age_end"), sep = " to ")
  ages[, age_start := gsub(" plus", "", age_start)]
  invisible(ages[age_group_id == 235, age_end := 99])
  ages[, age_start := as.numeric(age_start)]
  ages[, age_end := as.numeric(age_end)]

  source("/FILEPATH/get_draws.R")
  source("/FILEPATH/get_population.R")

  blank_vars <- c("lower", "upper", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", "standard_error", "cases")
  draws <- paste0("draw_", 0:999)
  age <- c(2:20, 30:32, 235) ##epi ages

  ## Redownload crosswalk data
  crosswalked_data <- get_crosswalk_version(cw_version)
  all_age <- copy(crosswalked_data)
  epi_order <- names(all_age)

  ##Get and format data points to split
  all_age <- all_age[measure %in% c("prevalence", "incidence"),]
  if(acause == "mental_unipolar_dys"){all_age <- all_age[nid != 119655,]} # Age-splitting study over-inflates prevalence for these locations.
  all_age <- all_age[!group_review==0 | is.na(group_review),] ## don't use group_review 0
  all_age <- all_age[is_outlier==0,] ## don't age split outliered data
  all_age <- all_age[(age_end-age_start)>25 & study_covariate != 'marketscan',]
  all_age <- all_age[!mean ==0, ] ## don't split points with zero prevalence
  all_age[, sex_id := ifelse(sex == "Male", 1, ifelse(sex == "Female", 2, 3))]

  all_age[measure == "prevalence", measure_id := 5]
  all_age[measure == "incidence", measure_id := 6]
  all_age[, year_id := year_start]

  ##Calculate cases and sample size if missing
  all_age[is.na(sample_size) & is.na(effective_sample_size), imputed_sample_size := 1]

  all_age[measure == "prevalence" & is.na(sample_size) & !is.na(effective_sample_size), sample_size := effective_sample_size]
  all_age[measure == "prevalence" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean*(1-mean)/standard_error^2]
  all_age[measure == "incidence" & is.na(sample_size) & !is.na(effective_sample_size), sample_size := effective_sample_size]
  all_age[measure == "incidence" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean/standard_error^2]
  all_age[is.na(cases), cases := sample_size * mean]
  all_age <- all_age[!cases==0,]

  ## Round age groups to the nearest 5-y boundary
  all_age_round <- copy(all_age)
  all_age_round <- all_age[, age_start := age_start - age_start %%5]
  all_age_round[, age_end := age_end - age_end %%5 + 4]
  all_age_round <- all_age_round[age_end > 99, age_end := 99]

  ## Expand for age
  all_age_round[, n.age:=(age_end+1 - age_start)/5]
  all_age_round[, age_start_floor:=age_start]
  all_age_round[, drop := cases/n.age]
  all_age_round <- all_age_round[!drop<1,]
  all_age_parents <- copy(all_age_round)
  expanded <- rep(all_age_round$seq, all_age_round$n.age) %>% data.table("seq" = .)
  split <- merge(expanded, all_age_round, by="seq", all=T)
  split[,age.rep:= 1:.N - 1, by =.(seq)]
  split[,age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4 ]

  ## Get super region information and merge on
  super_region <- get_location_metadata(location_set_id = 35)
  super_region <- super_region[, .(location_id, super_region_id=region_id)]
  split <- merge(split, super_region, by = "location_id")
  super_regions <- unique(split$super_region_id)

  ## get age group ids
  all_age_total <- merge(split, ages, by = c("age_start", "age_end"), all.x = T)

  ## create age_group_id == 1 for 0-4 age group
  all_age_total[age_start == 0 & age_end == 4, age_group_id := 1]
  all_age_total <- all_age_total[age_group_id %in% age | age_group_id ==1]

  ## get locations and years for population info later
  pop_locs <- unique(all_age_total$location_id)
  pop_years <- unique(all_age_total$year_id)

  ######GET AND FORMAT AGE PATTERN DATA###############################################################
  locations <- super_regions

  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = age_pattern_me,
                         measure_id = c(5,6), location_id = locations, source = "epi",
                         status = "best", sex_id = c(1,2), decomp_step = 'iterative',
                         age_group_id = age, year_id = 2019, gbd_round_id = 6)

  population_data <- get_population(location_id = locations, year_id = 2019, sex_id = c(1, 2),
                                  age_group_id = age, decomp_step = 'iterative')
  population_data <- population_data[, .(age_group_id, sex_id, population, location_id)]
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]

  ## Create age group id 1 (collapse all age groups by summing population weighted rates)
  age_1 <- copy(age_pattern)
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5), ]
  se <- copy(age_1)
  se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)]
  age_1 <- merge(age_1, population_data, by = c("age_group_id", "sex_id", "location_id"))
  age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
  age_1[, frac_pop := population / total_pop]
  age_1[, weight_rate := rate_dis * frac_pop]
  age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
  age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
  age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
  age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
  age_1[, age_group_id := 1]
  age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5)]
  age_pattern <- rbind(age_pattern, age_1)

  ##Get cases and sample size
  age_pattern[measure_id == 5, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0]
  age_pattern[is.nan(cases_us), cases_us := 0]

  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
  ######################################################################################################

  ##merge on age pattern info
  age_pattern1 <- copy(age_pattern)
  all_age_total <- merge(all_age_total, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"), all.x = T)

  ##get population info and merge on
  populations <- get_population(location_id = pop_locs, year_id = pop_years, sex_id = c(1, 2), age_group_id = age, decomp_step = 'iterative')
  age_1 <- copy(populations)
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5)]
  age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
  age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
  age_1[, age_group_id := 1]
  populations <- populations[!age_group_id %in% c(2, 3, 4, 5)]
  populations <- rbind(populations, age_1)
  all_age_total <- merge(all_age_total, populations, by = c("location_id", "sex_id", "year_id", "age_group_id"))

  #####CALCULATE AGE SPLIT POINTS#######################################################################
  ##Create new split data points
  all_age_total[, total_pop := sum(population), by = "seq"]
  all_age_total[, sample_size := (population / total_pop) * sample_size]
  all_age_total[, cases_dis := sample_size * rate_dis]
  all_age_total[, total_cases_dis := sum(cases_dis), by = "seq"]
  all_age_total[, total_sample_size := sum(sample_size), by = "seq"]
  all_age_total[, all_age_rate := total_cases_dis/total_sample_size]
  all_age_total[, ratio := mean / all_age_rate]
  all_age_total[, mean := ratio * rate_dis]
  ######################################################################################################

  ##Epi uploader formatting
  all_age_total[, (blank_vars) := NA]
  all_age_total[!is.na(specificity), specificity := paste0(specificity, ", age-split child")]
  all_age_total[is.na(specificity), specificity := paste0(specificity, "age-split child")]
  all_age_total[, group_review := 1]
  all_age_total <- all_age_total[,c(epi_order), with=F]
  setcolorder(all_age_total, epi_order)
  all_age_total[is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq)]

  ##Add to originals with group review 0
  all_age_parents <- all_age_parents[,c(epi_order), with=F]
  setcolorder(all_age_parents, epi_order)
  parent_seqs <- all_age_parents$seq

  total <- copy(all_age_total)
  rm(all_age_total)

  total[!is.na(note_modeler) & note_modeler != "", note_modeler := paste0(note_modeler, ". Age split using the region age pattern fro me_id ", age_pattern_me, ".")]
  total[is.na(note_modeler) | note_modeler == "", note_modeler := paste0(note_modeler, "Age split using the region age pattern fro me_id ", age_pattern_me, ".")]
  total[, specificity := gsub("NA", "", specificity)]
  total[, note_modeler := gsub("NA", "", note_modeler)]
  total[, study_covariate := ifelse(study_covariate == "ref", "age", paste0(study_covariate, ", age"))]

  total <- rbind(crosswalked_data[!(seq %in% parent_seqs),], total)
  total[study_covariate!="ref", seq := NA]

  total[nchar(note_modeler) > 1999, note_modeler := substring(note_modeler, 1, 1999)]

  ## save file
  crosswalk_save_file <- paste0(crosswalk_save_folder, "crosswalk_", Sys.Date(), "_agesplit_byregion.xlsx")
  write.xlsx(total, crosswalk_save_file, sheetName = "extraction")

}
  ##### Upload crosswalked dataset to database -----------------------------------------------------------------------
if(dim(review_sheet_final[(age_end - age_start) > 25 & study_covariate != 'marketscan' & (measure %in% c("prevalence", "incidence")),])[1]>0 & !is.na(age_pattern_me)){
  bundle_metadata <- fread("/FILEPATH/bundle_metadata.csv")
  write.csv(bundle_metadata, "/FILEPATH/bundle_metadata_backup.csv", row.names = F, na = '')
  save_results <- save_crosswalk_version(v_id, crosswalk_save_file, description = paste0("CW version ", cw_version, " age-split by regional pattern"))
  agesplit_cw_version <- save_results$crosswalk_version_id
  bundle_metadata[cause == acause, `:=` (bundle_version = v_id, crosswalk_version = cw_version, agesplit_crosswalk_version = agesplit_cw_version)]
  write.csv(bundle_metadata, "/FILEPATH/bundle_metadata.csv", row.names = F, na = '')
}



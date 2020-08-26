#######################################################################################
### Date:     06th June 2019
### Purpose:  Estimate Sex-ratio and crosswalks for GBD2019
#######################################################################################

bundle_id <- 167
acause <-"mental_adhd"
uses_csmr <- F
test_sex_by_super_region <- F
crosswalk_pairs <- 'FILEPATH.csv'
new_data_filename <- "FILEPATH.xlsx"
new_age_sex_split_estimates <- ""
decomp_2_bundle_version <- 7391
need_save_bundle_version <- 14825
sex_ratio_by_age <- F

library(data.table)
library(openxlsx)
library(msm)
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")

## upload new data to decomp 4 bundle ##
if(need_save_bundle_version == T){
  upload_bundle_data(bundle_id, filepath = paste0("FILEPATH", acause, "/", bundle_id, "FILEPATH", new_data_filename), decomp_step = 'step4')
}
## same bundle version after new upload and pull review sheet ##
if(need_save_bundle_version == T){
  v_id <- save_bundle_version(bundle_id, "step4")$bundle_version_id
} else {
  v_id <- need_save_bundle_version
}
v_id

review_sheet <- get_bundle_version(v_id)

## Flag if age-split by regional pattern estimates exist ##
if(length(review_sheet[(grepl("age-split child", specificity)),unique(nid)]) > 0){
  print(paste0("STOP! The following nid still has age-split estimates by regional pattern in your bundle version: ", review_sheet[(grepl("age-split child", specificity)),unique(nid)]))
}

## Remove excluded estimates ##
review_sheet[is.na(group_review), group_review := 1]
review_sheet <- review_sheet[group_review == 1, ]
review_sheet[, study_covariate := "ref"]

review_sheet[is.na(standard_error) & !is.na(lower), standard_error := (upper - lower) / (qnorm(0.975,0,1)*2)]
review_sheet[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(1/sample_size * mean * (1-mean) + 1/(4*sample_size^2)*qnorm(0.975,0,1)^2)]
review_sheet[is.na(standard_error) & measure %in% c("incidence", "remission"), standard_error :=  ifelse(mean*sample_size <= 5, ((5-mean*sample_size) / sample_size + mean * sample_size * sqrt(5/sample_size^2))/5, ((mean*sample_size)^0.5)/sample_size)]

##### Estimate and apply sex-ratios -----------------------------------------------------------------------

## Load decomp 2 sex ratios ##
sex_ratio <- fread(paste0("FILEPATH", acause, "/FILEPATH.csv"))

## Load in estimates that are age-sex split using the study sex-ratio
if(new_age_sex_split_estimates != ""){
  age_sex_split <- data.table(read.xlsx(new_age_sex_split_estimates))
  age_sex_split[, seq := NA]
  age_sex_split <- merge(age_sex_split, review_sheet[, .(nid, age_parent_seq = seq, age_start, age_end, location_id, site_memo)],
                         by = c("nid", "age_start", "age_end", "location_id", "site_memo"), all.x = T)
}

## Crosswalk both-sex data ##
if(new_age_sex_split_estimates != ""){
  review_sheet_both <- review_sheet[sex == "Both" & !(seq %in% age_sex_split$age_parent_seq), ]
} else {
  review_sheet_both <- review_sheet[sex == "Both", ]
}
review_sheet_both[, `:=` (crosswalk_parent_seq = NA)]

population <- get_population(location_id = unique(review_sheet_both$location_id), decomp_step = 'step4', age_group_id = c(1, 6:20, 30:32, 235), sex_id = c(1, 2), year_id = seq(min(review_sheet_both$year_start), max(review_sheet_both$year_end)))
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

review_sheet_both <- merge(review_sheet_both, sex_ratio, by = "measure", all.x = T) 

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
if(new_age_sex_split_estimates != ""){
  setnames(age_sex_split, "age_parent_seq", "crosswalk_parent_seq")
  age_sex_split[, `:=` (study_covariate = "sex")]
  review_sheet_final <- review_sheet_final[!(nid %in% age_sex_split$nid),] 
  review_sheet_final <- rbind(review_sheet_final, age_sex_split, fill = T) 
}

# For upload validation #
review_sheet_final[study_covariate != "ref", `:=` (lower = NA, upper = NA, cases = NA, sample_size = NA)]
review_sheet_final[is.na(lower), uncertainty_type_value := NA]

review_sheet_final <- review_sheet_final[group_review == 1, ] 

crosswalk_save_folder <- paste0("FILEPATH", acause, "/", bundle_id, "FILEPATH")
dir.create(file.path(crosswalk_save_folder), showWarnings = FALSE)
crosswalk_save_file <- paste0(crosswalk_save_folder, "crosswalk_", Sys.Date(), "_decomp4.xlsx")
write.xlsx(review_sheet_final, crosswalk_save_file, sheetName = "extraction")

##### Upload crosswalked dataset to database -----------------------------------------------------------------------

save_crosswalk_version(v_id, crosswalk_save_file, description = "Decomp 4 new data - excl outliers")




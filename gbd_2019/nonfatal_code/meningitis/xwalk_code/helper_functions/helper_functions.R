#' @author 
#' @date 2019/03/01
#' @description Helper functions for bundle_28_xwalk.R and upload_decomp4_bundle_28.R       


# DECLARE USER FUNCTIONS --------------------------------------------------
# REPLACE THIS FUNCTION WITH WHATEVER YOU WANT TO USE TO PULL IN DATA
get_dem_data <- function() {
  dt <- get_bundle_version(version_id, transform = T)
  return(dt)
}

# ASSIGN STUDY COVARIATES FOR INPATIENT AND CLAIMS
# recreating missing columns to match the format of clinical data
get_study_cov <- function(raw_dt) {
  #' get study covariate
  #' 
  #' @description fills in study covariate indicator (1 or 0) for 
  #'              cv_marketscan_inp_2000,
  #'              cv_marketscan_data,
  #'              cv_inpatient,
  #'              cv_population_surveillance, and
  #'              cv_sentinel_surveillance
  #'              
  #' 
  #' @param raw_dt data.table. data table to fill in study covariate columns
  #' @return dt data.table. data table with study covariate columns marked
  #' @details cv_inpatient is used as the gold standard for GBD 2019 and all
  #'          other study types are crosswalked up to the gold standard
  #' @note Hard coded nids for marketscan since clinical database does not
  #'       currently have separate identifiers for difference claims data. 
  #'       We want to treat marketscan 2000 data separately due to data 
  #'       quality concerns. Poland claims were new to GBD 2019 and were 
  #'       crosswalked with the same coefficient as marketscan claims
  #' 
  #' @examples get_study_cov(sex_split_final_dt)
  #' 
  dt <- copy(raw_dt)
  ms2000_nids <- c(244369, 336837)
  ms2010_ms2012_nids <- c(244370, 244371, 336848, 336850, 336849, 336203)
  ms2015_ms2016_nids <- c(336847, 408680)
  poland_claims_nids <- c(397812, 397813, 397814)
  dt[nid %in% ms2000_nids, cv_marketscan_inp_2000:= 1]
  dt[nid %in% c(ms2010_ms2012_nids, ms2015_ms2016_nids, poland_claims_nids), 
     cv_marketscan_data:= 1]
  dt[clinical_data_type == "inpatient", cv_inpatient:= 1]
  # fix NAs
  dt[is.na(cv_marketscan_data), cv_marketscan_data := 0]
  dt[is.na(cv_marketscan_inp_2000), cv_marketscan_inp_2000 := 0]
  dt[is.na(cv_population_surveillance), cv_population_surveillance := 0]
  dt[is.na(cv_sentinel_surveillance), cv_sentinel_surveillance := 0]
  dt[is.na(cv_inpatient), cv_inpatient := 0]
  # fix SINAN cv's
  sinan_nids <- c(228741,
                  228784,
                  228846,
                  229640,
                  229641,
                  229642,
                  229643)
  dt[nid %in% sinan_nids, cv_active_surveillance := 1]
  dt[nid %in% sinan_nids, cv_passive_surveillance := 0]
  dt[nid %in% sinan_nids, cv_sentinel_surveillance := 0]
  dt[nid %in% sinan_nids, cv_population_surveillance := 1]
  # drop sentinel surveillance sources (not used in GBD 2019)
  dt <- dt[cv_sentinel_surveillance != 1]
  # recode cv_hospital as cv_inpatient
  dt[cv_marketscan_data == 0 & cv_marketscan_inp_2000 == 0 
     & cv_population_surveillance == 0 & cv_hospital == 1, cv_inpatient := 1]
  # check that all data have only one of the used study covariates marked
  if (nrow(dt[cv_inpatient + cv_marketscan_inp_2000 + cv_marketscan_data + 
              cv_population_surveillance != 1]) > 0) {
    stop("Not all rows are uniquely defined as reference or alternative case definition")
  }
  return(dt)
}

# OUTLIER CLINICAL DATA WHERE MEAN == 0
outlier_clinical_data <- function(raw_dt) {
  #' outlier clinical data
  #' 
  #' @description outlier clinical data in bundle version where mean = 0 and 
  #'              sets is_outlier = 1 and comments in note_modeler column
  #' 
  #' @param raw_dt data.table. bundle version data table
  #' @return dt data.table. bundle version data with mean = 0 clinical data 
  #'                        outliered
  #' 
  #' @examples outlier_clinical_data(bundle_version_dt)
  #' 
  dt <- copy(raw_dt)
  dt[clinical_data_type %in% c("inpatient", "claims") & mean == 0, 
     `:=` (is_outlier = 1, 
           note_modeler = paste0(note_modeler, " | outliering since mean = 0 is implaussibly low"))]
  return(dt)
}

# REMOVE GROUP_REVIEW == 0
remove_group_review_0 <- function(adjusted_dt) {
  dt <- copy(adjusted_dt)
  dt <- dt[group_review != 0 | is.na(group_review)]
  return(dt)
}

# SAVES SEX-SPLIT XWALK VERSION
save_sex_split_version <- function(sex_split_dt, ratio) {
  sex_split_dt <- remove_group_review_0(sex_split_dt)
  wb <- createWorkbook()
  addWorksheet(wb, "extraction")
  writeData(wb, "extraction", sex_split_dt)
  saveWorkbook(wb, paste0(dem_dir, sex_model_name, ".xlsx"), overwrite = T)
  
  my_filepath <- paste0(dem_dir, sex_model_name, ".xlsx")
  my_desc <- paste0(sex_model_name, "_female_male_ratio (", ratio, ")")
  result <- save_crosswalk_version(bundle_version_id = version_id, 
                                   data_filepath = my_filepath, 
                                   description = my_desc)
  print(sprintf('Request status: %s', result$request_status))
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
}

# PULLS IN ADDITIONAL DATA FOR SEX SPLITTING
get_addiontional_sexsplit_data <- function() {
  # pull Brazil SINAN data
  dt_sinan <- fread(sinan_filepath)
  dt_sinan[, cv_active_surveillance := 1]
  dt_sinan[, cv_passive_surveillance := 0]
  dt_sinan[, cv_sentinel_surveillance := 0]
  dt_sinan[, cv_population_surveillance := 1]
  # pull in additional extracted data
  dt_other <- fread(extraction_filepath)
  dt_other[, measure := "incidence"]
  dt <- rbindlist(list(dt_sinan, dt_other), fill = T)
  return(dt)
}

# PULL IN ADDITIONAL DATA FOR XWALKING
get_addiontional_xwalk_data <- function(sex_split_dt) {
  dt <- copy(sex_split_dt)
  # pull invasive meningococcal disease population and surveilance logit ratios  
  # that have already been collapsed
  dt_imd_population <- fread(imd_pop_filepath)
  dt_imd_sentinel <- fread(imd_sentinel_filepath)
  dt <- rbindlist(list(dt_imd_population, dt_imd_sentinel, dt), fill = T)
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), 
     standard_error := (upper - lower) / 3.92]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, 
     standard_error := ((5 - mean * sample_size) / sample_size 
                        + mean * sample_size * sqrt(5 / sample_size^2)) / 5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, 
     standard_error := sqrt(mean / sample_size)]
  return(dt)
}

## GET DEFINITIONS (ALL BASED ON CV'S - WHERE ALL 0'S IS REFERENCE)
get_def <- function(ref_dt, ref_name, alt_names){
  dt <- copy(ref_dt)
  dt[, def := ""]
  for (alt in alt_names){
    dt[get(alt) == 1, def := paste0(def, "_", alt)]
  }
  dt[get(ref_name) == 1, def := "reference"]
  return(dt)
}

# GET POPULATION FOR EACH ROW IN EXTRACTION SHEET
get_row_population <- function(i, ds) {
  #' get population for each row in extraction sheet
  #' 
  #' @description pulls population using get_population shared function
  #'              from GBD 2019 decomp step ds for each row's demographics
  #'              using the closest age range in GBD land
  #' 
  #' @param i integer. row index in data table
  #' @param ds string. decomp step for get_population
  #' @return pop numeric. total population for row i's demographics
  #' 
  #' @examples get_row_population(1, 'step4')
  #' 
  # get age group id range
  age_start <- need_sample_size_dt[i, age_start]
  age_end <- need_sample_size_dt[i, age_end]
  age_start_index <- which.min(abs(age_start - age_meta$age_group_years_start))
  age_end_index <- which.min(abs(age_end - age_meta$age_group_years_end))
  my_age_group_id_start <- age_meta[age_start_index, age_group_id]
  my_age_group_id_end <- age_meta[age_end_index, age_group_id]
  # subset GBD age groups to those for this row
  gbd_age_group_id <- c(2:20, 30:32, 235)
  my_age_group_id_range <- gbd_age_group_id[gbd_age_group_id >= my_age_group_id_start
                                            & gbd_age_group_id <= my_age_group_id_end]
  # get sex id
  sex <- need_sample_size_dt[i, sex]
  if (sex == "Male") {
    my_sex_id <- 1
  } else if (sex == "Female") {
    my_sex_id <- 2
  } else if (sex == "Both") {
    my_sex_id <- 3
  } else {
    stop(paste("No sex id for sex:", sex))
  }
  # get year_id range
  year_start <- need_sample_size_dt[i, year_start]
  year_end <- need_sample_size_dt[i, year_end]
  my_year_range <- seq(year_start, year_end)
  # get location_id
  my_location_id <- need_sample_size_dt[i, location_id]
  # print arguments
  cat(paste("Pulling population for location_id:", my_location_id,
            "\nsex_id:", my_sex_id,
            "\nyear_ids:", paste(my_year_range, collapse = ", "),
            "\nage_group_ids:", paste(my_age_group_id_range, collapse = ", "),
            "\n row age_start:", age_start, "row age_end:", age_end, "\n"
  ))
  # get population
  pop_dt <- get_population(location_id = my_location_id,
                           age_group_id = my_age_group_id_range,
                           sex_id = my_sex_id,
                           year_id = my_year_range,
                           decomp_step = ds,
                           gbd_round_id = 6)
  pop <- sum(pop_dt$population)
  return(pop)
}

## Get male, female, and both sex location-age specific population for each row 
get_row <- function(n, dt, pops) {
  row <- copy(dt)[n]
  # round age start and age end to closest gbd age group values
  row_age_start <- row[, age_start]
  row_age_end <- row[, age_end]
  gbd_age_start <- unique(pops$age_group_years_start)
  gbd_age_end <- unique(pops$age_group_years_end)
  row_gbd_age_start <- gbd_age_start[which.min(abs(row_age_start - gbd_age_start))]
  row_gbd_age_end <- gbd_age_end[which.min(abs(row_age_end - gbd_age_end))]
  if (row_gbd_age_start  == row_gbd_age_end) {
    pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                            age_group_years_start == row_gbd_age_start])
  } else {
    pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                            age_group_years_start >= row_gbd_age_start & age_group_years_end <= row_gbd_age_end])
  }
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex_id")]
  row[, `:=` (male_N = agg[sex_id == 1, pop_sum], female_N = agg[sex_id == 2, pop_sum],
              both_N = agg[sex_id == 3, pop_sum])]
  return(row)
}

split_data <- function(dt, model) {
  #' sex split data
  #' 
  #' @description apply sex ratio (from an existing model object) to Both Sex data 
  #' 
  #' @param dt data.table. data table to be split
  #' @param model list. MR-BRT model stored as a list
  #' @return total_dt data.table. data table that is split
  #' @details more details
  #' 
  #' @examples split_data(bundle_version_dt, sex_split_model)
  #' 
  # Create required columns for xwalk version
  if (!"crosswalk_parent_seq" %in% colnames(dt)) dt[, "crosswalk_parent_seq":= integer()]
  if (!"group" %in% colnames(dt)) dt[, "group":= integer()]
  if (!"specificity" %in% colnames(dt)) dt[, "specificity":= character()]
  if (!"group_review" %in% colnames(dt)) dt[, "group_review":= integer()]
  # Split both sex data that are not group review == 0
  tosplit_dt <- copy(dt)
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review == 0)
                           | measure != "incidence"]
  tosplit_dt <- tosplit_dt[sex == "Both" & (group_review == 1 | is.na(group_review))
                           & measure == "incidence"]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]
  # Create draws of log(female : male) ratio incorporating between study heterogeneity
  preds <- predict_mr_brt(model, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T)
  pred_draws <- as.data.table(preds$model_draws)
  pred_draws[, c("X_intercept", "Z_intercept") := NULL]
  draws <- paste0("draw_", 0:999)
  pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
  ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
  ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
  message(paste("Splitting both sex data with ratio", ratio_mean, "(", ratio_se, ")"))
  # Get age-location specific population to split denominator
  pops <- get_population(age_group_id = c(2:20, 30:32, 235), location_id = tosplit_dt[, unique(location_id)], 
                         year_id = tosplit_dt[, unique(midyear)], sex_id = c(1,2,3), gbd_round_id = 6, decomp_step = 'step2')
  age_meta <- get_age_metadata(12, gbd_round_id = 6)
  pops <- merge(pops, age_meta[, .(age_group_id, age_group_years_start, age_group_years_end)], 
                by = "age_group_id")
  pops[age_group_years_end == 125, age_group_years_end := 99]
  tosplit_list <- mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt, pops), mc.cores = 9)
  tosplit_dt <- rbindlist(tosplit_list)
  if (nrow(tosplit_dt[is.na(both_N)]) > 0) stop("There are some rows with no custom populations pulled")
  # Create male and female rows from both sex row
  tosplit_dt[, merge := 1]
  pred_draws[, merge := 1]
  split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
  # The following equations for sex splitting are derived from:
  # Let ratio = mean_female / mean_male. Then
  # mean_male * pop_male + mean_female * pop_female = mean_both * pop_both
  # mean_male * pop_male + (ratio * mean_male) * pop_female = mean_both * pop_both
  # mean_male = mean_both * (pop_both / (pop_male + ratio * pop_female))
  # mean_female = ratio * mean_male
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_sample_size := sample_size * (male_N / both_N)]
  split_dt[, female_sample_size := sample_size * (female_N / both_N)]
  split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
  male_dt <- copy(split_dt)
  options(warn=-1)
  male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = "", lower = "", 
                  cases = "", sample_size = male_sample_size, uncertainty_type_value = "", sex = "Male",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                        ratio_se, ")"))]
  male_dt[, specificity := "sex"][, group_review:= 1][, group:= 1]
  male_dt[, crosswalk_parent_seq := seq]
  male_dt[, seq := NA]
  male_dt <- dplyr::select(male_dt, names(dt))
  female_dt <- copy(split_dt)
  female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = "", lower = "", 
                    cases = "", sample_size = female_sample_size, uncertainty_type_value = "", sex = "Female",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                          ratio_se, ")"))]
  female_dt[, specificity := "sex"][, group_review:= 1][, group:= 1]
  female_dt[, crosswalk_parent_seq := seq]
  female_dt[, seq := NA]
  female_dt <- dplyr::select(female_dt, names(dt))
  options(warn=1)
  # Check that male and female data tables have the same crosswalk parent seqs
  if (!all.equal(unique(male_dt$crosswalk_parent_seq), unique(female_dt$crosswalk_parent_seq))) {
    stop("Male and Female split data tables have different crosswalk parent seq values")
  }
  total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))
  return(total_dt)
}

# ADJUST ROW WITH CORRESPONDING MR-BRT LOGIT DIFFERENCES WITH HAQI POP SURV XWALK
adjust_row <- function(row) {
  # do not adjust group_review 0 rows
  row_group_review <- ifelse(is.na(row[, group_review]), 1, row[, group_review])
  if (row_group_review == 0) {
    return(row)
  }
  # do not adjust non-incidence rows
  if (row[, measure] != "incidence") {
    return(row)
  }
  # do not adjust reference rows
  if (row[, def] == "reference") {
    return(row)
  } else if (row[, def] == "_cv_marketscan_inp_2000") {
    
    # adjust the mean estimate: logit(mean_original) - (logit(alt) - logit(ref))
    logit_adj <- ms_2000_model_summary_dt[age_mid == row[, age_mid], logit_adj]
    row_logit_mean_adj <- logit(row[, mean]) - logit_adj
    row_mean_adj <- inv.logit(row_logit_mean_adj)
    row[, mean_adj := row_mean_adj]
    
    # adjust the standard error by
    # (1) Transform the original standard error into logit space using the delta method
    # (2) Let X be the distribution of logit(mean_original) and Y be the distribution of
    #     the logit difference (logit(alt) - logit(ref)).
    #     Assume that X and Y are  independent, so Var(X + Y) = Var(X) + Var(Y).
    # (3) Transform the adjusted standard error into linear space using the delta method
    row_se <- row[, standard_error]
    logit_adj_se <- ms_data_model_summary_dt[age_mid == row[, age_mid], logit_adj_se]
    row_logit_se <- sqrt((1 / (row[, mean] - row[, mean]^2))^2 * row_se^2)
    # row_logit_se_adj <- msm::deltamethod(~log(x1/(1-x1)), row_mean, row_se^2)
    row_logit_se_adj <- sqrt(row_logit_se^2 + logit_adj_se^2)
    row_se_adj <- sqrt((exp(row_logit_mean_adj) / (1 + exp(row_logit_mean_adj))^2)^2 
                       * row_logit_se_adj^2)
    # row_se_adj <- msm::deltamethod(~exp(x1)/(1+exp(x1)), row_logit_mean_adj, row_logit_se_adj^2)
    row[, se_adj := row_se_adj]
    
    # Add xwalk parent seq and note
    row[is.na(crosswalk_parent_seq), crosswalk_parent_seq:= seq][, seq := NA]
    row[, note_modeler := paste0(note_modeler, " | xwalk to inpatient with alternative - reference logit difference of ", round(logit_adj, 2), " (", round(logit_adj_se, 2), ")")]
    return(row)
  } else if (row[, def] == "_cv_marketscan_data") {
    logit_adj <- ms_data_model_summary_dt[age_mid == row[, age_mid], logit_adj]
    # adjust the mean estimate: logit(mean_original) - (logit(alt) - logit(ref))
    row_logit_mean_adj <- logit(row[, mean]) - logit_adj
    row_mean_adj <- inv.logit(row_logit_mean_adj)
    row[, mean_adj := row_mean_adj]
    # adjust the standard error by
    # (1) Transform the original standard error into logit space using the delta method
    # (2) Let X be the distribution of logit(mean_original) and Y be the distribution of
    #     the logit difference (logit(alt) - logit(ref)).
    #     Assume that X and Y are  independent, so Var(X + Y) = Var(X) + Var(Y).
    # (3) Transform the adjusted standard error into linear space using the delta method
    row_se <- row[, standard_error]
    logit_adj_se <- ms_data_model_summary_dt[age_mid == row[, age_mid], logit_adj_se]
    row_logit_se <- sqrt((1/(row[, mean] - row[, mean]^2))^2 * row_se^2)
    # row_logit_se_adj <- deltamethod(~log(x1/(1-x1)), row_mean, row_se^2)
    row_logit_se_adj <- sqrt(row_logit_se^2 + logit_adj_se^2)
    row_se_adj <- sqrt((exp(row_logit_mean_adj)/ (1 + exp(row_logit_mean_adj))^2)^2 * row_logit_se_adj^2)
    # row_se_adj <- deltamethod(~exp(x1)/(1+exp(x1)), row_logit_mean_adj, row_logit_se_adj^2)
    row[, se_adj := row_se_adj]
    # Add xwalk parent seq and note
    row[is.na(crosswalk_parent_seq), crosswalk_parent_seq:= seq][, seq := NA]
    row[, note_modeler := paste0(note_modeler, " | xwalk to inpatient with alternative - reference logit difference of ", round(logit_adj, 2), " (", round(logit_adj_se, 2), ")")]
    return(row)
  } else if (row[, def] == "_cv_population_surveillance") {
    logit_adj <- surv_model_summary_dt[haqi == row[, haqi], logit_adj]
    # adjust the mean estimate: logit(mean_original) - (logit(alt) - logit(ref))
    row_logit_mean_adj <- logit(row[, mean]) - logit_adj
    row_mean_adj <- inv.logit(row_logit_mean_adj)
    row[, mean_adj := row_mean_adj]
    # adjust the standard error by
    # (1) Transform the original standard error into logit space using the delta method
    # (2) Let X be the distribution of logit(mean_original) and Y be the distribution of
    #     the logit difference (logit(alt) - logit(ref)).
    #     Assume that X and Y are  independent, so Var(X + Y) = Var(X) + Var(Y).
    # (3) Transform the adjusted standard error into linear space using the delta method
    logit_adj_se <- surv_model_summary_dt[haqi == row[, haqi], logit_adj_se]
    row_se <- row[, standard_error]
    row_logit_se <- sqrt((1/(row[, mean] - row[, mean]^2))^2 * row_se^2)
    # row_logit_se_adj <- deltamethod(~log(x1/(1-x1)), row_mean, row_se^2)
    row_logit_se_adj <- sqrt(row_logit_se^2 + logit_adj_se^2)
    row_se_adj <- sqrt((exp(row_logit_mean_adj)/ (1 + exp(row_logit_mean_adj))^2)^2 * row_logit_se_adj^2)
    # row_se_adj <- deltamethod(~exp(x1)/(1+exp(x1)), row_logit_mean_adj, row_logit_se_adj^2)
    row[, se_adj := row_se_adj]
    # Add xwalk parent seq and note
    row[is.na(crosswalk_parent_seq), crosswalk_parent_seq:= seq][, seq := NA]
    row[, note_modeler := paste0(note_modeler, " | xwalk to inpatient with alternative - reference logit difference of ", round(logit_adj, 2), " (", round(logit_adj_se, 2), ")")]
    return(row)
  } else {
    return(row)
  }
}

# PLOT MEAN VS ADJUSTED MEAN AND SAVE AS PDF
plot_mean_mean_adj <- function(plot_dt) {
  plot_dt <- plot_dt[def != ""]
  plot_dt <- plot_dt[measure == "incidence"]
  plot_dt <- plot_dt[is_outlier == 0]
  plot_dt[is.na(mean_adj), mean_adj := mean]
  plot_dt[is.na(se_adj), se_adj := standard_error]
  g <- ggplot(plot_dt, aes(x = mean, y = mean_adj)) + geom_point(aes(color = def)) +
    geom_errorbar(aes(ymin = max(mean_adj - 1.96 * se_adj, 0), 
                      ymax = mean_adj + 1.96 * se_adj, color = def), width = 0.0000001) +
    geom_errorbarh(aes(xmin = max(mean - 1.96 * standard_error, 0), 
                       xmax = mean + 1.96 * standard_error, color = def), height =0.0000001) +
    geom_abline(color = "red", alpha = 0.2) + 
    theme_bw() + xlab("Mean") + ylab("Adjusted mean") + ggtitle("Meningitis incidence xwalk")
  saveRDS(g, paste0(model_plot_dir, date, "_meningitis_mean_vs_mean_adj_plot_object.RDS"))
  # remove points with large SE and adjusted SE to view better
  g2 <- ggplot(plot_dt[standard_error < 0.2 & se_adj < 0.2], 
               aes(x = mean, y = mean_adj)) + geom_point(aes(color = def)) +
    geom_errorbar(aes(ymin = max(mean_adj - 1.96 * se_adj, 0), 
                      ymax = mean_adj + 1.96 * se_adj, color = def), width = 0.0000001) +
    geom_errorbarh(aes(xmin = max(mean - 1.96 * standard_error, 0), 
                       xmax = mean + 1.96 * standard_error, color = def), height =0.0000001) +
    geom_abline(color = "red", alpha = 0.2) + 
    theme_bw() + xlab("Mean") + ylab("Adjusted mean") + ggtitle("Meningitis incidence xwalk")
  pdf(paste0(model_plot_dir, date, "_meningitis_mean_vs_mean_adj_plot_object.pdf"))
  print(g2)
  dev.off()
  print(paste0("Plot saved in", model_plot_dir, date, "_meningitis_mean_vs_mean_adj_plot_object.pdf"))
}

get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(sample_size) & !is.na(effective_sample_size), sample_size:= effective_sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[!is.na(cases) & is.na(sample_size), sample_size := cases / mean]
  return(dt)
}

## HELPER FUNCTION TO ROUND TO NEAREST AGE START/AGE END IN GBD-LAND
get_closest_age <- function(start = T, age) {
  age_start_list <- c(0, 0.01917808, 0.07671233, 1, seq(5, 95, by = 5))
  age_end_list <- c(0.01917808, 0.07671233, 1, seq(5, 95, by = 5), 125)
  if (start == T) {
    index <- which.min(abs(age_start_list - age))
    gbd_age <- age_start_list[index]
  } else if (start == F) {
    index <- which.min(abs(age_end_list - age))
    gbd_age <- age_end_list[index]
  }
  return(gbd_age)
}

# gets the age_group_id for age start/ age end in GBD land
get_gbd_age_group_id <- function(raw_dt, age_dt) {
  dt <- copy(raw_dt)
  dt <- merge(dt, age_dt[, .(age_group_id_start = age_group_id, gbd_age_start)], 
              by = c("gbd_age_start"))
  dt <- merge(dt, age_dt[, .(age_group_id_end = age_group_id, gbd_age_end)], 
              by = c("gbd_age_end"))
  return(dt)
}

# splits denominator using location-specific population distribution
split_denominator <- function(raw_dt, age_dt) {
  row_dt <- copy(raw_dt)
  # add custom age group id 0 for years 0 to 1
  my_age_dt <- rbind(age_dt, data.table(age_group_id = 0, gbd_age_start = 0, gbd_age_end = 1),
                     data.table(age_group_id = 100, gbd_age_start = 80, gbd_age_end = 125))
  age_group_ids <- c(2:20, 30:32, 235)
  row_age_group_id_start <- row_dt[, age_group_id_start]
  row_age_group_id_end <- row_dt[, age_group_id_end]
  row_age_group_ids <- age_group_ids[age_group_ids >= row_age_group_id_start
                                     & age_group_ids <= row_age_group_id_end]
  row_location_id <- row_dt[, location_id]
  row_year_start <- row_dt[, year_start]
  row_year_end <- row_dt[, year_end]
  row_year_ids <- row_year_start:row_year_end
  row_sex_id <- ifelse(row_dt[, sex] == "Male", 1, 2)
  pop <- get_population(age_group_id = row_age_group_ids, location_id = row_location_id, 
                        year_id = row_year_ids, sex_id = row_sex_id, gbd_round_id = 6, 
                        decomp_step = 'step2')
  pop[age_group_id %in% c(2,3,4), age_group_id:= 0] # sum neonatal age groups together
  pop[age_group_id %in% c(30:32, 235), age_group_id:= 100] # sum 80 - 95 + age groups together
  pop <- pop[, .(population = sum(population)), by = "age_group_id"]
  total_pop <- sum(pop$population)
  pop[, age_percent:= population / total_pop ]
  
  row_sample_size <- row_dt[, sample_size]
  pop[, age_split_sample_size:= age_percent * row_sample_size]
  row_dt$match_key <- 1
  pop$match_key <- 1
  age_split_dt <- merge(row_dt, pop[, .(age_group_id, age_split_sample_size, match_key)], 
                        by ="match_key", allow.cartesian = T)
  if (abs(sum(age_split_dt$age_split_sample_size) - row_sample_size) > 1) {
    print(sum(age_split_dt$age_split_sample_size))
    print(row_sample_size)
    stop("Age-split sample sizes do not sum up to the original sample size")
  }
  cols.remove <- c("match_key", "gbd_age_start", "gbd_age_end")
  age_split_dt[, (cols.remove):= NULL]
  age_split_dt <- merge(age_split_dt, my_age_dt, by = "age_group_id")
  return(age_split_dt)
}

# splits numerator using global age pattern seen in clinical data
split_numerator <- function(raw_dt, pattern_dt) {
  dt <- copy(raw_dt)
  age_group_ids <- c(2:20, 30:32, 235)
  row_age_group_id_start <- dt[, age_group_id_start]
  row_age_group_id_end <- dt[, age_group_id_end]
  row_age_group_ids <- age_group_ids[age_group_ids >= row_age_group_id_start
                                     & age_group_ids <= row_age_group_id_end]
  # add custom age group id 0 to split age group if age group contains all neonatal age groups
  if (all(c(2,3,4) %in% row_age_group_ids)) {
    row_age_group_ids <- c(0, row_age_group_ids)
  }
  # add custom age group id 0 to split age group if age group contains all neonatal age groups
  if (any(c(30:32, 235) %in% row_age_group_ids)) {
    row_age_group_ids <- c(100, row_age_group_ids)
  }
  pattern_dt <- pattern_dt[age_group_id %in% row_age_group_ids]
  total_cases <- sum(pattern_dt$cases)
  pattern_dt[, age_percent:= cases / total_cases]
  row_age_group_id <- dt[, age_group_id]
  age_percent <- pattern_dt[age_group_id == row_age_group_id, age_percent]
  dt[, age_split_cases:= age_percent * cases]
  return(dt)
}


# RETURNS TRUE IF CASES AND SAMPLE SIZE ARE AGE-SPLIT CORRECTLY AND FALSE OW
bool_check_age_splits <- function(dt) {
  age_split_dt <- dt[, .(total_age_split_cases = sum(age_split_cases), 
                         total_age_split_sample_size = sum(age_split_sample_size)),
                     by = c("crosswalk_parent_seq", "sex")]
  original_dt <- unique(dt[, .(crosswalk_parent_seq, sex, cases, sample_size)])
  age_split_dt <- merge(age_split_dt, original_dt, by = c("crosswalk_parent_seq", "sex"))
  check_agg <- nrow(age_split_dt[abs(total_age_split_cases - cases) > 1]) == 0 & 
    nrow(age_split_dt[abs(total_age_split_sample_size - sample_size) > 1]) == 0
  check_age_ranges <- nrow(dt[gbd_age_end - gbd_age_start > 45]) == 0
  return(check_agg & check_age_ranges)
}

remove_nondismod_locs <- function(dt) {
  loc_meta <- get_location_metadata(9, gbd_round_id = 6) # location set id 9 for DisMod locs
  dismod_locs <- unique(loc_meta$location_id)
  dt <- dt[location_id %in% dismod_locs]
  return(dt)
}



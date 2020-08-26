# Functions for prepping sex-ratios for meta-regression and then applying the result to both-sex data

## GET DEFINITIONS (ALL BASED ON CVS - WHERE ALL 0S IS REFERENCE)
sex_yearage <- function(ref_dt){
  dt<-copy(ref_dt)
  dt <- dt[ , age_mid := (age_end-age_start)/2 + age_start] 
  dt <- dt[ , year_mid := (year_end-year_start)/2 + year_start]
  return(dt)
}

get_definitions <- function(ref_dt){
  dt <- copy(ref_dt)
  cvs <- names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop]
  cvs <- cvs[!cvs %in% cv_drop] ## DROP CVs NOT NEEDED IN THE CV_DROP OBJECT
  dt[, definition := ""]
  for (cv in cvs){
    dt[get(cv) == 1, definition := paste0(definition, "_", cv)]
  }
  dt[definition == "", definition := "reference"]
  return(dt)
}

get_sex_ratios <- function(sxspec_dt) {
  
  column_list <- c("seq", "nid", "location_id", "region_id", "super_region_id", "super_region_name", "site_memo", "sex", "year_start", "year_end", "age_start", "age_end", "age_mid", "measure", "urbanicity_type", "recall_type", "recall_type_value", "mean", "lower", "upper", "standard_error", "case_name", "sample_size", "definition")
  dt <- copy(sxspec_dt)
  dt <- dt[ , ..column_list]
  dt_wide <- dcast(dt, nid + location_id + region_id + super_region_id + super_region_name + site_memo + year_start + year_end + age_start + age_end + age_mid + measure + urbanicity_type + recall_type + recall_type_value + case_name + definition ~ sex, value.var = c("seq", "mean", "lower", "upper", "standard_error", "sample_size"))
  
  dt_ratio <-  dt_wide[ , ratio:= mean_Female/mean_Male]
  dt_ratio <-  dt_ratio[ , rat_se:=sqrt((mean_Female^2/mean_Male^2)*(((standard_error_Female^2)/(mean_Female^2))+((standard_error_Male^2)/(mean_Male^2))))]
   
  return(dt_ratio)
  
}

drop_stuff <- function(ratio_dt, drop_missing = 1, drop_zerodiv = 1, drop_inf = 1) {
  
  dt <- copy(ratio_dt)
  
  if (drop_missing==1) {
    dt <- dt[!is.na(ratio) & !is.na(rat_se),]
  }
  if (drop_zerodiv==1) {
    dt <- dt[!is.nan(ratio) & !is.nan(rat_se),]
  }
  if (drop_inf==1) {
    dt <- dt[is.finite(ratio) & is.finite(rat_se),]
  }
  
  return(dt)
  
}

logtrans_ratios <- function(ratio_dt) {
  
  dt <- copy(ratio_dt)
  dt[ , lratio := log(dt$ratio)]
  
  dt$log_ratio_se <- sapply(1:nrow(dt), function(i) {
    ratio_i <- dt[i, "ratio"]
    ratio_se_i <- dt[i, "rat_se"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })
  
  return(dt)
  
}

predict_fromtrain <- function(sex_fit) {
  
  fit <- copy(sex_fit)
  
  #Predictions from sex ratio model to compare to input ratios
  predicted <- as.data.table(predict_mr_brt(fit)["model_summaries"])
  
  names(predicted) <- gsub("model_summaries.", "", names(predicted))
  names(predicted) <- gsub("X_age_mid", "age_mid", names(predicted))
  
  predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
  predicted[, `:=` (ratio = exp(Y_mean), ratio_se = (exp(Y_mean_hi) - exp(Y_mean_lo))/(2*qnorm(0.975,0,1)))]
  
  crosswalk_reporting <- write.xlsx(predicted, paste0(out_path, "/predicted_sexratios_train", date, ".xlsx"), col.names=TRUE)
  
  predicted[, (c("X_intercept", "Z_intercept", "Y_negp", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe")) := NULL]
 
  return(predicted)
   
}

predict_new <- function(sex_fit = NULL, old_model_summary = NULL, bothsx_dt, by = "age_mid") {
  
  dt <- bothsx_dt[ , age_mid := (age_end-age_start)/2 + age_start] 
  dt <- dt[ , year_mid := (year_end-year_start)/2 + year_start]
  
  if (is.null(sex_fit)) {
    predicted <- as.data.table(read.csv(old_model_summary))
  } else {
    fit <- copy(sex_fit)
    predicted <- as.data.table(predict_mr_brt(fit, newdata=dt)["model_summaries"])
  }
  
  names(predicted) <- gsub("model_summaries.", "", names(predicted))
  names(predicted) <- gsub("X_age_mid", "age_mid", names(predicted))
  
  predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
  predicted[, `:=` (ratio = exp(Y_mean), ratio_se = (exp(Y_mean_hi) - exp(Y_mean_lo))/(2*qnorm(0.975,0,1)))]
  
  predicted[, (c("X_intercept", "Z_intercept", "Y_negp", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe")) := NULL]
  
  if (by == "none") {
    
    return(predicted)
    
  } else {
  
  predicted <- unique(predicted, by = "age_mid")
  return(predicted)
  
  }
  
}
  
get_sex_ppln <- function(bothsex_dt, decomp_step, age_dt = age_dt) {
  
  dt <- copy(bothsex_dt)
  source(paste0(central_fxn, "get_population.R"))
  
  ## Get sex-specific GBD population estimates for all GBD age-groups that we are using and both sexes, specific to the year-location combinations seen in our real both-sex data
  population <- get_population(location_id = unique(dt$location_id), decomp_step = decomp_step, age_group_id = age_dt$age_group_id, sex_id = c(1, 2), year_id = seq(min(dt$year_start), max(dt$year_end)))
  #Add age limits to population estimates
  population <- merge(population, age_dt, by = "age_group_id")
  
  ## Assign age limits to real both-sex data that correspond to cut-points in GBD age-groups
  dt[, `:=` (age_start_r = round(age_start/5)*5, age_end_r = round(age_end/5)*5)]
  dt[age_start_r == age_end_r & age_mid < age_start_r, age_start_r := age_start_r - 5]
  dt[age_start_r == age_end_r & age_mid >= age_start_r, age_end_r := age_end_r + 5]
  dt[, age_end_r := age_end_r - 1]
  
  ## Aggregate GBD population estimates for the year-age-location combinations in real both-sex data
  #Aggregation of a given observation for a given sex
  pop_agg <- function(l, a_s, a_e, y_s, y_e, s){
    ages_toagg <- age_dt[age_start %in% c(a_s:a_e-4) & age_end %in% c(a_s+4:a_e), age_group_id]
    aggregated_ppln <- population[location_id == l & age_group_id %in% ages_toagg & sex_id == s & year_id %in% c(y_s:y_e),sum(population)]
    return(aggregated_ppln)
  }
  #Apply aggregation to each row for each sex
  dt[, pop_m := pop_agg(location_id, age_start_r, age_end_r, year_start, year_end, s = 1), by = "seq"]
  dt[, pop_f := pop_agg(location_id, age_start_r, age_end_r, year_start, year_end, s = 2), by = "seq"]
  #Sum for the both-sex denom ppln specific to that observation's demographics
  dt[, pop_b := pop_m + pop_f]

  return(dt)
  
}

transform_bothsexdt <- function(bothsex_dt, sex_ratios, by = "age_mid") {
  
  dt <- copy(bothsex_dt)
  
  if (by == "none") {
    
    dt <- cbind(dt, sex_ratios)
    
  } else {
  
    dt <- merge(dt, sex_ratios, by = by)
    
  }
  
  #Calculate mean and SE for females
  
  dt_female <- copy(dt) 
  dt_female[, `:=` (sex = "Female", mean_n = mean * (pop_b), mean_d =(pop_f + (1/ratio) * pop_m),
                              var_n = (standard_error^2 * pop_b^2), var_d = ratio_se^2 * pop_m^2)]
  dt_female[, `:=` (mean = mean_n / mean_d, standard_error = sqrt(((mean_n^2) / (mean_d^2)) * (var_n / (mean_n^2) + var_d / (mean_d^2))))]
  dt_female[, `:=` (input_type = "split", sex_issue = 1, specificity = paste0(dt_female$specificity, ", sex"), note_modeler = paste0(dt_female$note_modeler, "|this row created from both-sex data by applying sex-ratio from MR-BRT metaregression"))]
  
  #Calculate mean SE for males
  dt_male <- copy(dt)
  dt_male[, `:=` (sex = "Male", mean_n = mean * (pop_b), mean_d =(pop_m + ratio * pop_f),
                            var_n = (standard_error^2 * pop_b^2), var_d = ratio_se^2 * pop_f^2)]
  dt_male[, `:=` (mean = mean_n / mean_d, standard_error = sqrt(((mean_n^2) / (mean_d^2)) * (var_n / (mean_n^2) + var_d / (mean_d^2))))]
  dt_male[, `:=` (input_type = "split", sex_issue = 1, specificity = paste0(dt_male$specificity, ", sex"), note_modeler = paste0(dt_male$note_modeler, "|this row created from both-sex data by applying sex-ratio from MR-BRT metaregression"))]
  
  #Bind new male rows and new female rows together, remove columns that aren't in total data set, update group-related columns as needed, and return as transformed both-sex data to bind to original sex-specific data for subsequent processing steps
  dt_sexsplit <- rbind(dt_male, dt_female)
  col_remove <- c("Y_mean", "Y_se", "age_mid", "year_mid", "age_start_r", "age_end_r", "pop_m", "pop_f", "pop_b", "mean_n", "mean_d", "var_n", "var_d", "ratio", "ratio_se")
  dt_sexsplit[, (col_remove) := NULL]
  dt_sexsplit[specificity == ", sex", `:=` (group = 9999, group_review =1)]

  return(dt_sexsplit)
  
}

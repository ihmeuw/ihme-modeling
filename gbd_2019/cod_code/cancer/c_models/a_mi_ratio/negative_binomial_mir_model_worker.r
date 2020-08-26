##################################################################################
# Name of Script: negative_binomial_mir_model_worker.R                           #
# Description: Runs negative binomial regression on mirs for new causes          #
# Arguments:                                                                     #
# Output: compiled csv                                                           #
##################################################################################

####################################
## Set up workspace and libraries  #
####################################
rm(list = ls())

# set working directories by operating system
user <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "/home/j" 
  h_root <- file.path("/homes", user)
} else if (Sys.info()["sysname"]=="Darwin") {
  j_root <- "~/J"
  h_root <- "~/H"
} else { 
  j_root <- "J:"
  h_root <- "H:"
}

library(ggplot2); library(data.table);
library(here); library(RODBC)
library(RMySQL); library(RSQLite)
library(assertthat); library(MASS)
library(tidyr); library(parallel)

source(<FILEPATH>)
source(<FILEPATH>) 
source(<FILEPATH>) 
source(<FILEPATH>) 
source(<FILEPATH>) 

####################################
# Helper Functions                 #
####################################

check_data_columns <- function(data){
  # unit testing for columns of input data for the expected
  # values that should be present in final estimated MIRs for said cause
  #
  # Args:
  #   data: input data with columns acause, age_group_id, year_id, location_id
  #
  # Returns:
  #   NONE
  acause <- data$cause_name %>% unique
  haqi_quintile <- get_covariate_estimates(covariate_id = 1099, age_group_id = "all", location_id = "all", 
                                           year_id = "all", sex_id = "all", decomp_step = "step4")

  for(col in c("age_group_id", "year_id", "location_id")){
    if(col == "age_group_id"){
      if(cause %in% c("neo_bone", "neo_eye_other", "neo_lymphoma_burkitt", "neo_lymphoma_other")){
        assert_that(identical(unique(data$age_group_id) %>% sort, c(1, 6:20, 30:32, 235) %>% sort), msg = paste0("Missing ages in cause ", acause))
      } else if(cause %in% c("neo_eye", "neo_tissue_sarcoma", "neo_neuro")){
        assert_that(identical(unique(data$age_group_id) %>% sort, c(1, 6:20, 30:32, 235) %>% sort), msg = paste0("Missing ages in cause ", acause))
      } else if(cause == "neo_eye_rb"){
        assert_that(identical(unique(data$age_group_id) %>% sort, c(1, 6:8) %>% sort), msg = paste0("Missing ages in cause ", acause))
      }else if(cause == "neo_liver_hbl"){
        assert_that(identical(unique(data$age_group_id) %>% sort, c(1, 6:7) %>% sort), msg = paste0("Missing ages in cause ", acause))
      }
    } else if(col == "year_id"){
      assert_that(identical(unique(data$year_id) %>% sort, c(1980:2019) %>% sort), msg = paste0("Missing years in cause ", acause))
    } else if(col == "location_id"){
      assert_that(identical(unique(data$location_id) %>% sort, unique(haqi_quintile$location_id) %>% sort), msg = paste0("Missing locations in cause ", unique(data$cause_name)))
    }
  }
  assert(sum(data[is.na(mi_ratio)]) == 0, msg = "some NA mi_ratios")
}

check_final_file <- function(final_df){
  # unit testing for final compiled file for
  # values that should be present in final estimated MIRs for all causes
  #
  # Args:
  #   data: input data with columns acause, sex_id
  #
  # Returns:
  #   NONE
  causes <- c("neo_bone", "neo_eye_other", "neo_lymphoma_burkitt", "neo_lymphoma_other", 
              "neo_eye", "neo_tissue_sarcoma", "neo_neuro", "neo_eye_rb", "neo_liver_hbl")
  
  for(col in c("acause", "sex_id")){
    if(col == "acause") assert_that(identical(unique(final_df$acause) %>% sort, causes %>% sort), msg = paste0("Missing causes in final"))
    if(col == "sex_id") assert_that(identical(unique(final_df$sex_id %>% sort), c(1, 2, 3) %>% sort), msg = paste0("Missing sexes in cause"))
  }
}

grab_caps <- function(mir_version){
  # grabs all CR and VR data and returns the caps associated with it, 95% and 5% caps 
  # lower caps are calculated by acause and age_group_id 
  # upper caps are calculated by age_group_id
  #
  # Args:
  #   mir_version: version of MIRs to generate caps from
  #
  # Returns:
  #   dataframe of upper and lower caps by age and cause
  all_causes <- list.files(<FILEPATH>, full.names = T)
  all_causes <- all_causes[grepl("neo", all_causes)]
  caps <- lapply(all_causes, function(x){
    fread(x)
  })
  caps <- rbindlist(caps, fill = T)
  caps[, mi_ratio := deaths/incident_cases]
  caps[, lower_cap := quantile(mi_ratio, probs = 0.05), by = c("acause", "age_group_id")]
  caps[, upper_cap := quantile(mi_ratio, probs = 0.95), by = c("age_group_id")]
  
  all_caps <- copy(caps)
  caps <- unique(caps[, list(age_group_id, acause, lower_cap, upper_cap)])
  return(caps)
}

####################################
# Main NB Functions                #
####################################

get_negative_binomial_coef <- function(cause_name = "neo_bone", transform_mir = T, 
                                       mir_version = 60, 
                                       apply_caps = T, use_haqi_estimates = T,
                                       exclude_locations = F, locs_to_remove = c(),
                                       use_year_as_var = F,
                                       exclude_age_group = F, ages_to_remove = c(), 
                                       combine_data_types = F,
                                       apply_all_cause_caps = F, apply_cause_specific_caps = F, apply_old_caps = T,
                                       use_dummy_age_model = F, keep_all_caps = T, use_continous_age = F, 
                                       all_causes = new_causes,
                                       aggregate_old_age = T, old_age_split = 1, 
                                       generate_specific_age_caps = T,
                                       generate_final_age_aggregate_model = T){
  
  # This function preps the data for NB regression and runs the nb model on it, you have options to exclude data, 
  # as well as apply different caps for vetting purposes
  # general model: log(deaths) ~ haq + age_vars + offset(log(incident_cases))
  #
  # Args:
  #   cause_name
  #   transform_mir 
  #   mir_version
  #   apply_caps
  #   use_haqi_estimates
  #   exclude_locations
  #   locs_to_remove
  #   use_year_as_var
  #   exclude_age_group
  #   ages_to_remove
  #   combine_data_types
  #   apply_all_cause_caps
  #   apply_cause_specific_caps
  #   apply_old_caps
  #   use_dummy_age_model
  #   keep_all_caps
  #   use_continous_age
  #   all_causes
  #   aggregate_old_age
  #   old_age_split
  #   generate_specific_age_caps
  #   generate_final_age_aggregate_model
  #
  # Returns:
  #   model: NB model results
  #   results: dataframe of breakdown of coeffients and statistics (p-value for ex)
  #   age_coeffs: dataframe of model coeffients and p values
  #
  
  # prep for aggregation
  if(use_haqi_estimates == T){
    haqi_quintile <- get_covariate_estimates(covariate_id = 1099, age_group_id = "all", location_id = "all", 
                                             year_id = "all", sex_id = "all", decomp_step = "step4")
    haqi_quintile$age_group_id <- NULL
    setnames(haqi_quintile, old = "mean_value", new = "haq")
  } else{
    haqi_quintile <- mir.get_HAQValues(gbd_round_id = 6)
    setDT(haqi_quintile)
  }
  
  # this specifies if we want to add CRCR CRVR combined or not
  if(combine_data_types){
    data <- lapply(cause_name, function(x){
      fread(paste0(<FILEPATH>, 
                   mir_version, "/", x, ".csv"))})
    data <- rbindlist(data, fill = T)
    cause_name <- cause_name[1]
  } else{
    data <- fread(paste0(<FILEPATH>, 
                         mir_version, "/", cause_name, ".csv"))
  }
  
  # for unit testing
  input_data <- copy(data)
  message(paste0("Data for ", cause_name, " read in!"))
  
  # getting raw_mirs
  data[, mi_ratio := deaths/incident_cases]
  
  # aggregating older age groups for these causes
  if(aggregate_old_age){
    data[, age_group_id := ifelse(age_group_id > old_age_split, 2, 1)]
    data[, c("upper_cap", "lower_cap", "mi_ratio") := NULL]
    cols <- c("age_group_id", "acause", "location_id", "year_id", "sex_id")
    data <- data[, lapply(.SD, sum), .SDcols = c("deaths", "incident_cases"), by = cols]
    data[, mi_ratio := deaths/incident_cases]
  }
  
  # our final age model for new causes: GBD2019
  if(generate_final_age_aggregate_model & cause_name != "neo_lymphoma_burkitt"){
    # neo eye rb is 0-4 as reference to 5-19 (upper age restriction)
    if(cause_name == "neo_eye_rb"){
      data[, age_group_id := ifelse(age_group_id > 1, 2, 1)]
    }
    # neo liver hbl is 0-4 ref to 5-14 
    else if(cause_name == "neo_liver_hbl"){
      data[, age_group_id := ifelse(age_group_id > 1, 2, 1)]
    }
    # they are all 0-19, 20-39, 40-59, 60-79, 80+
    else if(cause_name %in% c("neo_bone", "neo_neuro", "neo_eye", "neo_eye_other", "neo_lymphoma_other", "neo_tissue_sarcoma")){
      ages <- get_age_metadata(age_group_set_id = 12)
      data <- merge(data, ages, by = c("age_group_id"))
      data[, age_group_id := ifelse(age_group_years_start >= 80, 5, 
                                    ifelse((age_group_years_start >= 60 & age_group_years_start < 80), 4, 
                                           ifelse((age_group_years_start >= 40 & age_group_years_start < 60), 3, 
                                                  ifelse((age_group_years_start >= 20 & age_group_years_start < 40), 2, 1))))]
      data[, c("age_group_years_start", "age_group_years_end", "age_group_weight_value") := NULL]
    }
    # they are 0-19 to 20+ (2 age categories)
    else if(cause_name == "neo_lymphoma_burkitt"){
      data[, age_group_id := ifelse(age_group_id > 8, 2, 1)]
    }
    data[, c("upper_cap", "lower_cap", "mi_ratio") := NULL]
    cols <- c("age_group_id", "acause", "location_id", "year_id", "sex_id")
    data <- data[, lapply(.SD, sum), .SDcols = c("deaths", "incident_cases"), by = cols]
    data[, mi_ratio := deaths/incident_cases]
  }
  
  # get all cause age upper caps instead of using new cause specific ones
  if(apply_all_cause_caps){
    upper_caps <- cdb.get_table(table = "mir_upper_cap")
    setDT(upper_caps)
    upper_caps$acause <- NULL
    
    lower_caps <- cdb.get_table(table = "mir_lower_cap")
    setDT(lower_caps)
    
    upper_caps <- upper_caps[mir_model_version_id == 74]
    lower_caps <- lower_caps[mir_model_version_id == 74]
    all_caps <- merge(lower_caps, upper_caps, by = c("age_group_id"))
    
    # neo eye rb is 0-4 as reference to 5-19 (upper age restriction)
    if(cause_name == "neo_eye_rb"){
      data[age_group_id == 1, upper_cap := upper_caps[age_group_id == 1, upper_cap]]
      data[age_group_id == 2, upper_cap := upper_caps[age_group_id == 8, upper_cap]]
    }
    # neo liver hbl is 0-4 ref to 5-14 
    if(cause_name == "neo_liver_hbl"){
      data[age_group_id == 1, upper_cap := upper_caps[age_group_id == 1, upper_cap]]
      data[age_group_id == 2, upper_cap := upper_caps[age_group_id == 7, upper_cap]]
    }
    # they are all 0-19, 20-39, 40-59, 60-79, 80+
    if(cause_name %in% c("neo_bone", "neo_neuro", "neo_eye", "neo_eye_other", 
                         "neo_lymphoma_other", "neo_tissue_sarcoma")){
      data[age_group_id == 1, upper_cap := upper_caps[age_group_id == 8, upper_cap]]
      data[age_group_id == 2, upper_cap := upper_caps[age_group_id == 12, upper_cap]]
      data[age_group_id == 3, upper_cap := upper_caps[age_group_id == 16, upper_cap]]
      data[age_group_id == 4, upper_cap := upper_caps[age_group_id == 20, upper_cap]]
      data[age_group_id == 5, upper_cap := upper_caps[age_group_id == 235, upper_cap]]
    }
    
    # they are 0-19 to 20+ (2 age categories)
    if(cause_name == "neo_lymphoma_burkitt"){
      data[age_group_id == 1, upper_cap := upper_caps[age_group_id == 1, upper_cap]]
      data[age_group_id == 2, upper_cap := upper_caps[age_group_id == 235, upper_cap]]
    }
    data$lower_cap <- 0
  } 
  
  if (apply_cause_specific_caps){
    # generate cause and age specific upper and lower caps
    upper_caps <- data[, lapply(.SD, quantile, probs = 0.95), .SDcols = c("mi_ratio"), by = list(age_group_id)]
    setnames(upper_caps, new = "upper_cap", old = "mi_ratio")
    
    lower_caps <- data[, lapply(.SD, quantile, probs = 0.05), .SDcols = c("mi_ratio"), by = list(age_group_id)]
    setnames(lower_caps, new = "lower_cap", old = "mi_ratio")
    
    data[, c("lower_cap", "upper_cap") := NULL]
    caps <- merge(upper_caps, lower_caps)
    data <- merge(data, caps, by = c("age_group_id"))
  }
  
  if(apply_old_caps){
    # generate old caps for CR-CR CR-VR combined
    caps <- lapply(all_causes, function(x){
      fread(paste0(<FILEPATH>, 
                   mir_version, "/", x, ".csv"))})
    caps <- rbindlist(caps, fill = T)
    
    caps[, mi_ratio := deaths/incident_cases]
    upper_caps <- caps[, lapply(.SD, quantile, probs = 0.95), .SDcols = c("mi_ratio"), by = list(age_group_id)]
    upper_caps$cause_name <- cause_name
    setnames(upper_caps, new = "upper_cap", old = "mi_ratio")
    
    lower_caps <- data[, lapply(.SD, quantile, probs = 0.05), .SDcols = c("mi_ratio")]
    lower_caps$cause_name <- cause_name
    setnames(lower_caps, new = "lower_cap", old = "mi_ratio")
    
    all_caps <- merge(upper_caps, lower_caps, by = "cause_name")
    data[, c('upper_cap', 'lower_cap') := NULL]
    data <- merge(data, all_caps, by = c("age_group_id"))
  }
  
  if(keep_all_caps){
    # these allows us to compare the various different caps
    all_caps <- cdb.get_table(table = "mir_upper_cap")
    setDT(all_caps)
    all_caps <- all_caps[mir_model_version_id == 61]
    setnames(all_caps, old = "upper_cap", new = "all_cause_cap")
    
    upper_caps <- data[, lapply(.SD, quantile, probs = 0.95), .SDcols = c("mi_ratio"), by = list(age_group_id)]
    setnames(upper_caps, new = "cause_age_cap", old = "mi_ratio")
    
    data <- merge(data, all_caps[, list(age_group_id, all_cause_cap)], by = c("age_group_id"))
    data <- merge(data, upper_caps, by = c("age_group_id"))
  }
  
  # caps for all causes in specific age groups
  if(generate_specific_age_caps){
    all_mirs <- get_all_mir_data()
    all_mirs[, mi_ratio := deaths/incident_cases]
    child_caps <- all_mirs[age_group_id <= 8]
    child_caps <- child_caps[, lapply(.SD, quantile, probs = 0.95), .SDcols = c("mi_ratio")]
    child_caps$cause_name <- cause_name
    setnames(child_caps, new = c("childhood_cap"), old = c("mi_ratio"))
    
    young_ages <- data[age_group_id <= 8]
    young_ages <- merge(young_ages, child_caps, by = c("cause_name"))
    
    # preserving the old caps (cause specific for all causes)
    old_ages <- data[age_group_id > 8]
    old_ages[, childhood_cap := copy(all_cause_cap)]
    
    data <- rbind(old_ages, young_ages)
  }
  
  # applying caps to mi ratios
  if(apply_caps){
    data[, mi_ratio := ifelse(mi_ratio > upper_cap, upper_cap, mi_ratio)]
    data[, mi_ratio := ifelse(mi_ratio < lower_cap, lower_cap, mi_ratio)]
  }
  
  # exclusions to apply
  if(exclude_locations){
    assert_that(length(locs_to_remove) != 0, msg = "Need to add locations to locs_to_remove argument")
    data <- data[!(location_id %in% locs_to_remove)]
  }
  if(exclude_age_group){
    assert_that(length(ages_to_remove) != 0, msg = "Need to add ages to ages_to_remove argument")
    data <- data[!(age_group_id %in% ages_to_remove)]
  }
  
  # to use negative binomial model, need integers on left hand side
  if(transform_mir == T){
    data[, deaths := (deaths) %>% round(digits = 0)]
  }
  
  if((!aggregate_old_age & !generate_final_age_aggregate_model)| cause_name == "neo_lymphoma_burkitt"){
    # change age ids to be incremental
    new_ages <- data.table(age_group_id = c(1, 6:20, 30:32, 235), new_age_id = c(1:20))
    data <- merge(data, new_ages, by = "age_group_id", all.x = T)
    setnames(data, new = c("age_group_id", "old_age_id"), old = c("new_age_id", "age_group_id"))
    if(cause_name == "neo_lymphoma_burkitt") data$old_age_id <- NULL
  }
  
  #assert_that(data[deaths < 1] %>% nrow == 0, msg = "deaths less than 1, need to be left unrounded!")
  assert_that(data$sex_id %>% unique == 3, msg = "sexes not aggregated!")
  
  # setting up our formula based on togged values
  # using model based on age categorization
  if(use_dummy_age_model & cause_name != "neo_lymphoma_burkitt"){
    formula <- "deaths ~ "
    age_vars <- paste0("age_id_", data$age_group_id %>% unique %>% sort)
    print(age_vars)
    
    # adding all the age categorical variables into our data with exception of reference age group
    lapply(age_vars, function(x){
      if(x == "age_id_1") data[, (x) := 0]
      else data[, (x) := ifelse(age_group_id == strsplit(x, split = "_")[[1]][3] %>% as.numeric, 1, 0)]
    })
    
    formula <- paste0(formula, paste0(age_vars, collapse = " + "))
  } else if (use_continous_age | cause_name == "neo_lymphoma_burkitt"){
    formula <- "deaths ~ age_group_id"
  } else{
    formula <- "deaths ~ "
  }
  
  if(use_year_as_var)  formula <- paste(formula, "year_id", sep = "+")
  
  # deciding if we wanted to use the haq quintiles or covariate estimates
  if(use_haqi_estimates == T){
    data <- merge(data, haqi_quintile[, list(location_id, year_id, haq)], by = c("location_id", "year_id"))
    formula <- paste(formula, "haq", sep = "+")
  } else{
    data <- merge(data, haqi_quintile[, list(location_id, year_id, haq, haq_quintile)])
    formula <- paste(formula, "haq_quintile", sep = "+")
  }
  
  message("Haqi estimates merged!")
  formula <- paste(formula, "offset(log(incident_cases))", sep = "+")
  our_formula <- as.formula(formula)
  
  print(our_formula)
  
  model <- glm.nb(formula = our_formula, data = data, link = "log")
  message("Model ran!")
  
  if(use_haqi_estimates) haqi_var <- "haq"
  else haqi_var <- "haq_quintile"
  
  # make predictions 
  # setting up coef multiplication
  coeffs <- coef(model)
  if(use_year_as_var){
    data[, pred_mir := exp((year_id*(coeffs['year_id'])) + (age_group_id*(coeffs['age_group_id'])) + (haq*(coeffs['haq'])) + (coeffs["(Intercept)"]))]
  } else if(use_dummy_age_model & cause_name != "neo_lymphoma_burkitt"){
    data[, sum_coeffs := 0]
    # getting age sums
    for(age_var in age_vars[-1]){
      data[, sum_coeffs := sum_coeffs + (get(age_var) * coeffs[age_var])]
    }
    data[, pred_mir := exp(sum_coeffs + (haq*(coeffs['haq'])) + (coeffs["(Intercept)"]))]
  } else{
    data[, pred_mir := exp((age_group_id*(coeffs['age_group_id'])) + (haq*(coeffs['haq'])) + (coeffs["(Intercept)"]))]
  }
  
  print(sprintf("HAQI Coefficient: %s", model$coefficients[haqi_var]))
  print(sprintf('Cause: %s', cause_name))
  
  # get location_names 
  locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 6)
  data <- merge(data, locs[, list(location_id, location_name)], by = c("location_id"))
  if(!aggregate_old_age) assert_that(nrow(data) == nrow(input_data), msg = "Dropping/collapsing rows somewhere in code")
  
  sum <- summary(model)$coefficients
  coefficients <- sum[rownames(sum), c(1, 4)]
  coefficients$cause_name <- cause_name
  
  # specifying specifics for the year_id variable
  if(use_year_as_var){
    if(data$year_id %>% unique %>% length == 1) year_p_value <- 0
    else year_p_value <- sum['year_id', 4]
  } else{
    year_p_value <- NA
  }
  
  # for table formatting 
  if(grepl("VR", cause_name)){
    data_type <- "CR-VR"
  } else if(combine_data_types){
    data_type <- "Combined: CR-CR & CR-VR"
  } else{
    data_type <- "CR-CR"
  }
  
  # dummy age model is model with age as categorical
  if(!use_dummy_age_model | cause_name == "neo_lymphoma_burkitt"){
    age_coefficient <- model$coefficients['age_group_id'] %>% round(3)
    age_p_value <- summary(model)$coefficients['age_group_id', 4]
  }
  
  results = data.table(#haq_variable = haqi_var,
    cause = cause_name, 
    haq_coefficient = model$coefficients[haqi_var] %>% round(3), 
    haq_p_value = summary(model)$coefficients['haq', 4] ,
    raw_mir_range = paste(data$mi_ratio %>% range %>% round(2), collapse = " to "),
    pred_mir_range = paste(data$pred_mir %>% range %>% round(2), collapse = " to "),
    two_log_likelihood = model$twologlik,
    AIC = model$aic,
    deviance = model$deviance,
    null_deviance = model$null.deviance,
    theta = model$theta,
    SE_theta = model$SE.theta,
    intercept = model$coefficients['(Intercept)'])
  
  if(use_dummy_age_model & cause_name != "neo_lymphoma_burkitt"){
    for(age_var in age_vars[-1]){
      results[, (age_var) :=  model$coefficients[age_var] %>% round(3)]
      results[, (paste0(age_var, "_p_val")) :=  summary(model)$coefficients[age_var, 4]]
    }
  } else{
    results$age_coefficient <- age_coefficient
    results$age_p_value <- age_p_value
  }
  
  return(list(results = results, age_coeffs = summary(model)$coefficients,
              model = model))
}


get_nb_config <- function(acause_name = NULL, use_dummy_age_model = T, 
                          use_continous_age = F,
                          generate_final_age_aggregate_model = T){
  
  # Calculates the ages specific to cause, as well as locations, haq and years
  # required for getting MIR estimates to compute coeffients on
  # 
  # Args:
  #   acause_name: version of MIRs to generate caps from
  #   use_dummy_age_model: T for using categorical age, F otherwise
  #   use_continous_age: T for using continious age, F otherwise
  #   generate_final_age_aggregate_model = T
  #
  # Returns:
  #   dataframe with all locations, years, haq, and ages that 
  #   we need to estimate MIRs for
  
  # setting up locations
  locs <- get_location_metadata(location_set_id = 22)
  
  # getting years
  haqi_quintile <- get_covariate_estimates(covariate_id = 1099, age_group_id = "all", location_id = "all", 
                                           year_id = "all", sex_id = "all", decomp_step = "step4")
  haqi_quintile$age_group_id <- NULL
  setnames(haqi_quintile, old = "mean_value", new = "haq")
  haqi_quintile <- haqi_quintile[location_id %in% locs$location_id %>% unique]
  config <- haqi_quintile[, list(location_id, year_id, sex_id, haq, lower_value, upper_value)]
  
  # create for multiple ages depending on cause age restrictions
  ages <- cdb.get_table(table = "registry_input_entity")
  setDT(ages)
  cause_id <- 1012
  ages <- ages[gbd_round_id == 6 & acause == acause_name & is_active == 1]
  age_start <- ages$yll_age_start
  age_end <- ages$yll_age_end
  
  if(age_end >= 95) age_end <- 125
  if(age_start == 1) age_start <- 0
  
  age_ids <- get_age_metadata(age_group_set_id = 12)
  age_ids <- age_ids[age_group_id > 4]
  age_ids[age_group_id == 5, age_group_id := 1]
  age_ids[age_group_id == 1, age_group_years_start := 0]
  age_ids[age_group_id == 1, age_group_years_end := 5]
  
  age_group_ids <- age_ids[age_group_years_start >= age_start & age_group_years_start <= age_end]$age_group_id
  new_ages <- data.table(age_group_id = c(1, 6:20, 30:32, 235), new_age_id = c(1:20))
  age_group_ids <- new_ages[age_group_id %in% age_group_ids]
  setnames(age_group_ids, new = c("age_group_id", "old_age_id"), old = c("new_age_id", "age_group_id"))
  age_group_ids <- age_group_ids$age_group_id
  
  # compile all estimated age group dataframes
  i <- 1
  age_data <- list()
  for(age_group_id in age_group_ids){
    age_data[[i]] <- copy(config)
    age_data[[i]] <- age_data[[i]][, age_group_id := age_group_id]
    i <- i + 1
  }
  total_data <- rbindlist(age_data)
  
  if(generate_final_age_aggregate_model){
    if(!(acause_name %in% c("neo_eye_rb", "neo_liver_hbl", "neo_lymphoma_burkitt"))){
      # adding all the age categorical variables into our data with exception of reference age group
      total_data[, age_id_1 := 0]
      total_data[, age_id_2 := ifelse(age_group_id > 4 & age_group_id <= 8, 1, 0)]
      total_data[, age_id_3 := ifelse(age_group_id > 8 & age_group_id <= 12, 1, 0)]
      total_data[, age_id_4 := ifelse(age_group_id > 12 & age_group_id <= 16, 1, 0)]
      total_data[, age_id_5 := ifelse(age_group_id > 16, 1, 0)]

    } else if(acause_name == "neo_eye_rb"){
      # neo_eye_rb 0-4, 5-19
      total_data[, age_id_1 := 0]
      total_data[, age_id_2 := ifelse(age_group_id > 1 & age_group_id <= 4, 1, 0)]
      
    } else if(acause_name == "neo_liver_hbl"){
      # neo_liver_hbl 0-4, 5-14
      total_data[, age_id_1 := 0]
      total_data[, age_id_2 := ifelse(age_group_id > 1 & age_group_id <= 3, 1, 0)]
    }
  }
  
  # dividing it into locations
  total_config <- list()
  i <- 1
  for(loc_id in total_data$location_id %>% unique){
    loc_subset <- total_data[location_id == loc_id]
    total_config[[i]] <- loc_subset
    i <- i + 1
  }
  
  return(total_config)
}


get_coef_matrix <- function(acause_name = NULL, model = NULL){
  # Takes the coeffients and resulting variance covariance 
  # matrix to produce 1000 coeffients for each variable in the model
  # distributed normally
  # 
  # Args:
  #   acause_name: cause for model
  #   model: nb model for the cause
  #
  # Returns:
  #   dataframe of 1000 rows by n variables of normalized coeffients
  coefs <- coef(model)
  coefs <- coefs[!(names(coefs) %in% c("age_id_1"))]
  vcov_matrix <- vcov(model)
  
  # sigma is our vcov
  draw_coeffs <- mvrnorm(n = 1000, coefs, Sigma = vcov_matrix, tol = 1e-6)
  return(draw_coeffs)
}


generate_mir_draws <- function(config_rows= NULL, cause_name = NULL, draw_coeffs = NULL, 
                               use_dummy_age_model = T, use_continous_age = F,
                               generate_final_age_aggregate_model = T){
  # Takes the 1000 coeffients and dataframe of inputs for one location to equation and 
  # calculates the MIRs for 1000 draws for that location
  # 
  # Args:
  #   config_rows: dataframe of inputs for model equation from get_nb_config
  #   cause_name: cause for model
  #   draw_coeffs: matrix of 1000 rows x n variables from get_coef_matrix
  #   use_dummy_age_model
  #   use_continous_age
  #   generate_final_age_aggregate_model
  #
  # Returns:
  #   dataframe of 1000 draws with corresponding uid cols:  location_id', 'year_id', 
  #   'sex_id', 'haq', "age_group_id" for that location
  start <- Sys.time()
  location_id <- config_rows$location_id %>% unique
  print(paste0("starting draws for location", location_id))
  
  # compute the list mean for each list element
  total_draws <- list()
  uid_cols <- c('location_id', 'year_id', 'sex_id', 'haq', 
                "lower_value", "upper_value", "age_group_id")
  
  
  # convert rows into age_id columns 
  # get our MIRs for each location
  computeMIR <- function(config_row, input_coeffs, cause_name){
    if(use_continous_age | cause_name == "neo_lymphoma_burkitt"){
      results <- apply(input_coeffs, 1, function(x) exp((x['(Intercept)'] + (x['age_group_id'] * config_row['age_group_id']) + (x['haq'] * config_row['haq']))))
    } else if(use_dummy_age_model & generate_final_age_aggregate_model & cause_name != "neo_lymphoma_burkitt"){
      age_vars <- colnames(input_coeffs)[grepl("age_id",  colnames(input_coeffs))]
      
      if(!(cause_name %in% c("neo_eye_rb", "neo_liver_hbl", "neo_lymphoma_burkitt"))){
        results <- apply(input_coeffs, 1, function(x){
          exp((x['(Intercept)'] + (x['age_id_2'] * config_row['age_id_2']) + (x['age_id_3'] * config_row['age_id_3']) + 
                 (x['age_id_4'] * config_row['age_id_4']) + (x['age_id_5'] * config_row['age_id_5']) +
                 (x['haq'] * config_row['haq'])))
        })
        
      } else if(cause_name %in% c("neo_eye_rb", "neo_liver_hbl", "neo_lymphoma_burkitt")){
        results <- apply(input_coeffs, 1, function(x){
          exp((x['(Intercept)'] + (x['age_id_2'] * config_row['age_id_2']) + (x['haq'] * config_row['haq'])))
        })
      }
    } else{
      message("Please specify what model you want to use")
    }
    names(results) <- paste0("draw_", c(0:999))
    results <- as.list(results)
    setDT(results)
    results[, (uid_cols) := as.list(config_row[uid_cols])]
    results[, cause_name := cause_name]
    results
  }
  
  total_draws <- apply(config_rows, 1, computeMIR, input_coeffs = draw_coeffs, cause_name = cause_name)
  total_draws <- rbindlist(total_draws)
  print(paste0("done with draws for location", location_id))
  # getting summary statistics
  draw_cols <- paste0("draw_", c(0:999))
  total_draws[, mean := rowMeans(.SD), .SDcols = draw_cols]
  total_draws[, upper := apply(.SD, 1, quantile, probs = 0.975), .SDcols = draw_cols]
  total_draws[, lower := apply(.SD, 1, quantile, probs = 0.025), .SDcols = draw_cols]
  total_draws[, (draw_cols) := NULL]
  print(paste0("done with summaries for location", location_id))
  
  # change age ids back
  #new_ages <- data.table(old_age_id = c(2:20, 30:32, 235), age_group_id = c(0.003834, 0.01150685,  0.1846575, 0.8, 2:20))
  new_ages <- data.table(old_age_id = c(1, 6:20, 30:32, 235), age_group_id = c(1:20))
  total_draws <- merge(total_draws, new_ages, by = 'age_group_id')
  setnames(total_draws, new = c("age_group_id", "new_age_id"), old = c("old_age_id", "age_group_id"))
  total_draws$new_age_id <- NULL
  
  end <- Sys.time()
  print(end - start)
  total_draws
}


parallel_draw_launch <- function(cause_name, mir_version = 74){
  # main function that launches the 1000 draw calculation of MIRs in parallel
  # (18 threads) by cause and produces the summaries for the MIRs,
  # applies caps to them and saves them
  # 
  # Args:
  #   mir_version: best version of mirs
  #   cause_name: cause for model
  #
  # Returns:
  #   None
  
  # launches the draws by location into respective folders
  mir_version <- mir_version
  data_results <- get_negative_binomial_coef(cause_name = cause_name, transform_mir = F,
                                    mir_version = mir_version, apply_caps = F, use_haqi_estimates = T, 
                                    use_year_as_var = F, 
                                    exclude_locations = F, locs_to_remove = c(),
                                    exclude_age_group = F, ages_to_remove = c(), 
                                    combine_data_types = T, apply_all_cause_caps = F, apply_cause_specific_caps = F,
                                    apply_old_caps = F, use_dummy_age_model = T, 
                                    keep_all_caps = F, use_continous_age = F, all_causes = all_causes,
                                    aggregate_old_age = F, old_age_split = 8,
                                    generate_specific_age_caps = F, generate_final_age_aggregate_model = T)
  model <- data_results[['model']]
  cause_name <- cause_name[1]
  total_config <- get_nb_config(acause_name = cause_name)
  draw_coeffs <- get_coef_matrix(acause_name = cause_name, model = model)
  
  
  all_results <- mclapply(total_config, FUN = function(x){ 
                                                generate_mir_draws(config_rows = x, draw_coeffs = draw_coeffs, cause_name = cause_name)
                                                }, mc.cores = 18) 
  # running serially for interactive testing
  #all_results <- lapply(total_config, generate_mir_draws, draw_coeffs = draw_coeffs, cause_name = cause_name)
  all_results <- rbindlist(all_results)
  
  # copying in females and male results
  females <- copy(all_results)
  females$sex_id <- 2
  males <- copy(all_results)
  males$sex_id <- 1
  all_results <- rbindlist(list(all_results, females, males))
  
  # final cleaning, renaming columns
  setnames(all_results, new = c("acause"), old = c("cause_name"))
  setnames(all_results, new = c("sex", "year"), old = c("sex_id", "year_id"))
  all_results$run_id <- 111
  all_results <- merge(caps[, list(acause, age_group_id, lower_cap, upper_cap)], all_results, by = c("age_group_id", "acause"), all.y = T)
  
  # applying caps
  all_results[mean > upper_cap, mean := upper_cap]
  all_results[mean < lower_cap, mean := lower_cap]
  all_results[, c("lower_cap", "upper_cap", "lower_value", "upper_value", "haq", "lower", "upper") := NULL]
  setnames(all_results, new = c("mi_ratio"), old = c("mean"))
  fwrite(all_results, paste0(<FILEPATH>, cause_name, "/", cause_name, "_mir_final.csv"))
  
  # apply caps
  all_results[mean > upper_cap, mean := upper_cap]
  all_results[mean < lower_cap, mean := lower_cap]
  all_results[, c("lower_cap", "upper_cap", "lower_value", "upper_value", "haq", "lower", "upper") := NULL]
  setnames(all_results, new = c("mi_ratio"), old = c("mean"))
  fwrite(all_results, paste0(<FILEPATH>, cause_name, "/", cause_name, "_mir_final_v3.csv"))

  # add in upper and lower caps
  upper_caps <- cdb.get_table(table = "mir_upper_cap")
  setDT(upper_caps)
  upper_caps <- upper_caps[mir_model_version_id == 74]
  upper_caps$acause <- NULL
  all_caps <- merge(caps, upper_caps, by = c("age_group_id"))[, list(age_group_id, acause, lower_cap, upper_cap)]
  
  caps <- grab_caps(74)
  for(age in c(2:5)){
    new_cap <- caps[age_group_id == 1]
    new_cap[, age_group_id := age]
    caps <- rbind(caps, new_cap)
  }

  new_cap <- caps[age_group_id == 16 & acause == "neo_lymphoma_burkitt"]
  new_cap[, age_group_id := 15]
  new_cap[, upper_cap := 1.004624]
  caps <- rbind(caps, new_cap)

  all_results <- merge(all_results, all_caps)
  fwrite(all_results, paste0(<FILEPATH>, cause_name, "/", cause_name, "_mir_summaries_final_v3", type, ".csv"))
  end <- Sys.time()
  end - start

}

# option for calling this worker script by itself
if (!interactive()) {
  cause_name1 <- commandArgs()[6]
  cause_name2 <- commandArgs()[7]
  mir_version <- commandArgs()[8]
  parallel_draw_launch(list(cause_name1, cause_name2), mir_version)
}else{
  print("Must run in cancer_env and non-interactively with R ")
}
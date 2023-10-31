##################################################################################
# Name of Script: updated_negative_binomial_mir_model_worker.R                  
# Description: Runs negative binomial regression on mirs for new causes          
# Arguments:                          
#     cause_name: see list of new causes in launcher script
#     cause_name2: see list of new causes VR in launcher script
#     mir_version: see mir_model_version table mir_version_ids
#     draw_count: [0, 100, 1000]
# Output: compiled csv summaries for new causes + draws if selected, vetting pdf of tables, plots
# Contributors: USERNAME
##################################################################################

####################################
## Set up workspace and libraries  #
####################################
rm(list = ls())

# Set working directories by operating system
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

library(ggplot2)
library(data.table)
library(RODBC)
library(RMySQL)
library(boot)

library(here)
library(blob)
library(RSQLite)

library(assertthat); library(MASS)
library(tidyr); library(parallel)
library(rhdf5)


source(file.path('FILEPATH/utilities.r'))
source(get_path(key = "cdb_utils_r", process = "common"))
source("FILEPATH") 
source(file.path('FILEPATH/Plotting_nb_models.R'))

cur_gbd_round <- get_gbd_parameter("current_gbd_round")
cur_decomp_step <- get_gbd_parameter("current_decomp_step")

####################################
# Helper Functions                 #
####################################

grab_caps <- function(mir_version, include_VR = F){
  # grabs all CR and VR data and returns the caps associated with it, 95% and 5% caps 
  # lower caps are calculated by acause and age_group_id 
  # upper caps are calculated and saved as all cause and cause specific
  # upper caps for age groups under 10 years of age are forced to be at least 1 in all cases
  #
  # Args:
  #   mir_version: version of MIRs to generate caps from
  #   include_VR: determines whether to include the VR data in the generation of caps
  #
  # Returns:
  #   dataframe of upper and lower caps by age and cause for all causes
  
  all_causes <- list.files(paste0(get_path(key = "mir_inputs", process = "mir_model"), "/model_version_", mir_version), full.names = T)
  
  # Allow for the exclusion of VR if specified
  if (include_VR) all_causes <- all_causes[grepl("neo", all_causes)]
  else all_causes <- all_causes[grepl("neo", all_causes)&!grepl("_VR", all_causes)]
  
  caps <- lapply(all_causes, function(x){
    fread(x)
  })
  caps <- rbindlist(caps, fill = T)
  caps[age_group_id==5, age_group_id:=1]
  
  caps[, mi_ratio := deaths/incident_cases]
  caps[, lower_cap := quantile(mi_ratio, probs = 0.05), by = c("acause", "age_group_id")]
  
  #Calculate separate all cause and cause specific caps
  caps[, all_cause_upper_cap := quantile(mi_ratio, probs = 0.95), by = c("age_group_id")]
  caps[, cause_specific_upper_cap := quantile(mi_ratio, probs = 0.95), by = c("acause","age_group_id")]
  
  #If the cause specific or all cause cap is less than 1 for a given age group, make it 1
  caps[all_cause_upper_cap<1 & age_group_id<9, all_cause_upper_cap := 1]
  caps[cause_specific_upper_cap<1 & age_group_id<9, cause_specific_upper_cap := 1]
  
  all_caps <- copy(caps)
  
  caps <- unique(caps[, list(age_group_id, acause, lower_cap, all_cause_upper_cap, cause_specific_upper_cap)])
  return(caps)
}

####################################
# Main NB Functions                #
####################################

get_negative_binomial_coef <- function(cause_name, transform_mir = T, 
                                       mir_version = 89, 
                                       use_haqi_estimates = T,
                                       use_year_as_var = F,
                                       st_gpr_input = T, combine_data_types = F, 
                                       use_continuous_age = F, 
                                       new_data = "",
                                       regular_categorical_age = F){
  
  # This function preps the data for NB regression and runs the nb model on it, you have options to exclude data, 
  # as well as apply different caps for vetting purposes
  # general model: log(deaths) ~ haq + age_vars + offset(log(incident_cases))
  #
  # Args:
  #   cause_name: options see launcher script
  #   transform_mir: [T, F] rounds input deaths 
  #   mir_version: see mir_model_version table mir_version_ids
  #   use_haqi_estimates: [T, F] applies haq covariate estimates, if F, uses haq in groups by percentile as predictor
  #   use_year_as_var: [T, F] if T, uses year_id as a predictor in model
  #   st_gpr_input: [T, F] if T, uses mir_version specific st-gpr inputs
  #   combine_data_types: [T, F] if T, uses GBD2019 model 
  #   use_continuous_age: [T, F] if T, uses age_group_id as predictor
  #   new_data: string with path to a new dataset to use, default = ""
  #   regular_categorical_age: [T, F] if T uses age as a categorical variable in 5 year gbd age bins
  #
  # Returns:
  #   model: NB model results
  #   results: dataframe of breakdown of coeffients and statistics (p-value for ex)
  #   age_coeffs: dataframe of model coeffients and p values
  #
  
  # prep for aggregation by adding in haqi estimates
  haqi_quintile <- get_covariate_estimates(covariate_id = 1099, age_group_id = "all", location_id = "all", 
                                           year_id = "all", sex_id = "all", decomp_step = cur_decomp_step, 
                                           gbd_round_id = cur_gbd_round)
  haqi_quintile$age_group_id <- NULL
  
  #different inputs if using values or quintiles
  if(use_haqi_estimates == T){
    setnames(haqi_quintile, old = "mean_value", new = "haq")
    message("Loaded haqi as a continuous variable.")
  } else{
    #Calculate the percentiles used in the model
    group0.05 = quantile(haqi_quintile$mean_value, probs = .05)
    group0.1 = quantile(haqi_quintile$mean_value, probs = .1)
    group0.15 = quantile(haqi_quintile$mean_value, probs = .15)
    group0.2 = quantile(haqi_quintile$mean_value, probs = .2)
    group0.4 = quantile(haqi_quintile$mean_value, probs = .4)
    group0.6 = quantile(haqi_quintile$mean_value, probs = .6)
    group0.8 = quantile(haqi_quintile$mean_value, probs = .8)
    group1 = quantile(haqi_quintile$mean_value, probs = 1)
    
    haqi_quintile[mean_value<=group0.05,haq:=1]
    haqi_quintile[mean_value<=group0.1&mean_value>group0.05,haq:= 1]
    haqi_quintile[mean_value<=group0.15&mean_value>group0.1,haq:= 2]
    haqi_quintile[mean_value<=group0.2&mean_value>group0.15,haq:= 2]
    haqi_quintile[mean_value<=group0.4&mean_value>group0.2,haq:= 3]
    haqi_quintile[mean_value<=group0.6&mean_value>group0.4,haq:= 4]
    haqi_quintile[mean_value<=group0.8&mean_value>group0.6,haq:= 5]
    haqi_quintile[mean_value<=group1&mean_value>group0.8,haq:= 6]
    
    message(paste0("Loaded haqi as ", length(haqi_quintile$haq)," distinct percentile groupings!"))
    setDT(haqi_quintile)
  }
  
  # READING IN INPUT DATA:
  # this specifies the input data and if we want to add CRCR CRVR combined or not
  if(combine_data_types & st_gpr_input){
    data <- lapply(cause_name, function(x){
      path = paste0(get_path(key = "mir_inputs", process = "mir_model"), "/model_version_",mir_version,"/",  
                    x, ".csv")
      
      print(path)
      input = fread(path)
      if(grepl("VR",x)) input$source = "VR" else input$source = "CR"
      return(input)
    })
    data <- rbindlist(data, fill = T)
    
    data[age_group_id==5, age_group_id:=1]
    
    data[ , uid := paste0(year_id, "_", location_id, "_", age_group_id)]
    n_occur_table <- data.table(table(data$uid))
    names(n_occur_table) <- c("uid", "n_occur")
    data <- merge(data, n_occur_table, by = "uid")
    data[ , to_drop := ifelse(n_occur > 1 & source == "VR", 1, 0)]
    message("Dropping ", nrow(data[to_drop==1]), " VR matched duplicates from data.")
    # Subset to non-dups
    data <- data[to_drop == 0,]
    
    cause_name <- cause_name[1]
    
  } else if (st_gpr_input){
    cause_name <- cause_name[1]
    
    message("Only reading in CR.")
    
    path = paste0(get_path(key = "mir_inputs", process = "mir_model"), "/model_version_",mir_version,"/",  
                  cause_name, ".csv")
    data <- fread(path)
    print(path)
    
    data[age_group_id==5, age_group_id:=1]
    
  } else {
    path <- new_data
    
    print(path)
    data <- fread(path)
    
    #if the file has cases instead of incident_cases add a variable
    #data$incident_cases = data$cases
    
    #cleans the data
    data[,V1:=NULL]
    
    #subsets the data to correct cause
    data = data[acause == cause_name[1]]
    
    cause_name = cause_name[1]
  }
 
  message(paste0("Data for ", cause_name, " read in!"))
  
  if (cause_name%in% c("neo_lymphoma_burkitt","neo_lymphoma_other","neo_eye","neo_eye_rb","neo_eye_other")){
    data = data[!location_id==102]
    message("Excluding new SEER data!")
  }
  else {
    data = data[!location_id%in%c(524, 527, 529, 533, 534, 538, 540, 541, 545, 553, 554, 567, 570)]
    message("Including new SEER data and dropping subnationals!")
  }
  
  # getting raw_mirs
  data[, mi_ratio := deaths/incident_cases]
  
  # OPTIONAL AGGREGATION OF AGE VARIABLES
  if(!regular_categorical_age){
      if (cause_name=="neo_neuro"){
        # 0-9, 10-19, 15-39, 40-69, 70+
        data[, age_group_id := ifelse(age_group_id<7, 1, age_group_id)]
        data[, age_group_id := ifelse(age_group_id>=7&age_group_id<9, 2, age_group_id)]
        data[, age_group_id := ifelse(age_group_id>=9&age_group_id<13, 3, age_group_id)]
        data[, age_group_id := ifelse(age_group_id>=13&age_group_id<19, 4, age_group_id)]
        data[, age_group_id := ifelse(age_group_id>=19, 5, age_group_id)]
      } else if(cause_name%in% c("neo_lymphoma_burkitt")){
        data[, age_group_id := ifelse(age_group_id<8, 1, age_group_id)]
        data[, age_group_id := ifelse(age_group_id>=8&age_group_id<13, 2, age_group_id)]
        data[, age_group_id := ifelse(age_group_id>=13&age_group_id<19, 3, age_group_id)]
        data[, age_group_id := ifelse(age_group_id>=19, 4, age_group_id)]
      }else if(cause_name == "neo_eye_rb"){
        data[, age_group_id := ifelse(age_group_id > 5, 2, 1)]
      } else if(cause_name == "neo_liver_hbl"){
        data[, age_group_id := ifelse(age_group_id > 5, 2, 1)]
      } else if(cause_name%in%c("neo_eye", "neo_eye_other")) {
        # 0-4, 5-9, 10-29, 30-49, 50-69, 70-89, 90+
        data[, age_group_id := ifelse(age_group_id<7, 1, age_group_id)]
        data[, age_group_id := ifelse(age_group_id>=7&age_group_id<9, 2, age_group_id)]
        data[, age_group_id := ifelse(age_group_id>=9&age_group_id<13, 3, age_group_id)]
        data[, age_group_id := ifelse(age_group_id>=13&age_group_id<17, 4, age_group_id)]
        data[, age_group_id := ifelse(age_group_id>=17&age_group_id<30, 5, age_group_id)]
        data[, age_group_id := ifelse(age_group_id>=30, 6, age_group_id)]
      } else if(cause_name %in% c("neo_lymphoma_burkitt", "neo_bone", "neo_eye", "neo_eye_other", "neo_lymphoma_other", "neo_tissue_sarcoma")){
        # bins: 0-19, 20-39, 40-59, 60-79, 80+
        ages <- get_age_metadata(age_group_set_id = 12)
        data <- merge(data, ages, by = c("age_group_id"))
        data[, age_group_id := ifelse(age_group_years_start >= 80, 5, 
                                ifelse((age_group_years_start >= 60 & age_group_years_start < 80), 4, 
                                ifelse((age_group_years_start >= 40 & age_group_years_start < 60), 3, 
                                ifelse((age_group_years_start >= 20 & age_group_years_start < 40), 2, 1))))]
        data[, c("age_group_years_start", "age_group_years_end", "age_group_weight_value") := NULL]
      }
      data[, c("upper_cap", "lower_cap") := NULL]
      cols <- c("age_group_id", "acause", "location_id", "year_id", "sex_id")
      data <- data[, lapply(.SD, sum), .SDcols = c("deaths", "incident_cases"), by = cols]
  }
  
  # to use negative binomial model, may need integers on left hand side
  if(transform_mir == T){
    data[, deaths := (deaths) %>% round(digits = 0)]
  }
  
  if(use_continuous_age){
    # change age ids to be incremental
    new_ages <- data.table(age_group_id = c(1, 6:20, 30:32, 235), new_age_id = c(1:20))
    data <- merge(data, new_ages, by = "age_group_id", all.x = T)
    setnames(data, new = c("age_group_id", "old_age_id"), old = c("new_age_id", "age_group_id"))
  }
  
  # CREATING MODEL FORMULA
  # setting up our formula based on togged values
  # using model based on age categorization
  if(!use_continuous_age){
    formula <- "deaths ~ "
    age_vars <- paste0("age_id_", data$age_group_id %>% unique %>% sort)
    print(age_vars)
    
    # adding all the age categorical variables into our data with exception of reference age group
    lapply(age_vars, function(x){
      if(x == "age_id_1") data[, (x) := 0]
      
      else data[, (x) := ifelse(age_group_id == strsplit(x, split = "_")[[1]][3] %>% as.numeric, 1, 0)]
    })
    
    formula <- paste0(formula, paste0(age_vars, collapse = " + "))
  } else if (use_continuous_age){
    formula <- "deaths ~ age_group_id"
  } 
  
  if(use_year_as_var)  formula <- paste(formula, "year_id", sep = "+")
  
  # including haqi in the formula
  data <- merge(data, haqi_quintile[, list(location_id, year_id, haq)], by = c("location_id", "year_id"))
  formula <- paste(formula, "haq", sep = "+")
  head(data)
  
  message("Haqi estimates merged!")
  formula <- paste(formula, "offset(log(incident_cases))", sep = "+")
  our_formula <- as.formula(formula)
  print(our_formula)
 
  model <- glm.nb(formula = our_formula, data = data, link = "log")
  message("Model ran!")
  print(summary(model))
  
  #Calculate the 10-fold validation
  print("Estiamted 10-fold Validation:")
  print(cv.glm(data = data, model, K=10)$delta)
  
  haqi_var <- "haq"
  print(sprintf("HAQI Coefficient: %s", model$coefficients[haqi_var]))
  print(sprintf('Cause: %s', cause_name))
  
  # get location_names 
  locs <- get_location_metadata(location_set_id = 35, decomp_step = cur_decomp_step, gbd_round_id = cur_gbd_round)
  data <- merge(data, locs[, list(location_id, location_name)], by = c("location_id"))
  
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

  
  if(use_continuous_age){
    age_coefficient <- model$coefficients['age_group_id'] %>% round(3)
    age_p_value <- summary(model)$coefficients['age_group_id', 4]
  }
  
  results = data.table(#haq_variable = haqi_var,
    cause = cause_name, 
    haq_coefficient = model$coefficients[haqi_var] %>% round(3), 
    haq_p_value = summary(model)$coefficients['haq', 4] ,
    raw_mir_range = paste(data$mi_ratio %>% range %>% round(2), collapse = " to ")
    #pred_mir_range = paste(data$pred_mir %>% range %>% round(2), collapse = " to "),
    # two_log_likelihood = model$twologlik,
    # AIC = model$aic,
    # deviance = model$deviance,
    # null_deviance = model$null.deviance,
    # theta = model$theta,
    # SE_theta = model$SE.theta,
    # intercept = model$coefficients['(Intercept)']
    )
  
  if(!use_continuous_age){
    for(age_var in age_vars[-1]){
      results[, (age_var) :=  model$coefficients[age_var] %>% round(3)]
    }
  } else{
    results$age_coefficient <- age_coefficient
    #results$age_p_value <- age_p_value
  }
  
  return(list(results = results, age_coeffs = summary(model)$coefficients,
              model = model))
}


get_nb_config <- function(acause_name = NULL,
                          use_continuous_age = F,
                          regular_categorical_age = F, use_haqi_estimates = T){
  
  # Calculates the ages specific to cause, as well as locations, haq and years
  # required for getting MIR estimates to compute coeffients on
  # 
  # Args:
  #   acause_name: version of MIRs to generate caps from
  #   use_continuous_age: T for using continious age, F otherwise
  #
  # Returns:
  #   dataframe with all locations, years, haq, and ages that 
  #   we need to estimate MIRs for
  
  # setting up locations
  locs <- get_location_metadata(location_set_id = 22, decomp_step = cur_decomp_step, 
                                gbd_round_id = cur_gbd_round)
  
  # getting years
  haqi_quintile <- get_covariate_estimates(covariate_id = 1099, age_group_id = "all", location_id = "all", 
                                           year_id = "all", sex_id = "all", decomp_step = cur_decomp_step, 
                                           gbd_round_id = cur_gbd_round)
  haqi_quintile$age_group_id <- NULL
  if(use_haqi_estimates){
    setnames(haqi_quintile, old = "mean_value", new = "haq")
  } else {
    #Calculate the percentiles used in the model
    group0.05 = quantile(haqi_quintile$mean_value, probs = .05)
    group0.1 = quantile(haqi_quintile$mean_value, probs = .1)
    group0.15 = quantile(haqi_quintile$mean_value, probs = .15)
    group0.2 = quantile(haqi_quintile$mean_value, probs = .2)
    group0.4 = quantile(haqi_quintile$mean_value, probs = .4)
    group0.6 = quantile(haqi_quintile$mean_value, probs = .6)
    group0.8 = quantile(haqi_quintile$mean_value, probs = .8)
    group1 = quantile(haqi_quintile$mean_value, probs = 1)
    
    haqi_quintile[mean_value<=group0.05,haq:=1]
    haqi_quintile[mean_value<=group0.1&mean_value>group0.05,haq:= 1]
    haqi_quintile[mean_value<=group0.15&mean_value>group0.1,haq:= 1]
    haqi_quintile[mean_value<=group0.2&mean_value>group0.15,haq:= 1]
    haqi_quintile[mean_value<=group0.4&mean_value>group0.2,haq:= 2]
    haqi_quintile[mean_value<=group0.6&mean_value>group0.4,haq:= 3]
    haqi_quintile[mean_value<=group0.8&mean_value>group0.6,haq:= 4]
    haqi_quintile[mean_value<=group1&mean_value>group0.8,haq:= 5]
    
  }
  haqi_quintile <- haqi_quintile[location_id %in% (locs$location_id %>% unique)]
  config <- haqi_quintile[, list(location_id, year_id, sex_id, haq, lower_value, upper_value)]
  
  # create for multiple ages depending on cause age restrictions
  ages <- cdb.get_table(table = "registry_input_entity")
  setDT(ages)
  ages <- ages[gbd_round_id == cur_gbd_round & acause == acause_name & is_active == 1 & decomp_step == 3]
  age_start <- ages$yll_age_start[2]
  age_end <- ages$yll_age_end
  print(age_end)
  
  if(age_end >= 95) age_end <- 125
  if(age_start == 1) age_start <- 0
  
  age_ids <- get_age_metadata(age_group_set_id = 12)
  age_ids <- age_ids[age_group_id > 4]
  age_ids[age_group_id == 5, age_group_id := 1]
  age_ids[age_group_id == 1, age_group_years_start := 0]
  age_ids[age_group_id == 1, age_group_years_end := 5]
  
  
  age_group_ids <- age_ids[age_group_years_start >= age_start & age_group_years_start <= age_end]$age_group_id
  
  if(!regular_categorical_age){
    new_ages <- data.table(age_group_id = c(1, 6:20, 30:32, 235), new_age_id = c(1:20))
    age_group_ids <- new_ages[age_group_id %in% age_group_ids]
    setnames(age_group_ids, new = c("age_group_id", "old_age_id"), old = c("new_age_id", "age_group_id"))
    age_group_ids <- age_group_ids$age_group_id
  } 
  
  # compile all estimated age group dataframes
  i <- 1
  age_data <- list()
  for(age_group_id in age_group_ids){
    age_data[[i]] <- copy(config)
    age_data[[i]] <- age_data[[i]][, age_group_id := age_group_id]
    i <- i + 1
  }

  total_data <- rbindlist(age_data)

  if(!regular_categorical_age&!use_continuous_age){
    if (acause_name=="neo_neuro") {
      total_data[, age_id_1 := ifelse(age_group_id<3, 1, 0)]
      total_data[, age_id_2 := ifelse(age_group_id>=3&age_group_id<5, 1, 0)]
      total_data[, age_id_3 := ifelse(age_group_id>=5&age_group_id<9, 1, 0)]
      total_data[, age_id_4 := ifelse(age_group_id>=9&age_group_id<15, 1, 0)]
      total_data[, age_id_5 := ifelse(age_group_id>=15, 1, 0)]
    } else if (acause_name%in%c("neo_lymphoma_burkitt")) {
      total_data[, age_id_1 := ifelse(age_group_id<4, 1, 0)]
      total_data[, age_id_2 := ifelse(age_group_id>=4&age_group_id<9, 1, 0)]
      total_data[, age_id_3 := ifelse(age_group_id>=9&age_group_id<15, 1, 0)]
      total_data[, age_id_4 := ifelse(age_group_id>=15, 1, 0)]
    } else if (acause_name %in% c("neo_eye","neo_eye_other")){
      total_data[, age_id_1 := ifelse(age_group_id<3, 1, 0)]
      total_data[, age_id_2 := ifelse(age_group_id>=3&age_group_id<5, 1, 0)]
      total_data[, age_id_3 := ifelse(age_group_id>=5&age_group_id<9, 1, 0)]
      total_data[, age_id_4 := ifelse(age_group_id>=9&age_group_id<13, 1, 0)]
      total_data[, age_id_5 := ifelse(age_group_id>=13&age_group_id<17, 1, 0)]
      total_data[, age_id_6 := ifelse(age_group_id>=17, 1, 0)]
    } else if(!(acause_name %in% c("neo_eye_rb", "neo_liver_hbl"))){
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
  } else if (regular_categorical_age) {
    total_data[, age_id_1 := ifelse(age_group_id==1, 1, 0)]
    total_data[, age_id_6 := ifelse(age_group_id==6, 1, 0)]
    total_data[, age_id_7 := ifelse(age_group_id==7, 1, 0)]
    total_data[, age_id_8 := ifelse(age_group_id==8, 1, 0)]
    total_data[, age_id_9 := ifelse(age_group_id==9, 1, 0)]
    total_data[, age_id_10 := ifelse(age_group_id==10, 1, 0)]
    total_data[, age_id_11 := ifelse(age_group_id==11, 1, 0)]
    total_data[, age_id_12 := ifelse(age_group_id==12, 1, 0)]
    total_data[, age_id_13 := ifelse(age_group_id==13, 1, 0)]
    total_data[, age_id_14 := ifelse(age_group_id==14, 1, 0)]
    total_data[, age_id_15 := ifelse(age_group_id==15, 1, 0)]
    total_data[, age_id_16 := ifelse(age_group_id==16, 1, 0)]
    total_data[, age_id_17 := ifelse(age_group_id==17, 1, 0)]
    total_data[, age_id_18 := ifelse(age_group_id==18, 1, 0)]
    total_data[, age_id_19 := ifelse(age_group_id==19, 1, 0)]
    total_data[, age_id_20 := ifelse(age_group_id==20, 1, 0)]
    total_data[, age_id_30 := ifelse(age_group_id==30, 1, 0)]
    total_data[, age_id_31 := ifelse(age_group_id==31, 1, 0)]
    total_data[, age_id_32 := ifelse(age_group_id==32, 1, 0)]
    total_data[, age_id_235 := ifelse(age_group_id==235, 1, 0)]
  }
  
  # helper function for dividing into locations
  add_loc_data <- function(loc_id, total_data){
    loc_subset <- total_data[location_id == loc_id]
    return(loc_subset)
  }
  
 total_config <- mclapply(total_data$location %>% unique, FUN = function(x){
    add_loc_data(loc_id = x, total_data = total_data)
  }, mc.cores = detectCores() - 2)
  
  print("Retrieved location configs")
  return(total_config)
}


get_coef_matrix <- function(acause_name = NULL, model = NULL, draw_count = 0, regular_categorical_age = F){
  # Takes the coeffients and resulting variance covariance 
  # matrix to produce 1000 coeffients for each variable in the model
  # distributed normally
  # 
  # Args:
  #   acause_name: cause for model
  #   model: nb model for the cause
  #   draw_count: number of coeffs for each var to generate, possible values: 0, 100, 1000
  #
  # Returns:
  #   dataframe of 1000 rows by n variables of normalized coeffients
  coefs <- coef(model)
  
  if(acause_name=="neo_eye_other") {
    if (!regular_categorical_age) coefs <- coefs[!(names(coefs) %in% c("age_id_1","age_id_6"))]
    if (regular_categorical_age) coefs <- coefs[!(names(coefs) %in% c("age_id_235"))]
  }else{
    coefs <- coefs[!(names(coefs) %in% c("age_id_1"))]
  }

  vcov_matrix <- vcov(model)
  
  if(draw_count == 0) draw_count <- 1000
  
  # sigma is our vcov
  draw_coeffs <- mvrnorm(n = draw_count, coefs, Sigma = vcov_matrix, tol = 1e-6)
  return(draw_coeffs)
}


generate_mir_draws <- function(config_rows= NULL, cause_name = NULL, draw_coeffs = NULL, 
                               use_continuous_age = F,
                               draw_count = 0, regular_categorical_age = F, run_description=""){
  # Takes the 1000 coeffients and dataframe of inputs for one location to equation and 
  # calculates the MIRs for 1000 draws for that location
  # 
  # Args:
  #   config_rows: dataframe of inputs for model equation from get_nb_config
  #   cause_name: cause for model
  #   draw_coeffs: matrix of 1000 rows x n variables from get_coef_matrix
  #   use_continuous_age: if T, use continuous age as predictor
  #   draw_count: number of draw columns to generate, possible values: [0, 100, 1000]
  #
  # Returns:
  #   dataframe of 1000 draws with corresponding uid cols:  location_id', 'year_id', 
  #   'sex_id', 'haq', "age_group_id" for that location
  
  location_id <- config_rows$location_id %>% unique
  print(paste0("starting draws for location: ", location_id))
  
  # compute the list mean for each list element
  total_draws <- list()
  uid_cols <- c('location_id', 'year_id', 'sex_id', 'haq', 
                "lower_value", "upper_value", "age_group_id")
  
  
  # convert rows into age_id columns 
  # get our MIRs for each location
  computeMIR <- function(config_row, input_coeffs, cause_name){
    if(use_continuous_age){
      results <- apply(input_coeffs, 1, function(x) exp((x['(Intercept)'] + (x['age_group_id'] * config_row['age_group_id']) + (x['haq'] * config_row['haq']))))
    } else if (regular_categorical_age & !(cause_name %in% c("neo_liver_hbl","neo_eye_rb","neo_eye_other"))){
      age_vars <- colnames(input_coeffs)[grepl("age_id",  colnames(input_coeffs))]
      results <- apply(input_coeffs, 1, function(x){
        exp((x['(Intercept)'] +  (x['age_id_6'] * config_row['age_id_6']) + 
               (x['age_id_7'] * config_row['age_id_7']) + (x['age_id_8'] * config_row['age_id_8']) +(x['age_id_9'] * config_row['age_id_9']) +
               (x['age_id_10'] * config_row['age_id_10']) + (x['age_id_11'] * config_row['age_id_11']) + (x['age_id_12'] * config_row['age_id_12'])+
               (x['age_id_13'] * config_row['age_id_13']) + (x['age_id_14'] * config_row['age_id_14']) + (x['age_id_15'] * config_row['age_id_15'])+
               (x['age_id_16'] * config_row['age_id_16']) + (x['age_id_17'] * config_row['age_id_17'])+ (x['age_id_18'] * config_row['age_id_18']) +
               (x['age_id_19'] * config_row['age_id_19']) + (x['age_id_20'] * config_row['age_id_20']) +(x['age_id_30'] * config_row['age_id_30'])+
               (x['age_id_31'] * config_row['age_id_31']) + (x['age_id_32'] * config_row['age_id_32'])+(x['age_id_235'] * config_row['age_id_235'])+
               (x['haq'] * config_row['haq'])))
      })
      
    }else if(regular_categorical_age&cause_name=="neo_eye"){
      results <- apply(input_coeffs, 1, function(x){
        exp((x['(Intercept)'] +  (x['age_id_6'] * config_row['age_id_6']) + 
             (x['age_id_9'] * config_row['age_id_9']) +
             (x['age_id_10'] * config_row['age_id_10']) + (x['age_id_11'] * config_row['age_id_11']) + (x['age_id_12'] * config_row['age_id_12'])+
             (x['age_id_13'] * config_row['age_id_13']) + (x['age_id_14'] * config_row['age_id_14']) + (x['age_id_15'] * config_row['age_id_15'])+
             (x['age_id_16'] * config_row['age_id_16']) + (x['age_id_17'] * config_row['age_id_17'])+ (x['age_id_18'] * config_row['age_id_18']) +
             (x['age_id_19'] * config_row['age_id_19']) + (x['age_id_20'] * config_row['age_id_20']) +(x['age_id_30'] * config_row['age_id_30'])+
             (x['age_id_31'] * config_row['age_id_31']) + (x['age_id_32'] * config_row['age_id_32'])+(x['age_id_235'] * config_row['age_id_235'])+
             (x['haq'] * config_row['haq'])))
      })
      
      }else if(regular_categorical_age & cause_name=="neo_liver_hbl"){
      age_vars <- colnames(input_coeffs)[grepl("age_id",  colnames(input_coeffs))]
      results <- apply(input_coeffs, 1, function(x){
        exp((x['(Intercept)'] +  (x['age_id_6'] * config_row['age_id_6']) +
               (x['haq'] * config_row['haq'])))
      })
    }else if(regular_categorical_age & cause_name=="neo_eye_rb"){
      age_vars <- colnames(input_coeffs)[grepl("age_id",  colnames(input_coeffs))]
      results <- apply(input_coeffs, 1, function(x){
        exp((x['(Intercept)'] +  (x['age_id_6'] * config_row['age_id_6']) + 
               (x['haq'] * config_row['haq'])))
      })
      
    } else if (regular_categorical_age & cause_name=="neo_eye_other"){
      results <- apply(input_coeffs, 1, function(x){
        exp((x['(Intercept)'] + (x['age_id_7'] * config_row['age_id_7']) + (x['age_id_8'] * config_row['age_id_8']) + (x['age_id_9'] * config_row['age_id_9']) + 
               (x['age_id_10'] * config_row['age_id_10']) + (x['age_id_11'] * config_row['age_id_11']) + (x['age_id_12'] * config_row['age_id_12'])+
               (x['age_id_13'] * config_row['age_id_13']) + (x['age_id_14'] * config_row['age_id_14']) + (x['age_id_15'] * config_row['age_id_15'])+
               (x['age_id_16'] * config_row['age_id_16']) + (x['age_id_17'] * config_row['age_id_17'])+ (x['age_id_18'] * config_row['age_id_18']) +
               (x['age_id_19'] * config_row['age_id_19']) + (x['age_id_20'] * config_row['age_id_20']) +(x['age_id_30'] * config_row['age_id_30'])+
               (x['age_id_31'] * config_row['age_id_31']) + (x['age_id_32'] * config_row['age_id_32']) + #(x['age_id_235'] * config_row['age_id_235'])+
               (x['haq'] * config_row['haq'])))
      })
    
    }else if(!regular_categorical_age & !use_continuous_age){
      # for dummy age models with aggregated age groups
      age_vars <- colnames(input_coeffs)[grepl("age_id",  colnames(input_coeffs))]
      
      if(!(cause_name %in% c("neo_eye","neo_eye_rb", "neo_liver_hbl","neo_eye_other","neo_lymphoma_burkitt"))){
        results <- apply(input_coeffs, 1, function(x){
          exp((x['(Intercept)'] + (x['age_id_2'] * config_row['age_id_2']) + (x['age_id_3'] * config_row['age_id_3']) + 
                 (x['age_id_4'] * config_row['age_id_4']) + (x['age_id_5'] * config_row['age_id_5']) +
                 (x['haq'] * config_row['haq'])))
        })
      } else if(cause_name %in% c("neo_eye")){
        results <- apply(input_coeffs, 1, function(x){
          exp((x['(Intercept)'] + (x['age_id_2'] * config_row['age_id_2']) + (x['age_id_3'] * config_row['age_id_3']) + 
                 (x['age_id_4'] * config_row['age_id_4']) + (x['age_id_5'] * config_row['age_id_5'])+(x['age_id_6'] * config_row['age_id_6'])+
               #(x['age_id_7'] * config_row['age_id_7'])+
                 (x['haq'] * config_row['haq'])))
        })
      } else if (cause_name %in% c("neo_eye_other")){
          results <- apply(input_coeffs, 1, function(x){
            exp((x['(Intercept)'] +  (x['age_id_2'] * config_row['age_id_2']) + (x['age_id_3'] * config_row['age_id_3']) + 
                   (x['age_id_4'] * config_row['age_id_4']) + (x['age_id_5'] * config_row['age_id_5']) +
                   (x['haq'] * config_row['haq'])))
          })
        
      } else if(cause_name%in% c("neo_lymphoma_burkitt")){
        results <- apply(input_coeffs, 1, function(x){
          exp((x['(Intercept)'] + (x['age_id_2'] * config_row['age_id_2']) + (x['age_id_3'] * config_row['age_id_3']) + 
                 (x['age_id_4'] * config_row['age_id_4']) +  
                 (x['haq'] * config_row['haq'])))
        })
       
      }else if(cause_name %in% c("neo_eye_rb", "neo_liver_hbl")){
        results <- apply(input_coeffs, 1, function(x){
          exp((x['(Intercept)'] + (x['age_id_2'] * config_row['age_id_2']) + (x['haq'] * config_row['haq'])))
        })
      } 
    } else{
      message("Please specify what model you want to use")
    }
    
    if(draw_count == 0 | draw_count == 1000){
      names(results) <- paste0("draw_", c(0:999))
    }
    if(draw_count == 100){
      names(results) <- paste0("draw_", c(0:99))
    }
    
    results <- as.list(results)
    setDT(results)
    results[, (uid_cols) := as.list(config_row[uid_cols])]
    results[, cause_name := cause_name]
    results
  }
  
  total_draws <- apply(config_rows, 1, computeMIR, input_coeffs = draw_coeffs, cause_name = cause_name)
  total_draws <- rbindlist(total_draws)
  print(paste0("done with draws for location", location_id))

  # change age ids back
  if(!regular_categorical_age){
    new_ages <- data.table(old_age_id = c(1, 6:20, 30:32, 235), age_group_id = c(1:20))
    total_draws <- merge(total_draws, new_ages, by = 'age_group_id')
    setnames(total_draws, new = c("age_group_id", "new_age_id"), old = c("old_age_id", "age_group_id"))
    total_draws$new_age_id <- NULL
  }

  # copying in females and male results
  females <- copy(total_draws)
  females$sex_id <- 2
  males <- copy(total_draws)
  males$sex_id <- 1
  all_results <- rbindlist(list(total_draws, females, males))
  print("done copying sexes")
  # final cleaning, renaming columns
  setnames(all_results, new = c("acause"), old = c("cause_name"))
  setnames(all_results, new = c("sex", "year"), old = c("sex_id", "year_id"))
  all_results$run_id <- 111
  
  # merging in caps
  caps <- grab_caps(mir_version = mir_version)
  
  all_results <- merge(caps[, list(acause, age_group_id, lower_cap, all_cause_upper_cap, cause_specific_upper_cap)], all_results, 
                       by = c("age_group_id", "acause"), all.y = T)
  
  if(draw_count == 0 | draw_count == 1000){
    draw_cols <- paste0("draw_", c(0:999))
  }
  if(draw_count == 100){
    draw_cols <- paste0("draw_", c(0:99))
  }

  # APPLY POST-PROCESSING CAPS
  
  # Apply all cause caps for the subset of causes:
  if(!(cause_name %in% c("neo_liver_hbl", "neo_eye_rb"))){
    # calculate the all cause cap for all except the above
    all_results <- all_results[, lapply(.SD, function(x) ifelse(x > all_cause_upper_cap, all_cause_upper_cap, x)), .SDcols = draw_cols, 
                             by = c("age_group_id", "acause", "location_id", "year", "sex", 
                                    "run_id", "all_cause_upper_cap","cause_specific_upper_cap", "lower_cap", "haq")]
  } else {
    # Calculate the cause specific caps for the causes excluded above
    all_results <- all_results[, lapply(.SD, function(x) ifelse(x > cause_specific_upper_cap, cause_specific_upper_cap, x)), .SDcols = draw_cols, 
                             by = c("age_group_id", "acause", "location_id", "year", "sex", 
                                    "run_id", "all_cause_upper_cap","cause_specific_upper_cap", "lower_cap", "haq")]
  }
  
  #Apply lower caps, which are cause specific for all causes
  all_results <- all_results[, lapply(.SD, function(x) ifelse(x < lower_cap, lower_cap, x)), .SDcols = draw_cols,
                             by = c("age_group_id", "acause", "location_id", "year", "sex", 
                                    "run_id", "lower_cap", "all_cause_upper_cap","cause_specific_upper_cap", "haq")]
  
  if(draw_count != 0){
    print("The draws are being saved right now")
    fwrite(all_results, paste0(get_path(key = "new_mir_tempFolder", process = "mir_model"), "/",mir_version,"/", cause_name, "/", 
                                cause_name, "_mir_v", mir_version, "_draws", location_id,"_",run_description,".csv"))
    print("\n Draws saved!")
  }
  
  all_results[, mean := rowMeans(.SD), .SDcols = draw_cols]
  all_results[, upper := apply(.SD, 1, quantile, probs = 0.975, na.rm=T), .SDcols = draw_cols]
  all_results[, lower := apply(.SD, 1, quantile, probs = 0.025, na.rm=T), .SDcols = draw_cols]
  print(paste0("done with summaries for location", location_id))
  
  all_results[, (draw_cols) := NULL]
  all_results
}


parallel_draw_launch <- function(cause_name, mir_version = 85, draw_count = 0, new_data="", 
                                 add_year = F , regular_categorical_age =T, use_continuous_age=F, 
                                 use_haqi_estimates = T, combine_data_types = F,
                                 run_description = ""){
  # main function that launches the 1000 draw calculation of MIRs in parallel
  # (18 threads) by cause and produces the summaries for the MIRs,
  # applies caps to them and saves them
  # 
  # Args:
  #   mir_version: best version of mirs
  #   cause_name: cause for model 
  #   draw_count: number of draw columns to generate, possible values: [0, 100, 1000]
  #   new_data: specify the path to the new data set to include (not the stgpr prepared inputs for a round)
  #   add_year: whether to add year as a variable 
  #   ages: whether to include ages as a categorical variable in gbd 5 year bins
  #
  # Returns:
  #   None
  
  # launches the draws by location into respective folders
  mir_version <- mir_version
  
  if(new_data==""){st_gpr_input=T} else {st_gpr_input=F}
  data_results <- get_negative_binomial_coef(cause_name = cause_name, transform_mir = F,
                                            mir_version = mir_version, use_haqi_estimates = use_haqi_estimates, 
                                            use_year_as_var = add_year, 
                                            combine_data_types = combine_data_types,
                                            st_gpr_input = st_gpr_input, new_data = new_data, 
                                            use_continuous_age = use_continuous_age,
                                            regular_categorical_age = regular_categorical_age)
  model <- data_results[['model']]
  cause_name <- cause_name[1]
  total_config <- get_nb_config(acause_name = cause_name, regular_categorical_age = regular_categorical_age, use_continuous_age = use_continuous_age, use_haqi_estimates = use_haqi_estimates)
  draw_coeffs <- get_coef_matrix(acause_name = cause_name, model = model, draw_count = draw_count, regular_categorical_age = regular_categorical_age)

  all_results <- mclapply(total_config, FUN = function(x){
   generate_mir_draws(config_rows = x, draw_coeffs = draw_coeffs,
                      cause_name = cause_name, draw_count = draw_count, regular_categorical_age = regular_categorical_age, use_continuous_age = use_continuous_age,
                      run_description = run_description)
  }, mc.cores = detectCores() - 2)

  all_results <- rbindlist(all_results)
  print("Done with compiling results")

  all_results[, c("lower_cap", "all_cause_upper_cap", "cause_specific_upper_cap", "haq", "lower", "upper") := NULL]
  setnames(all_results, new = c("mi_ratio"), old = c("mean"))

  path_to_save_means = paste0(get_path(key = "new_mir_tempFolder", process = "mir_model"), "/",mir_version,"/", cause_name, "/", 
                        cause_name, "_mir_v", mir_version, "_",run_description,".csv")
  fwrite(all_results, path_to_save_means)

  # CREATE PLOTS
  # designate the paths to the input data
  if(st_gpr_input){
    input_set = paste0(get_path(key = "mir_inputs", process = "mir_model"), "/model_version_",mir_version,"/", 
                       cause_name, ".csv")
      if(combine_data_types){
        input_set_VR = paste0(get_path(key = "mir_inputs", process = "mir_model"), "/model_version_",mir_version,"/", 
                              cause_name, "_VR.csv")
      } else{
        input_set_VR = data.table()
      }
  } else {
    input_set = new_data
    input_set_VR = data.table()
  }
  # run plotting function
  create_plots(cause_name=cause_name, run_description=run_description, 
               predictions_set = path_to_save_means,
               input_set = input_set, input_set_VR = input_set_VR,
               combine_data_types=combine_data_types, mir_version = mir_version,
               out_dir = paste0(get_path(key = "new_mir_tempFolder", process = "mir_model"), "/",mir_version,"/", cause_name, "/"))
  
  # Rbind all locations draws into one compiled file
  all_locations_draws <- list.files(paste0(get_path(key = "new_mir_tempFolder", process = "mir_model"), "/",mir_version,"/", cause_name), full.names = T)
  all_locations_draws <- all_locations_draws[grepl(paste0("v",mir_version,"_draws"), all_locations_draws)&grepl(run_description,all_locations_draws)]
 
  all_locations_draws_df <- lapply(all_locations_draws, function(x){
    fread(x)
  })
  all_locations_draws_df <- rbindlist(all_locations_draws_df, fill = T)
  fwrite(all_locations_draws_df, paste0(get_path(key = "new_mir_tempFolder", process = "mir_model"), "/",mir_version,"/", cause_name, "/", 
                                cause_name, "_mir_v", mir_version, "compiled_draws_",run_description,".csv"))
}

# MAIN:
# For calling this worker script:
if (!interactive()) {
  cause_name1 <- commandArgs()[6] # options: see list of new causes in launcher script
  cause_name2 <- commandArgs()[7] # options: see list of new causes_VR in launcher script
  mir_version <- commandArgs()[8] # options: see mir_model_version table mir_version_ids
  draw_count <- commandArgs()[9] # options: [0, 100, 1000]

  # SPECIFY FINAL AGE BINS
  if(cause_name1 %in% c("neo_bone","neo_eye_rb")){
    # Launch these causes with regular gbd age bins as a categorical variable
    gbd_age_bins = T
  } else{
    # Launch the remaining causes (bone, neuro, tissue_sarcoma, 'hepatoblastoma') with binned age groups
    gbd_age_bins = F
  }
  
  #SPECIFY FINAL DATA SOURCE
  #all CR
  combine_data_types = F
  if (cause_name1=="neo_eye_rb") combine_data_types = T
  
  # HAQI COVARIATE
  if(cause_name1 %in% c("neo_neuro")){
    # bin haqi for neuroblastoma to improve trend
    use_haqi_estimates = F
  } else{
    use_haqi_estimates = T
  }
  
  # DESCRIPTION
  # providing a unique descriptor ensures that the correct location draws are compiled
  description = ""
  
  parallel_draw_launch(list(cause_name1, cause_name2), mir_version, as.numeric(draw_count), 
                       use_haqi_estimates = use_haqi_estimates, regular_categorical_age = gbd_age_bins, 
                       combine_data_types = combine_data_types, new_data = path_to_inputs,
                       run_description = description)
  
}else{
  print("Must run in cancer_env and non-interactively with R ")
}

# Static data loaders ----------------------------------------------------------

.load_essential_workers <- function(static_data_root = DATA_ROOTS$STATIC_VACCINE_INPUTS_ROOT) {
  essential <- fread(file.path(static_data_root, "prop_essential_workers.csv"))
  essential_jobtypes <- fread(file.path(static_data_root, "prop_essential_workers_by_industry.csv"))
  setnames(essential_jobtypes, c("essential"), c("essential_jobtype"))
  essential <- merge(essential, essential_jobtypes[,c("location_id","essential_jobtype","gbd_health_workers","count_gbd_health_workers")],
                     by = "location_id", all.x = T)
  return(essential)

}

static_data <- list(
  load_essential_workers = .load_essential_workers
)

# GBD data loaders----------------------------------------------------------

# Dumb wrapper just to keep extra stuff out of the name space.
.get_location_metadata <- function(location_set_id, location_set_version_id=NULL, gbd_round_id=NULL, decomp_step=NULL, release_id=NULL) {
  source(file.path(CODE_PATHS$CC_CODE_ROOT, "get_location_metadata.R"))
  hierarchy <- get_location_metadata(location_set_id, location_set_version_id, gbd_round_id, decomp_step, release_id)
  return(hierarchy)
}

.get_covid_modeling_hierarchy <- function() {
  hierarchy <- .get_location_metadata(
    location_set_id = 111,
    location_set_version_id = 1158, # Previous: 771, 1009, 1020, 1050
    release_id = 9)
  return(hierarchy)
}

.get_gbd_hierarchy <- function() {
  hierarchy <- fread('FILEPATH/gbd_analysis_hierarchy.csv')
  return(hierarchy)
}

.get_fhs_hierarchy <- function() {
  hierarchy <- .get_location_metadata(
    location_set_id = 39,
    location_set_version_id = 891,
    release_id = 9)
  return(hierarchy)
}

.get_counties_modeling_hierarchy <- function(new=TRUE) {
  
  if (new) {

    hierarchy <- fread('FILEPATH/fh_small_area_hierarchy.csv')
    
  } else {
  
  hierarchy <- .get_location_metadata(
    location_set_id = 120,
    location_set_version_id = 1052, # Previous 841
    release_id = 9)
  
  }
  
  return(hierarchy)
}

.get_covid_covariate_prep_hierarchy <- function() {
  hierarchy <- .get_location_metadata(
    location_set_id = 115,
    location_set_version_id = 1155, # Previous: 746, 1008, 1021, 1051
    release_id = 9)
  return(hierarchy)
}

.get_european_union_hierarchy <- function() {
  hierarchy <- .get_location_metadata(
    location_set_id = 124,
    location_set_version_id = 811,
    release_id = 9)
  return(hierarchy)
}

.get_who_european_hierarchy <- function() {
  hierarchy <- .get_location_metadata(
    location_set_id = 118,
    location_set_version_id = 783,
    release_id = 9)
  return(hierarchy)
}

.get_who_covid_hierarchy <- function() {
  hierarchy <- .get_location_metadata(
    location_set_id = 123,
    location_set_version_id = 1159, # previous: 1062
    release_id = 9)
  return(hierarchy)
}

# Oh man, why are there two of these?
.get_other_european_union_hierarchy <- function() {
  hierarchy <- .get_location_metadata(
    location_set_id = 11,
    release_id = 10)
  return(hierarchy)
}

.get_african_union_hierarchy <- function() {
  hierarchy <- .get_location_metadata(
    location_set_id = 32,
    gbd_round_id = 6)
  return(hierarchy)
}

.get_sdi <- function() {
  source(file.path(CODE_PATHS$CC_CODE_ROOT, 'get_covariate_estimates.R'))
  sdi_cov <- get_covariate_estimates(
    covariate_id = 1099,
    gbd_round_id = 6,
    year_id = 2019,
    decomp_step = "step4")
 return(sdi_cov)
}

gbd_data <- list(
  get_covid_modeling_hierarchy = .get_covid_modeling_hierarchy,
  get_counties_modeling_hierarchy = .get_counties_modeling_hierarchy,
  get_covid_covariate_prep_hierarchy = .get_covid_covariate_prep_hierarchy,
  get_gbd_hierarchy = .get_gbd_hierarchy,
  get_fhs_hierarchy = .get_fhs_hierarchy,
  get_european_union_hierarchy = .get_european_union_hierarchy,
  get_other_european_union_hierarchy = .get_other_european_union_hierarchy,
  get_african_union_hierarchy = .get_african_union_hierarchy,
  get_sdi = .get_sdi
)

# Model inputs loaders ----------------------------------------------------------

.load_all_populations <- function(model_inputs_root) {
  population <- fread(file.path(model_inputs_root, "FILEPATH/all_populations.csv"))
  return(population)
}

.load_total_population <- function(model_inputs_root) {
  population <- .load_all_populations(model_inputs_root)
  population <- population[age_group_id == 22 & sex_id == 3]
  population <- population[, lapply(.SD, function(x) sum(x)), by = "location_id", .SDcols = "population"]
  return(population)
}

.load_adult_population <- function(model_inputs_root, model_parameters) {

  pop_full <- .load_all_populations(model_inputs_root)
  #hierarchy <- .get_covid_modeling_hierarchy()
  
  adult_age_groups <- c(8:19,20,30:32,235) # Over 15
  
  if (model_parameters$include_o12 == T) {
    
    adult_age_groups <- c(7:19,20,30:32,235)
    pop_full[age_group_id == 7, population := population / 2]
  }
  
  if (model_parameters$include_o5 == T) adult_age_groups <- c(6:19,20,30:32,235) # Over 5

  adult_population <- pop_full[sex_id == 3 & age_group_id %in% adult_age_groups, lapply(.SD, function(x) sum(x)), by="location_id", .SDcols = "population"]
  setnames(adult_population, "population", "adult_population")

  return(adult_population)
}


.load_o12_u18_population <- function(model_inputs_root) {
  
  pop_full <- .load_all_populations(model_inputs_root)
  
  #o12_u18_age_groups <- 7:8
  #pop_full[age_group_id %in% 7, population := population * (3/5)] # 10-15
  #pop_full[age_group_id %in% 8, population := population * (3/5)] # 15-20

  o12_u18_age_groups <- 60:65
  
  out <- pop_full[sex_id == 3 & age_group_id %in% o12_u18_age_groups,
                             lapply(.SD, function(x) sum(x)), by = "location_id", .SDcols = "population"]
  
  setnames(out, "population", "o12_u18_population")
  return(out)
}

.load_o18_u65_population <- function(model_inputs_root) {
  
  pop_full <- .load_all_populations(model_inputs_root)
  
  #o18_u65_age_groups <- 8:17
  #pop_full[age_group_id %in% 8, population := population * (2/5)] # 15-20
  
  o18_u65_age_groups <- 66:112
  
  out <- pop_full[sex_id == 3 & age_group_id %in% o18_u65_age_groups,
                  lapply(.SD, function(x) sum(x)), by = "location_id", .SDcols = "population"]
  
  setnames(out, "population", "o18_u65_population")
  return(out)
}

.load_o65_population <- function(model_inputs_root) {
  pop_full <- .load_all_populations(model_inputs_root)
  o65_age_groups <- c(18:20, 30:32, 235)
  o65_population <- pop_full[sex_id == 3 & age_group_id %in% o65_age_groups,
                             lapply(.SD, function(x) sum(x)), by = "location_id", .SDcols = "population"]
  setnames(o65_population, "population", "over65_population")
  return(o65_population)
}

.load_o5_population <- function(model_inputs_root) {
  pop_full <- .load_all_populations(model_inputs_root)
  o5_age_groups <- c(6:19,20,30:32,235)
  o5_population <- pop_full[sex_id == 3 & age_group_id %in% o5_age_groups,
                             lapply(.SD, function(x) sum(x)), by = "location_id", .SDcols = "population"]
  setnames(o5_population, "population", "over5_population")
  return(o5_population)
}

.load_observed_vaccinations <- function(model_inputs_root, data_source, template="old") {
  #country_sources <- c("canada", "brazil", "italy", "india", "germany", "spain", "mexico")
  
  template_path = "vaccination.csv"
  #country_sources <- c(country_sources ,"global", "cdc")
  
  if (data_source == "our_world_in_data") {
    
    path <- file.path(model_inputs_root, "vaccine", glue("global_{template_path}"))
    
  } else {
    
    path <- file.path(model_inputs_root, "vaccine", glue("{data_source}_{template_path}"))
  }
  
  observed_data <- fread(path)
  
  setnames(observed_data, # for indexing, ordering and clarity
           c("additional_dose", "second_additional_dose"),
           c("booster_1",       "booster_2"))
  
  if ('Date' %in% colnames(observed_data)) {
    observed_data[, date := as.Date(Date)]
    observed_data[, Date := NULL]
  } else {
    observed_data[, date := as.Date(date)]
  }
  
  # If boosters_administered is all missing, assign to booster_1 column
  observed_data <- rbindlist(
        lapply(
          split(observed_data,
                by = "location_id"),
          function(x) {
            if (all(is.na(x$booster_1))) {
            message(glue("Assigning booster_1 from boosters_administered for {unique(x$location_name)} ({unique(x$location_id)})"))
            # create booster_1 from all boosters if booster_1 is blank
            x[all(is.na(booster_1)), booster_1 := boosters_administered]
            } else {
              x
            }
          }
        )
      )
  
  return(observed_data)
}

#' Hot-wire in data from an arbitrary location using complete path to file
#'
#' @param file_path [character] complete path to hot-wired data file
#' ex: /mnt/share/covid-19/data_intake/vaccine/spain/spain_vaccination.csv"
#'
#' @return [data.table] raw vaccine data
.load_hotwired_vaccinations <- function(file_path) {
  
  observed_data <- fread(file_path)
  
  setnames(observed_data, # for indexing, ordering and clarity
           c("additional_dose", "second_additional_dose"),
           c("booster_1",       "booster_2"))
  
  if ("Date" %in% colnames(observed_data)) {
    setnames(observed_data, "Date", "date")
  }
  
  # If boosters_administered is all missing, assign to booster_1 column
  observed_data <- rbindlist(
    lapply(
      split(observed_data,
            by = "location_id"),
      function(x) {
        if (all(is.na(x$booster_1))) {
          message(glue("Assigning booster_1 from boosters_administered for {unique(x$location_name)} ({unique(x$location_id)})"))
          # create booster_1 from all boosters if booster_1 is blank
          x[all(is.na(booster_1)), booster_1 := boosters_administered]
        } else {
          x
        }
      }
    )
  )
  
  return(observed_data)
}

# Load vaccination data stratified by brand or age
.load_new_vaccination_data <- function(model_inputs_root,  # The root directory containing the data (/ihme/covid-19/data_intake OR /ihme/covid-19/model-inputs/{version})
                                       type=NULL           # Which data type: NULL = only location-date, brand = location-date-brand, age = location-date-age
) {
  
  if (type == 'brand') file_suffix <- 'vaccination_brand.csv' else 
    if (type == 'age') file_suffix <- 'vaccination_age.csv' else
      stop('Unrecognized type')
    
    path <- file.path(model_inputs_root, 'vaccine', file_suffix)
    
    observed_data <- fread(path)
    
    if ('Date' %in% colnames(observed_data)) {
      observed_data[, date := as.Date(Date)]
      observed_data[, Date := NULL]
    } else {
      observed_data[, date := as.Date(date)]
    }
    
    observed_data$source <- sapply(observed_data$data_filename, 
                                   FUN = function(x) unlist(strsplit(x, '_'))[1])
    
    # observed_data[, data_filename := NULL]
    
    return(observed_data)
}


.load_emergency_use_authorizations <- function(model_inputs_root, use_data_intake=FALSE) {
  options("openxlsx.dateFormat" = "yyyy-mm-dd")
  if (use_data_intake) root <- DATA_ROOTS$DATA_INTAKE_ROOT else root <- model_inputs_root
  openxlsx::read.xlsx(file.path(root, 'FILEPATH/emergency_use_authorizations.xlsx'), detectDates = TRUE)
}


.load_manual_start_dates <- function(model_inputs_root) {
  path <- file.path(model_inputs_root, "vaccine", "manual_start_dates.csv")
  known_start_date <- fread(path)
  known_start_date[, date := as.Date(date, "%m/%d/%Y")]
  known_start_date[, reported_vaccinations := 1000]
  known_start_date[, daily_reported_vaccinations := 1000]
  known_start_date[, daily_fully_vaccinated := 0]
  known_start_date[, manual_start := 1]
  return(known_start_date)
}

.load_purchase_candidates_data <- function(model_inputs_root, data_set) {
  data_set_map <- list(
    stage_probability = "probability_success.csv",
    manufacturing_capacity = "vaccine_capacity_manufacturers_20201014.csv",
    secured_doses = "vaccine_doses_secured_by_country_20201014.csv",
    secured_doses_manufacturer = "vaccine_doses_secured_country_manufacturer_2021_06_24.csv",
    total_capacity = "vaccine_total_capacity_quarters_20201014.csv",
    quarter_capacity = "Linksbridge_Capacity_by_Quarter.csv",
    candidate_stages = "vaccine_trial_stage_20201106.csv"
  )
  path <- file.path(model_inputs_root, "vaccine", data_set_map[[data_set]])
  data <- fread(path)
  return(data)
}

.load_gavi_data <- function(model_inputs_root, gavi_dose_scenario) {
  data_set_map <- list(
    less = "AZ_and_Pfizer_allocation_for_impact_models_2021-02-11_13B.csv",
    more = "AZ_and_Pfizer_allocation_for_impact_models_2021-02-11_18B.csv"
  )
  path <- file.path(model_inputs_root, "vaccine", data_set_map[[gavi_dose_scenario]])
  data <- fread(path)
  return(data)
}

.load_new_gavi_data <- function(model_inputs_root, gavi_dose_scenario) {
  data_set_map <- list(
    low = "covax_amc92_cov_scenarios_2021-08_age_50_cov_15.csv",
    medium = "covax_amc92_cov_scenarios_2021-08_age_18_cov_60.csv",
    high = "covax_amc92_cov_scenarios_2021-08_age_12_cov_70.csv"
  )
  
  path <- file.path(model_inputs_root, data_set_map[[gavi_dose_scenario]])
  data <- fread(path)
  names(data)[which(names(data) == "cname")] <- "country_name"
  return(data)
}

.load_dose_update_sheet <- function(model_inputs_root) {
  path <- file.path(model_inputs_root, "vaccine", "dose_date_purchase_updates_2021_06_24.csv")
  data <- fread(path)
  return(data)
}

.load_vaccine_efficacy_table <- function(model_inputs_root) {
  path <- file.path(model_inputs_root, "vaccine", "vaccine_efficacy_table_2021_09_14.csv")
  data <- fread(path)
  return(data)
}

.load_covax_locations <- function(model_inputs_root) {
  path <- file.path(model_inputs_root, "vaccine", "covax_locations.csv")
  data <- fread(path)
  return(data)
}

.load_symptom_survey_data <- function(model_inputs_root, use_limited_use) {
  if (use_limited_use) {
    data <- list(
      us_states = fread(file.path(DATA_ROOTS$LIMITED_USE_SYMPTOM_SURVEY, "FILEPATH/d_f.csv")),
      global = rbind(
        fread(file.path(DATA_ROOTS$LIMITED_USE_SYMPTOM_SURVEY, "FILEPATH/d_c.csv")),
        fread(file.path(DATA_ROOTS$LIMITED_USE_SYMPTOM_SURVEY, "FILEPATH/d_r.csv")),
        fill = T))
  } else {
    data <- list(
      us_states = fread(file.path(model_inputs_root, "symptom_survey/US_d_f.csv")),
      global = rbind(
        fread(file.path(model_inputs_root, "symptom_survey/GLOBAL_d_c.csv")),
        fread(file.path(model_inputs_root, "symptom_survey/GLOBAL_d_r.csv")),
        fill = T))
  }
  return(data)
}

.load_vaccine_hesitancy_data <- function(model_inputs_root, use_data_intake) {
  if (use_data_intake) {
    path <- DATA_ROOTS$DATA_INTAKE_ROOT
  } else {
    path <- model_inputs_root
  }
  data <- fread(file.path(path, 'FILEPATH/hesitancy_ca.csv'))
  data$date <- as.Date(data$date)
  return(data)
}

model_inputs_data = list(
  load_all_populations = .load_all_populations,
  load_total_population = .load_total_population,
  load_adult_population = .load_adult_population,
  load_o12_u18_population = .load_o12_u18_population,
  load_o18_u65_population = .load_o18_u65_population,
  load_o65_population = .load_o65_population,
  load_o5_population = .load_o5_population,
  load_observed_vaccinations = .load_observed_vaccinations,
  load_hotwired_vaccinations = .load_hotwired_vaccinations,
  load_new_vaccination_data = .load_new_vaccination_data,
  load_manual_start_dates = .load_manual_start_dates,
  load_purchase_candidates_data = .load_purchase_candidates_data,
  load_gavi_data = .load_gavi_data,
  load_new_gavi_data = .load_new_gavi_data,
  load_dose_update_sheet = .load_dose_update_sheet,
  load_vaccine_efficacy_table = .load_vaccine_efficacy_table,
  load_covax_locations = .load_covax_locations,
  load_symptom_survey_data = .load_symptom_survey_data
)

# Inputs and outputs for the vaccine version ----------------------------------------------------------

.write_vaccine_efficacy <- function(data, vaccine_output_root) {
  path <- file.path(vaccine_output_root, 'vaccine_efficacy_table.csv')
  write.csv(data, path, row.names = F)
}

.load_vaccine_efficacy <- function(vaccine_output_root) {
  path <- file.path(vaccine_output_root, "vaccine_efficacy_table.csv")
  data <- fread(path)
  return(data)
}

.write_purchase_candidates <- function(data, vaccine_output_root) {
  path <- file.path(vaccine_output_root, 'purchase_candidates.csv')
  write.csv(data, path, row.names = F)
}

.load_purchase_candidates <- function(vaccine_output_root) {
  path <- file.path(vaccine_output_root, 'purchase_candidates.csv')
  data <- fread(path)
  return(data)
}

.write_final_doses <- function(data, vaccine_output_root, filename='final_doses_by_location.csv') {
  path <- file.path(vaccine_output_root, filename)
  write.csv(data, path, row.names = F)
}

.load_final_doses <- function(vaccine_output_root) {
  path <- file.path(vaccine_output_root, 'final_doses_by_location.csv')
  data <- fread(path)
  return(data)
}

.write_time_point_vaccine_hesitancy <- function(data, vaccine_output_root, scenario) {
  suffix <- if (scenario == 'default') '' else glue('_{scenario}')
  path <- file.path(vaccine_output_root, glue('time_point_vaccine_hesitancy{suffix}.csv'))
  write.csv(data, path, row.names = F)
}

.load_time_point_vaccine_hesitancy <- function(vaccine_output_root, scenario) {
  suffix <- if (scenario == 'default') '' else glue('_{scenario}')
  path <- file.path(vaccine_output_root, glue('time_point_vaccine_hesitancy{suffix}.csv'))
  data <- fread(path)
  return(data)
}

.write_time_series_vaccine_hesitancy <- function(data, vaccine_output_root, scenario) {
  suffix <- if (scenario == 'default') '' else glue('_{scenario}')
  path <- file.path(vaccine_output_root, glue('time_series_vaccine_hesitancy{suffix}.csv'))
  write.csv(data, path, row.names = F)
}

.load_time_series_vaccine_hesitancy <- function(vaccine_output_root, scenario) {
  suffix <- if (scenario == 'default') '' else glue('_{scenario}')
  path <- file.path(vaccine_output_root, glue('time_series_vaccine_hesitancy{suffix}.csv'))
  data <- fread(path)
  data[, date := as.Date(date)]
  return(data)
}

.write_observed_vaccinations <- function(data, vaccine_output_root) {
  path <- file.path(vaccine_output_root, "observed_data.csv")
  write.csv(data, path, row.names = F)
}

.load_observed_vaccinations <- function(vaccine_output_root) {
  path <- file.path(vaccine_output_root, "observed_data.csv")
  data <- fread(path)
  data[, date := as.Date(date)]
  data[, max_date := as.Date(max_date)]
  data[, min_date := as.Date(min_date)]
  return(data)
}

.write_empirical_lag_days <- function(data, vaccine_output_root) {
  path <- file.path(vaccine_output_root, "empirical_lag_days.csv")
  write.csv(data, path, row.names = F)
}

.load_empirical_lag_days <- function(vaccine_output_root) {
  path <- file.path(vaccine_output_root, "empirical_lag_days.csv")
  data <- fread(path)
  return(data)
}

.write_scenario_forecast <- function(data, vaccine_output_root, scenario) {
  path <- file.path(vaccine_output_root, glue("{scenario}_scenario_vaccine_coverage.csv"))
  write.csv(data, path, row.names = F)
}

.load_scenario_forecast <- function(vaccine_output_root, scenario) {
  path <- file.path(vaccine_output_root, glue("{scenario}_scenario_vaccine_coverage.csv"))
  data <- fread(path)
  data[, date := as.Date(date)]
  return(data)
}

.load_hestancy_model_ouput <- function(vaccine_output_root) {
  path <- file.path(vaccine_output_root, "time_series_vaccine_hesitancy.csv")
  data <- fread(path)
  data[, date := as.Date(date)]
  return(data)
}

.write_model_parameters <- function(model_parameters, vaccine_output_root_write) {
  path <- file.path(vaccine_output_root_write, "model_parameters.yaml")
  yaml::write_yaml(model_parameters, file = path)
}

.load_model_parameters <- function(vaccine_output_root_load) {
  path <- file.path(vaccine_output_root_load, "model_parameters.yaml")
  model_parameters <- yaml::read_yaml(path)
  return(model_parameters)
}

vaccine_data = list(
  write_vaccine_efficacy = .write_vaccine_efficacy,
  load_vaccine_efficacy = .load_vaccine_efficacy,
  write_purchase_candidates = .write_purchase_candidates,
  load_purchase_candidates = .load_purchase_candidates,
  write_final_doses = .write_final_doses,
  load_final_doses = .load_final_doses,
  write_time_point_vaccine_hesitancy = .write_time_point_vaccine_hesitancy,
  load_time_point_vaccine_hesitancy = .load_time_point_vaccine_hesitancy,
  write_time_series_vaccine_hesitancy = .write_time_series_vaccine_hesitancy,
  load_time_series_vaccine_hesitancy = .load_time_series_vaccine_hesitancy,
  write_observed_vaccinations = .write_observed_vaccinations,
  load_observed_vaccinations = .load_observed_vaccinations,
  write_empirical_lag_days = .write_empirical_lag_days,
  load_empirical_lag_days = .load_empirical_lag_days,
  write_scenario_forecast = .write_scenario_forecast,
  load_scenario_forecast = .load_scenario_forecast,
  load_hestancy_model_ouput = .load_hestancy_model_ouput,
  write_model_parameters = .write_model_parameters,
  load_model_parameters = .load_model_parameters
)

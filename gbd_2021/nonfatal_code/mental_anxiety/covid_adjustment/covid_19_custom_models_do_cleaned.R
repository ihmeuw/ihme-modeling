
source("/FILEPATH/get_ids.R")
source("/FILEPATH/get_draws.R")
source("/FILEPATH/interpolate.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_population.R")
library(mrbrt001, lib.loc = '/FILEPATH/')

working_folder <- "/FILEPATH/"

rlogit <- function(x){exp(x)/(1+exp(x))}
logit <- function(x){log(x/(1-x))}

get_beta_vcov <- function(model){
  model_specs <- mrbrt001::core$other_sampling$extract_simple_lme_specs(model)
  beta_hessian <- mrbrt001::core$other_sampling$extract_simple_lme_hessian(model_specs)
  solve(beta_hessian)
}

get_beta_sd <- function(model){
  beta_sd <- sqrt(diag(get_beta_vcov(model)))
  names(beta_sd) <- model$cov_names
  return(beta_sd)
}

get_gamma_sd <- function(model){
  gamma <- model$gamma_soln
  gamma_fisher <- model$lt$get_gamma_fisher(gamma)
  return(sqrt(diag(solve(gamma_fisher))))
}

get_beta_with_alpha <- function(model){
  beta_sd <- sqrt(diag(get_beta_vcov(model)))
  alpha <- sd((model$data$obs - model$predict(model$data, predict_for_study=T))/model$data$obs_se)
  return(beta_sd <- beta_sd * alpha)
}


args<-commandArgs(trailingOnly = TRUE)
location <- args[1]

set.seed(516857) 

epi_age_groups <- c(2:3, 388:389, 238, 34,  6:20, 30:32, 235)

parent <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)[location_id == location, parent_id]
grand_parent <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)[location_id == parent, parent_id]


# Load indicator models ---------------------------------------------------
model_indicator_mdd <- py_load_object(filename = paste0(working_folder, "mdd_indicator_model.pkl"), pickle = "dill")
model_indicator_anx <- py_load_object(filename = paste0(working_folder, "anx_indicator_model.pkl"), pickle = "dill")

# Load adjustment models --------------------------------------------------
model_mdd <- py_load_object(filename = paste0(working_folder, "aic_mdd_model.pkl"), pickle = "dill")
model_anxiety <- py_load_object(filename = paste0(working_folder, "aic_anx_model.pkl"), pickle = "dill")

# Load indicator data -----------------------------------------------------

########## Human mobility #########

loc_mobility <-  fread("/FILEPATH/mobility_reference.csv")
loc_mobility[, date_n := as.numeric(as.Date(paste0(date)) - as.Date(0, origin="1899-12-30", tz='UTC')), by = "date"]
dates <- unique(loc_mobility[date_n %in% c(43831:44926), .(date, date_n)])

if(location %in% loc_mobility$location_id){
  loc_mobility <- loc_mobility[location_id == location, ]
} else if(parent %in% loc_mobility$location_id){
  loc_mobility <- loc_mobility[location_id == parent, ]
  loc_mobility[, location_id := location]
} else {
  loc_mobility <- loc_mobility[location_id == grand_parent, ]
  loc_mobility[, location_id := location]
}

loc_mobility <- merge(loc_mobility, dates, by = c('date_n', 'date'), all = T) # In case any data is missing on covid-19 impact indicators
loc_mobility[, year_id := as.numeric(substr(date, 1, 4))]
loc_mobility <- loc_mobility[year_id %in% c(2020:2022),]

loc_mobility[, mobility_reference := (-mean)/100] # reverse direction to reflect the 'drop'
loc_mobility[is.na(mobility_reference), mobility_reference := (-mobility_forecast)/100]
loc_mobility[is.na(mobility_reference), `:=` (mobility_reference = 0, location_id = loc_mobility[!is.na(location_id), unique(location_id)], location_name = loc_mobility[!is.na(location_name), unique(location_name)])]

loc_mobility <- loc_mobility[,.(date = date_n, mobility_reference, location_id, location_name, year_id)]

########## Covid-19 daily deaths #########

ihme_deaths <- fread("/FILEPATH/daily_deaths.csv")

if(location %in% ihme_deaths$location_id){
  ihme_deaths <- ihme_deaths[location_id == location, ]
  deaths_location <- location
} else if(parent %in% ihme_deaths$location_id){
  ihme_deaths <- ihme_deaths[location_id == parent, ]
  ihme_deaths[, location_id := location]
  deaths_location <- parent
} else {
  ihme_deaths <- ihme_deaths[location_id == grand_parent, ]
  ihme_deaths[, location_id := location]
  deaths_location <- grand_parent
}

ihme_deaths <- melt.data.table(ihme_deaths, id.vars = names(ihme_deaths)[!(names(ihme_deaths) %like% "draw")], value.name="deaths", variable.name="draw")
ihme_deaths[, date_n := as.numeric(as.Date(paste0(date)) - as.Date(0, origin="1899-12-30", tz='UTC')), by = "date"]
ihme_deaths[is.na(deaths), deaths := 0]
ihme_deaths[, deaths := mean(deaths), by = c("location_id", "date")]
ihme_deaths <- unique(ihme_deaths[,.(location_id, date, date_n, deaths)])
ihme_deaths <- merge(ihme_deaths, dates, by = c('date_n', 'date'), all = T) # In case any data is missing on covid-19 impact indicators
ihme_deaths[, year_id := as.numeric(substr(date, 1, 4))]
ihme_deaths <- ihme_deaths[year_id %in% c(2020:2022),]
ihme_deaths[is.na(deaths), `:=` (deaths = 0, location_id = ihme_deaths[!is.na(location_id), unique(location_id)])]
ihme_deaths <- ihme_deaths[,.(date = date_n, location_id, deaths, year_id)]

population_deaths <- get_population(age_group_id = 22, location_id = deaths_location, year_id = c(2020:2022), gbd_round_id = 7, decomp_step = 'iterative')
ihme_deaths <- merge(ihme_deaths, population_infections[,.(year_id, population)], by = 'year_id')

ihme_deaths[, death_rate := deaths / population]
ihme_deaths[, covid_deaths_sqrt := sqrt(death_rate)]

########## Create indicators #########
indicator <- merge(loc_mobility, ihme_deaths, by = c("location_id", "date", "year_id"), all = T)

indicator[, indicator_mdd := mobility_reference * as.numeric(model_indicator_mdd$fe_soln["social_mobility"]) + covid_deaths_sqrt * as.numeric(model_indicator_mdd$fe_soln["covid_death_sqrt"])]
indicator[, indicator_anx := mobility_reference * as.numeric(model_indicator_anx$fe_soln["social_mobility"]) + covid_deaths_sqrt * as.numeric(model_indicator_anx$fe_soln["covid_death_sqrt"])]

rm(loc_mobility, ihme_infections, ihme_deaths, stringency_data)

mean_mid_age_mdd <- as.numeric(fread(paste0(working_folder, "mean_mid_age_mdd.csv")))
mean_mid_age_anx <- as.numeric(fread(paste0(working_folder, "mean_mid_age_anx.csv")))

for(d in c(1981, 1989)){
  dismod_prev <- interpolate(gbd_id_type = "modelable_entity_id", gbd_id = d, source = "epi", age_group_id = epi_age_groups, measure_id = c(5), location_id = location, reporting_year_start=1990, reporting_year_end=2022, sex_id = c(1,2), status = "best", decomp_step='iterative', gbd_round_id = 7)
  dismod_inc <- interpolate(gbd_id_type = "modelable_entity_id", gbd_id = d, source = "epi", age_group_id = epi_age_groups, measure_id = c(6), location_id = location, reporting_year_start=1990, reporting_year_end=2022, sex_id = c(1,2), status = "best", decomp_step='iterative', gbd_round_id = 7)
  dismod_prev <- rbind(dismod_prev, dismod_inc)
  dismod_prev[, model_version_id := NULL]
  dismod_prev[, modelable_entity_id := NULL]
  if(d == 1981){
    dismod_prev[, modelable_entity_id  := 26756]
  } else {
    dismod_prev[, modelable_entity_id  := 26759]
  }
  
  for(y in unique(dismod_prev$year_id[dismod_prev$year_id < 2020])){
    write.csv(dismod_prev[year_id == y,], paste0(working_folder, "covid_adj_prev/", d, "/prev_", location, "_", y,".csv"), row.names=F)
  }
 
  dismod_prev <- melt.data.table(dismod_prev[year_id %in% c(2020:2022)], id.vars = names(dismod_prev)[!(names(dismod_prev) %like% "draw")], value.name="raw_prev", variable.name="draw")
  
  dismod_inc <- dismod_prev[measure_id == 6,]
  dismod_prev <- dismod_prev[measure_id == 5,]
  
  
  ages <- get_ids('age_group')[age_group_id %in%  epi_age_groups,]
  ages[, `:=` (age_start = as.numeric(unlist(strsplit(age_group_name, " "))[1]), age_end = as.numeric(unlist(strsplit(age_group_name, " "))[3])), by = "age_group_id"]
  ages[age_group_id %in% c(2, 3, 238, 388, 389), `:=` (age_start = 0, age_end = 0)]
  ages[age_start == 95, age_end := 99]
  ages[, mid_age := (age_start+age_end)/2]
  
  dismod_prev <- merge(dismod_prev, ages[,.(age_group_id, mid_age)], all.x = T, by = "age_group_id")
  dismod_prev[, percent_female := sex_id - 1.5] # mean-centre at 50% female
  
  if(d == 1981){
    dismod_prev <- merge(dismod_prev, indicator[,.(location_id, date, year_id, indicator = indicator_mdd)], by = c("location_id", "year_id"), all.x = T, allow.cartesian=TRUE)
    dismod_prev[, m_mid_age := mid_age - mean_mid_age_mdd]
  } else{
    dismod_prev <- merge(dismod_prev, indicator[,.(location_id, date, year_id, indicator = indicator_anx)], by = c("location_id", "year_id"), all.x = T, allow.cartesian=TRUE)
    dismod_prev[, m_mid_age := mid_age - mean_mid_age_anx]
  }
  
  dismod_prev[is.na(indicator), indicator := 0]
  
  dismod_prev[, `:=` (age_int = m_mid_age * indicator, sex_int = percent_female * indicator)]
  
  # Create draws ------------------------------------------------------------
  
  
  predict_matrix <- unique(dismod_prev[,.(intercept = 0, indicator, age_int, sex_int, int_both_md = 0, int_online_panel = 0, cv_cross_panel = 0)])
  
  
  predict_data <- MRData()
  predict_data$load_df(data = predict_matrix, col_covs=as.list(names(predict_matrix)))
  
  if(d == 1981){
    beta_samples <- mrbrt001::core$other_sampling$sample_simple_lme_beta(1000L, model_mdd)
   
    gamma_outer_samples <- matrix(rep(model_mdd$gamma_soln, each = 1000L), nrow = 1000L)
    gamma_outer_samples[,1] <- 0 # remove intercept gamma
    draws <- model_mdd$create_draws(predict_data,
                                    beta_samples = beta_samples,
                                    gamma_samples = gamma_outer_samples,
                                    random_study = F)
  } else {
    beta_samples <- mrbrt001::core$other_sampling$sample_simple_lme_beta(1000L, model_anxiety)
    
    gamma_outer_samples <- matrix(rep(model_anxiety$gamma_soln, each = 1000L), nrow = 1000L)
    gamma_outer_samples[,1] <- 0 # remove intercept gamma
    draws <- model_anxiety$create_draws(predict_data,
                                        beta_samples = beta_samples,
                                        gamma_samples = gamma_outer_samples,
                                        random_study = F)
  }
  draws <- data.table(draws)
  names(draws) <- paste0("draw_", as.numeric(gsub("V", "", names(draws)))-1)
  predict_matrix <- cbind(predict_matrix, draws)
  
  predict_matrix <- melt.data.table(predict_matrix, id.vars = names(predict_matrix)[!(names(predict_matrix) %like% "draw")], value.name="adjustment", variable.name="draw")
  predict_matrix[, `:=` (cv_cross_panel = NULL, int_both_md = NULL, int_online_panel = NULL)]
  
  dismod_prev <- merge(dismod_prev, predict_matrix, all.x = T, by = c("indicator", "age_int", "sex_int", "draw"))
  
  
  # Apply adjustments -------------------------------------------------------
  
  dismod_prev[, `:=` (raw_prev = logit(raw_prev))]
  dismod_prev[, adj_prev := raw_prev - adjustment]
  dismod_prev[, `:=` (adj_prev = rlogit(adj_prev), raw_prev = rlogit(raw_prev))]
  
  ## Estimate annual point prevalence
  dismod_prev[, `:=` (annual_prev = mean(adj_prev)), by = c("draw", "age_group_id", "sex_id", 'year_id')]
  
  dismod_prev <- unique(dismod_prev[, .(modelable_entity_id, draw, location_id, age_group_id, measure_id, sex_id, year_id, metric_id, raw_prev, annual_prev)])
  
  ## Adjust incidence data
  dismod_prev[, ratio := annual_prev / raw_prev]
  dismod_prev[is.na(ratio), ratio := 1]
  
  dismod_inc <- merge(dismod_inc, dismod_prev[,.(age_group_id, sex_id, year_id, draw, ratio)], all = T, by = c("draw", "age_group_id", "sex_id", "year_id"))
  dismod_inc[, annual_prev := raw_prev * ratio] # annual_prev is just to aid rbind and dcast functions below, but still represents incidence. raw_prev = raw incidence in incidence data frame.
  
  dismod_prev <- rbind(dismod_prev, dismod_inc)
  
  dismod_prev[, `:=` (raw_prev = NULL, ratio = NULL)]
  
  dismod_prev <- dcast(dismod_prev, modelable_entity_id + location_id + age_group_id + measure_id + sex_id + year_id + metric_id ~ draw, value.var="annual_prev")
  
  for(y in c(2020:2022)){
    write.csv(dismod_prev[year_id == y,], paste0(working_folder, "covid_adj_prev/", d, "/prev_", location, "_", y, ".csv"), row.names=F)
  }
}



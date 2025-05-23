######################################################################################################################################################################################################################################
#
# Purpose: Use ARIMA to fore/backcast from STGPR draws, using the range of available data (or in locations with no data, the super-regional data range) as the original time series
#
#####################################################################################################################################################################################################################################
# rm(list=ls())

#manual parameters
gbd_rnd <- 2023
release <- 16
main_dir <- 'FILEPATH'

#we model out 1 extra year
gbd_rnd <- gbd_rnd + 1 

# set-up environment -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
invisible(sapply(list.files("FILEPATH", full.names = T), source))
source("FILEPATH/utility.r")
source("FILEPATH/public.R")
pacman::p_load(readstata13, data.table, ggplot2, gridExtra, dplyr, mgcv, boot, splines, msm)
library(gamm4, lib.loc = "FILEPATH")
library('forecast')
library('tseries')

#get arguments
if (interactive()) {
  
} else {
  # bring in args
  args <- commandArgs(trailingOnly = TRUE)
  l <- args[1]
  run_id <- args[2]
  a_input <- args[3]
  version <- args[4] 
}

#get standard vars
locs <- get_location_metadata(22, release_id = release)
vars <- paste0("draw_", seq(0,999,1))

# functions for backcasting
reverse_ts <- function(y) 
{
  ts(rev(y), start=tsp(y)[1L], frequency=frequency(y)) #tsp = start time; frequency = number of samples per unit time
}

# function to reverse a forecast
reverse_forecast <- function(object) 
{
  h <- length(object[["mean"]])
  f <- frequency(object[["mean"]])
  object[["x"]] <- reverse_ts(object[["x"]])
  object[["mean"]] <- ts(rev(object[["mean"]]), #reverse the time series
                         end=tsp(object[["x"]])[1L]-1/f, #end = earliest year of time series -1/ divided by frequency
                         frequency=f)
  object[["lower"]] <- object[["lower"]][h:1L,]
  object[["upper"]] <- object[["upper"]][h:1L,]
  return(object)
}

# start ARIMA modeling -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#read in stgpr model, subset to locs with more than 2 data points in a given age band (holt needs at least 2 observations to estimate a trend)
initial_model_with_data <- fread(a_input)
initial_model_with_data <- initial_model_with_data[year_id <= gbd_rnd] #limit results to gbdround + 1

#subset pre-loaded vars in case we are using a 100 draw test
vars <- vars[vars %in% names(initial_model_with_data)]

#read in input data
input_data <- get_input_data(run_id, data_stage_name = "original") #changed from model_load(id, "data")
input_data <- merge(input_data, locs[, c('location_id', 'super_region_id', 'region_id')], by='location_id')
input_data <- input_data[year_id <= gbd_rnd] #limit results to gbdround 

#create directory to write files to
dir.create(paste0(main_dir, 'forecasted_draws/', run_id, '/', version, '/'), recursive=T)

#set final dt up
predicted_draws_age <- data.table()

for (a in unique(initial_model_with_data$age_group_id)){ #do this by age
  country_data <- initial_model_with_data[location_id == l & age_group_id==a] #get country data for location-age combo
  if (is.na(unique(country_data$min_year))){ #min_year will be filled out if available data, so if no country-level data, look at super-region
    regional_input <- input_data[region_id==locs[location_id==l]$region_id]
    min_year <- min(regional_input$year_id)
    max_year <- max(regional_input$year_id) 
    country_data <- country_data[year_id <= max(regional_input$year_id) & year_id >= min(regional_input$year_id)] 
  } else { #if data, use country_data min and max year
    min_year <- unique(country_data$min_year)
    max_year <- unique(country_data$max_year)
    if (min_year %in% seq(max_year-1,max_year+1,1)) {
      regional_input <- input_data[region_id==locs[location_id==l]$region_id]
      min_year <- min(regional_input$year_id)
      max_year <- max(regional_input$year_id) 
      country_data <- country_data[year_id <= max(regional_input$year_id) & year_id >= min(regional_input$year_id)] 
    } else {
      country_data <- country_data[training==1]
    }
  }
  
  #set data table
  predicted_draws <- data.table()
  
  #print some info for error file
  message(paste0('minimum yr: ', min_year))
  message(paste0('maximum yr: ', max_year))
  message(paste0(l,": ",a))
  
  for(d in vars){
    # create a time series for the data that are present
    data_ma_new <- ts(na.omit(country_data[,d,with=F]), start=c(min_year), end=c(max_year))
    
    # build arima model
    # h = number of periods for forecasting
    # phi = value of damping parameter if damped = T
    fcast <- holt(data_ma_new, damped=TRUE, phi = 0.9, h = gbd_rnd - max_year)
    
    #time series in reverse order
    new_time_series <- data_ma_new %>%
      reverse_ts()
    
    #'forecasts' the reverse ts
    fit_og_forecast <- holt(new_time_series, damped=TRUE, phi = 0.9, h = ((min_year) - 1980))
    
    # reverse forecasts
    reverse_fit <- reverse_forecast(fit_og_forecast)
    
    #formatting
    reverse_fit_to_plot <- data.table(mean = as.numeric(reverse_fit$mean), lower = as.numeric(reverse_fit$lower[,2]), upper = as.numeric(reverse_fit$upper[,2]), year_id = seq(1980, min_year - 1, 1), version = "backcasted")
    forecast_fit_to_plot <- data.table(mean = as.numeric(fcast$mean), lower = as.numeric(fcast$lower[,2]), upper = as.numeric(fcast$upper[,2]), year_id = seq(max_year + 1, gbd_rnd + 1, 1), version = "forecasted")
    data_present_to_plot <- data.table(mean = as.numeric(data_ma_new), year_id = seq(min_year,max_year,1), version = "rolling mean")
    
    all_data <- rbind(reverse_fit_to_plot,forecast_fit_to_plot)
    all_data <- rbind(all_data,data_present_to_plot,fill=T)
    all_data$location_id <- l
    all_data[,age_group_id := a]
    
    # aren't actually going to ever use the upper and lower, just a hold over from Parkes' code
    all_data[, c("mean","lower","upper") := list(inv.logit(mean), inv.logit(lower), inv.logit(upper))]
    
    # add draw to the total dataset
    all_data[,draw := d]
    predicted_draws <- rbind(predicted_draws,all_data)
  }
  predicted_draws_age <- rbind(predicted_draws, predicted_draws_age)
}

#final trim (1980 - gbd round; ages 15+)
predicted_draws_age <- predicted_draws_age[year_id >= 1980 & year_id <= gbd_rnd]

#save
write.csv(predicted_draws_age,paste0(main_dir, "forecasted_draws/", run_id, "/", version, "/", l, ".csv"), row.names = F)




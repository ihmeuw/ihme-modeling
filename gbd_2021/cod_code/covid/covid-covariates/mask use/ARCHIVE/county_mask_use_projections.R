######################################################
## Explore approaches to model vaccine hesitancy, 
## mask use at the county level in the US. 
######################################################

library(ggplot2)
library(data.table)
library(gridExtra)

source("/ihme/cc_resources/libraries/current/r/get_location_metadata.R")
source("/ihme/cc_resources/libraries/current/r/get_population.R")

## install ihme.covid (always use newest version)
tmpinstall <- system("mktemp -d --tmpdir=/tmp", intern = TRUE)
.libPaths(c(tmpinstall, .libPaths()))
devtools::install_github("ihmeuw/ihme.covid", upgrade = "never", quiet = T, ref = "get_latest_output_dir")
##

hierarchy <- get_location_metadata(location_set_id = 111, 771)
data_dir <- "/snfs1/Project/covid/data_intake/symptom_survey/us/"

## Weighted
vaccine_county <- fread(paste0(data_dir, "getvaccine_ts_counties_weighted_assigned_states_from_zip.csv"))
  vaccine_county[, date := as.Date(date, "%d.%m.%Y")]
  vaccine_county[, county_vax_yes := (getvaccine_yes + getvaccine_yesprobably) / N]
mask_county <- fread(paste0(data_dir, "mask5days_ts_counties_weighted_assigned_states_from_zip.csv"))
  mask_county[, date := as.Date(date, "%d.%m.%Y")]
  
## Unweighted
mask_raw_county <- fread(paste0(data_dir, "mask5days_ts_counties_assigned_states_from_zip.csv"))
  mask_raw_county[, date := as.Date(date, "%d.%m.%Y")] 
  mask_raw_county[, N_raw := N_mask5days]
  
## Merge to get sample size
mask_county <- merge(mask_county, mask_raw_county[,c("date","state","county","N_raw")])

vaccine_state_dt <- fread(paste0(data_dir, "getvaccine_ts_weighted_assigned_states_from_zip.csv"))
mask_state_dt <- fread(paste0(data_dir, "mask5days_ts_weighted_assigned_states_from_zip.csv"))

vaccine_predictions <- fread("/ihme/covid-19-2/vaccine-coverage/best/time_series_vaccine_hesitancy.csv")
mask_predictions <- fread("/ihme/covid-19-2/mask-use-outputs/best/mask_use.csv")
  mask_predictions[, date := as.Date(date)]

# Drop Georgia country
mask_predictions <- mask_predictions[location_id != 35]
vaccine_predictions <- vaccine_predictions[location_id != 35]
  
#####################################################
## Okay, what are our options?

## For vaccine coverage, time series less important (very small samples)
## so aggregate to county

plot_dt <- mask_county[!is.na(location_id)]

pdf("/home/j/temp/ctroeger/COVID19/county_mask_use_plots.pdf", height = 10, width = 14)
# for(ct in unique(plot_dt$location_id)){
#   st <- unique(plot_dt[location_id == ct, state])
#   p <- ggplot(plot_dt[state == st & location_id == ct]) + geom_point(aes(x = date, y = mask5days_all / N_mask5days, size = N_raw, col = county), alpha = 0.3) +
#     stat_smooth(aes(x = date, y = mask5days_all / N_mask5days, col = county), method = "loess", se = F) +
#     geom_line(data = mask_predictions[location_name == st & date < "2022-01-01"], aes(x = as.Date(date), y = mask_use), col = "black") +
#     theme_classic() + scale_size_continuous("Sample size") + scale_color_discrete("County") + xlab("Date") + ylab("Always mask use") +
#     ggtitle(paste0(unique(plot_dt[location_id == ct, county]),": ", unique(plot_dt[location_id == ct, state])), subtitle = "Masks")
#   print(p)
# }
for(ct in unique(plot_dt$state)){
  p <- ggplot(plot_dt[state == ct]) + geom_point(aes(x = date, y = mask5days_all / N_mask5days, size = N_raw), alpha = 0.3) +
    stat_smooth(aes(x = date, y = mask5days_all / N_mask5days), method = "loess", se = F) +
    facet_wrap(~county) + 
    geom_line(data = mask_predictions[location_name == st & date < "2022-01-01"], aes(x = as.Date(date), y = mask_use), col = "black") +
    theme_classic() + scale_size_continuous("Sample size") + scale_color_discrete("County") + xlab("Date") + ylab("Always mask use") +
    ggtitle(ct)
  print(p)
}
dev.off()

## So, my proposed shift, is to modify the state time series up or down based on county-level mask use (aggregate)
# Pull in raw data, find mean ratio to scale smoothed estimate

county_dt <- copy(plot_dt)

county_dt <- county_dt[N_mask5days > 0]
county_dt <- county_dt[!is.na(prop_mask5days_all)]
county_dt <- county_dt[prop_mask5days_all > 0]
county_dt[, location_obs := .N, by = "location_id"]

# Drop last day of data
county_dt <- county_dt[date < max(date)]
max_obs_date <- max(county_dt$date)

smooth_neighbors <- 5
smooth_iterations <- 10
county_dt <- county_dt[location_obs > smooth_neighbors]

county_dt[, smooth_county_mask_use := ihme.covid::barber_smooth(prop_mask5days_all, n_neighbors = smooth_neighbors, times = smooth_iterations), by = "location_id"]

# Project forward indefinitely
library(zoo)
county_estimates <- data.table()
for(ct in unique(county_dt$location_id)){
  tmp <- county_dt[location_id == ct]
  tmp <- tmp[, location_id := NULL]
  tmp <- merge(mask_predictions[,c("date","location_name","mask_use","location_id")], 
               county_dt[location_id == ct], by.x = c("location_name","date"), by.y = c("state","date"))
  date_range <- seq(max(tmp$date), as.Date("2022-09-01"), by = "1 day")
  complete_ts <- data.table(date = date_range)
  complete_ts$location_id.y <- ct
  complete_ts$location_id.x <- unique(tmp$location_id.x)
  complete_ts$county <- unique(tmp$county)
  complete_ts$state <- unique(tmp$location_name)
  tmp$state <- tmp$location_name
  tmp <- rbind(tmp, complete_ts, fill = T)
  tmp[, smooth_county_mask_use := na.locf(smooth_county_mask_use)]
  #tmp[, mask_use.x := na.locf(mask_use.x)]
  county_estimates <- rbind(county_estimates, tmp)
}

#county_estimates[, state_mask_use := mask_use.x]
county_estimates[, mask_use := smooth_county_mask_use]
county_estimates[, county_location_id := location_id.y]
county_estimates[, location_id := location_id.x]
county_estimates[, c("location_id.x","location_id.y") := NULL]

### Seems to work. Now modify vaccine scalar function
vaccine_behavior_change <- function(mask_estimates = out_dt, vaccine_version = "best", reduction_mask_vaccine = 0.25, 
                                    end_scalar = 2, end_coverage = 1, lag_vaccine = 0){
  
  model_hier <- get_location_metadata(111, 771)
  population <- fread("/ihme/covid-19/model-inputs/best/age_pop.csv")
  population <- population[, lapply(.SD, function(x) sum(x)), by="location_id", .SDcols = "population"]
  
  vaccine <- fread(paste0("/ihme/covid-19-2/vaccine-coverage/",vaccine_version,"/slow_scenario_vaccine_coverage.csv"))
  vaccine[, date := as.Date(date)]
  vaccine[, cumulative_all_vaccinated := shift(cumulative_all_vaccinated, n = lag_vaccine, fill = 0), by = "location_id"]
  
  mask_estimates <- merge(mask_estimates, vaccine[,c("location_id","date","cumulative_all_vaccinated","adult_population")],
                          by = c("location_id","date"), all.x = T)
  mask_estimates <- merge(mask_estimates, population, by = "location_id")
  mask_estimates <- mask_estimates[order(location_id, county, date)]
  mask_estimates[is.na(cumulative_all_vaccinated) & date < "2021-12-31", cumulative_all_vaccinated := 0]
  mask_estimates[, cumulative_all_vaccinated := na.locf(cumulative_all_vaccinated), by = c("state","county")]
  mask_estimates[is.na(adult_population) & date < "2021-12-31", adult_population := 0]
  mask_estimates[, adult_population := na.locf(adult_population), by = c("state","county")]
  mask_estimates[is.na(cumulative_all_vaccinated), cumulative_all_vaccinated := 0]
  
  mask_estimates[, vaccinated_pct := cumulative_all_vaccinated / adult_population]
  
  ## Adjust accept = yes for people already vaccinated
  mask_estimates[date == max_obs_date + lag_vaccine, obs_vaccinated_pct := vaccinated_pct]
  mask_estimates[, obs_vaccinated_pct := max(obs_vaccinated_pct, na.rm = T), by = c("state","county")]
  
  # mask_estimates[, county_vaccine := (county_vaccine * adult_population + obs_vaccinated_pct * adult_population) / 
  #                                     (adult_population + adult_population * obs_vaccinated_pct)]
  # 
  # ## Adjust for scalar of people who say they would be vaccinated, by city
  # mask_estimates[, state_level_vax := max(vaccinated_pct, na.rm = T), by = c("state","county")]
  # mask_estimates[, state_vax_scalar := county_vaccine / state_level_vax]
  # mask_estimates[, vaccinated_pct := vaccinated_pct * state_vax_scalar]
  
  mask_estimates[, end_tail := tail(mask_use,1), by = c("state","county")]

  mask_estimates[, option_100 := end_tail * (1-vaccinated_pct / end_coverage)]
  
  ## Exponential change
  decrease_range <- seq(0, end_coverage, 0.001)
  exp_decrease <- log(end_scalar / reduction_mask_vaccine) / length(decrease_range)
  t <- data.table(decrease_range, rows = 1:length(decrease_range))
  t[, scalar := reduction_mask_vaccine * exp(exp_decrease * rows)]
  t[, linear_scalar := 1 - (rows - 1) / max(rows)]
  t[, decrease_range := round(decrease_range, digits = 3)]
  
  mask_estimates[, decrease_range := round(vaccinated_pct, digits = 3)]
  mask_estimates <- merge(mask_estimates, t[,c("decrease_range","scalar", "linear_scalar")], by = "decrease_range", all.x = T)
  mask_estimates[is.na(scalar), scalar := end_scalar]
  
  mask_estimates[, option_exp := end_tail * (1 - vaccinated_pct * scalar)]
  mask_estimates[option_exp < 0, option_exp := 0]

  # Intercept shift estimates:
  mask_estimates[, c("option_exp","option_100") := 
                   lapply(.SD, function(x) x + (head(end_tail, 1) - x[date == Sys.Date()])), 
                 by = c("state","county"),
                 .SDcols = c("option_exp", "option_100")]
  
  mask_estimates[, c("cumulative_all_vaccinated","end_tail") := NULL]
  
  return(mask_estimates)
}

out_dt <- vaccine_behavior_change(county_estimates, vaccine_version = "best", reduction_mask_vaccine = 0.25, end_coverage = 1,
                                  lag_vaccine = 90)
out_dt[, base_estimate_mask_use := mask_use]

out_dt[option_100 < 0, option_100 := 0]
out_dt[, option_exp := ifelse(option_exp < option_100, option_100, option_exp)]
out_dt[, mask_use := ifelse(date > Sys.Date(), option_exp, mask_use)]

out_dt <- merge(out_dt, mask_predictions[, c("location_id","mask_use","date")],
                     by.x = c("location_id","date"), by.y = c("location_id","date"))

out_dt[, mask_use := mask_use.x]
out_dt[, state_mask_use := mask_use.y]

## Add Puerto Rico
out_dt <- rbind(out_dt, mask_predictions[location_name == "Puerto Rico"], fill = T)

pdf("/home/j/temp/ctroeger/COVID19/county_mask_use_projections.pdf", height = 12, width = 18)
# for(ct in unique(county_list$city)){
#   p <- ggplot(out_dt[city == ct]) + geom_line(aes(x = date, y = mask_use, col = "county level")) +
#     geom_line(aes(col = "state level", x = date, y = mask_use.y)) + 
#     geom_point(aes(x = date, y = county_mask_use, col = "county level"), alpha = 0.25) + 
#     ylab("Mask use") + theme_classic() + ggtitle(ct) + ylim(0,1) + 
#     scale_color_discrete("")
#   print(p)
# }
for(ct in unique(plot_dt$state)){
  p <- ggplot(out_dt[state == ct]) + geom_line(aes(x = date, y = mask_use, col = "County")) +
    facet_wrap(~ county) + 
    scale_color_manual(values = "purple") + 
    geom_line(aes(x = date, y = state_mask_use), col = "black") +
    theme_classic() + xlab("Date") + ylab("Always mask use") +
    ggtitle(ct)
  print(p)
}
dev.off()

out_dt[, location := ifelse(!is.na(county), county, location_name)]
final_dt <- out_dt[, c("location","state","county","date","mask_use","state_mask_use")]


write.csv(final_dt, "/home/j/temp/ctroeger/COVID19/county_mask_use_projections.csv", row.names = F)

## Subset for ones of CSU interest.
county_list <- data.table(state = c("Florida","Florida","Florida",
                                    "Florida",
                                    "North Carolina",
                                    "Georgia", "Georgia",
                                    "Arizona",
                                    "Texas","Texas","Texas",
                                    "Texas", "Texas","Texas",
                                    "Colorado"),
                          county = c("Miami-Dade County","Broward County","Palm Beach County",
                                     "Orange County", 
                                     "Mecklenburg County", 
                                     "Fulton County","Dekalb County",
                                     "Maricopa County",
                                     "Bexar County","Medina County","Comal County",
                                     "Harris County", "Fort Bend County", "Montgomery County",
                                     "Denver County"),
                          city = c("Miami","Miami","Miami",
                                   "Orlando",
                                   "Charlotte",
                                   "Atlanta","Atlanta",
                                   "Phoenix",
                                   "San Antonio","San Antonio","San Antonio",
                                   "Houston","Houston","Houston",
                                   "Denver"))

csu_results <- merge(final_dt, county_list, by = c("state","county"))
csu_results <- rbind(csu_results, final_dt[location == "Puerto Rico"], fill = T)

pdf("/home/j/temp/ctroeger/COVID19/csu_county_mask_use.pdf")
for(ct in unique(county_list$city)){
  f <- ggplot(csu_results[city == ct], aes(x = date, y = mask_use, col = county)) + geom_line() +
    xlab("") + scale_y_continuous("Always mask use (%)", labels = percent, limits = c(0,1)) + 
    theme_classic() + scale_color_discrete("") + 
    geom_line(aes( y = state_mask_use, col = "State level")) + 
    ggtitle(ct)
  print(f)
}
  ggplot(csu_results[location == "Puerto Rico"], aes(x = date, y = mask_use)) + geom_line() +
    xlab("") + scale_y_continuous("Always mask use (%)", labels = percent, limits = c(0,1)) + 
    theme_classic() +
    ggtitle("Puerto Rico")
dev.off()

write.csv(csu_results, "/home/j/temp/ctroeger/COVID19/csu_county_mask_use.csv", row.names = F)



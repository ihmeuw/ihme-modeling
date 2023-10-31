###############################################################
## Function to adjust mask use based on vaccine coverage.
## Assumption is that vaccinated people are less likely to 
## continue wearing a mask.
###############################################################
library(zoo)

model_hier <- get_location_metadata(ls, lsvid, release_id = release_id)
#population <- fread("/ihme/covid-19/model-inputs/best/age_pop.csv")
#population <- population[, lapply(.SD, function(x) sum(x)), by="location_id", .SDcols = "population"]

#mask_estimates = out_dt
#vaccine_version = "best"
#reduction_mask_vaccine = 0.25
#end_scalar = 2
#end_coverage = 1
#lag_vaccine = 0 
#yshift = F

vaccine_behavior_change <- function(mask_estimates = out_dt, 
                                    vaccine_version = "best", 
                                    reduction_mask_vaccine = 0.25, 
                                    end_scalar = 2, 
                                    end_coverage = 1, 
                                    lag_vaccine = 0, 
                                    yshift = F){
  
  vaccine <- fread(paste0("FILEPATH",vaccine_version,"/slow_scenario_vaccine_coverage.csv"))
  vaccine[, date := as.Date(date)]
  mask_estimates <- merge(mask_estimates, vaccine[,c("location_id","date","cumulative_all_vaccinated","adult_population")],
                          by = c("location_id","date"), all.x = T)
  #mask_estimates[, cumulative_all_vaccinated := shift(cumulative_all_vaccinated, n = lag_vaccine, fill = 0), by = "location_id"]
  mask_estimates <- merge(mask_estimates, population, by = "location_id")
  mask_estimates <- mask_estimates[order(location_id, date)]
  mask_estimates[is.na(cumulative_all_vaccinated) & date < "2021-12-31", cumulative_all_vaccinated := 0]
  mask_estimates[, cumulative_all_vaccinated := na.locf(cumulative_all_vaccinated), by = "location_id"]
  mask_estimates[is.na(adult_population) & date < "2021-12-31", adult_population := 0]
  mask_estimates[, adult_population := na.locf(adult_population), by = "location_id"]
  mask_estimates[is.na(cumulative_all_vaccinated), cumulative_all_vaccinated := 0]
  
  mask_estimates[, vaccinated_pct := cumulative_all_vaccinated / adult_population]
  
  # Uncomment this if you want a larger impact of vaccine
  # mask_estimates[, vaccinated_pct := vaccinated_pct / tail(vaccinated_pct, 1), by = "location_id"]
  
  mask_estimates[, end_tail := tail(mask_use,1), by = "location_id"]
  
  mask_estimates[, behavior_change_scenario := end_tail * (1-vaccinated_pct * reduction_mask_vaccine)]
  
  mask_estimates[, option_25 := end_tail * (1-vaccinated_pct * 0.25)]
  mask_estimates[, option_50 := end_tail * (1-vaccinated_pct * 0.5)]
  mask_estimates[, option_75 := end_tail * (1-vaccinated_pct * 0.75)]
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
  
  mask_estimates[, option_linear := end_tail * linear_scalar]
  

  ## Re-aggregate to national level 
  # mask_estimates <- merge(mask_estimates, model_hier[,c("location_id","parent_id")], by = "location_id")
  # parents <- model_hier[level == 3 & most_detailed == 0, location_id]
  # 
  # parent_update <- mask_estimates[location_id %in% model_hier[level == 3 & most_detailed == 0, location_id],
  #                                 lapply(.SD, function(x) sum(x * population)), 
  #                                 by = c("parent_id","date","observed"),
  #                                 .SDcols = c("mask_use","behavior_change_scenario","option_50","option_75")]
  
  # Intercept shift estimates:
  if(yshift == T){
    mask_estimates[, c("behavior_change_scenario","option_50","option_75","option_100","option_exp","option_linear") :=
                     lapply(.SD, function(x) x + (head(end_tail, 1) - x[date == Sys.Date()])),
                   by = c("location_id"),
                   .SDcols = c("behavior_change_scenario","option_50","option_75","option_100","option_exp","option_linear")]
  }
  
  mask_estimates[, c("option_100","option_exp") :=
                   lapply(.SD, function(x) shift(x, n = lag_vaccine, fill = 0)),
                 by = c("location_id"),
                 .SDcols = c("option_100","option_exp")]

  mask_estimates[, c("cumulative_all_vaccinated","end_tail") := NULL]
  
  return(mask_estimates)
}

# mask_estimates <- vaccine_behavior_change(checkpoint, vaccine_version = "best", reduction_mask_vaccine = 1, end_coverage = 1,
#                                           lag_vaccine = 30, yshift = F, end_scalar = 1)
# no_yshift <- vaccine_behavior_change(checkpoint, vaccine_version = "best", reduction_mask_vaccine = 1, end_coverage = 1,
#                                           lag_vaccine = 90, yshift = F, end_scalar = 1)
# loc_id <- 555
# ggplot(mask_estimates[location_id == loc_id], aes(x = as.Date(date))) +
#   geom_line(aes(y = mask_use, col = "Unchanged")) +
#   geom_line(data = no_yshift[location_id == loc_id], aes(x = as.Date(date), y = option_exp, col = "option 2")) +
#   geom_line(aes(y = option_exp, col = "option 1")) +
#   theme_bw() + xlab("") + ylab("Mask use") + ggtitle(unique(mask_estimates[location_id == loc_id, location_name])) +
#   geom_line(data = out_dt[location_id == loc_id], aes(x = as.Date(date), y = mask_use, col = "production"))

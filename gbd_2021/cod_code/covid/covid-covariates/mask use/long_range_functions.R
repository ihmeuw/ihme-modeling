###################################################
## Functions to make long range scenarios
###################################################
source(file.path("FILEPATH/get_covariate_estimates.R"))

## Scale using pneumonia seasonality
pneumo_scale <- function(df, time_period_mean = 90, mask_min = NA, pct_mask_max = 1, pct_min_bound = 0.1){
  df[, period_mean_mask := mean(mask_use[date > Sys.Date() - time_period_mean & date <= Sys.Date()]), by = "location_id"]
  df[, period_mean_pneumo := mean(pneumonia[date > Sys.Date() - time_period_mean & date <= Sys.Date()]), by = "location_id"]
  df[, min_pneumo := min(pneumonia), by = "location_id"]
  
  # Scale max use to range
  df[, max_mask_use_obs := max(mask_use), by = "location_id"]
  df[, seasonal_upper_bound := max_mask_use_obs * pct_mask_max]
  df[, seasonal_lower_bound := max_mask_use_obs * pct_min_bound]
  
  if(is.na(mask_min)){
    df[, mask_min := seasonal_lower_bound]
  } else{
    df$mask_min <- mask_min
  }
  
  # Scale pneumonia seasonality 0-1
  df[, pneumo_ratio := (pneumonia / min_pneumo)]
  df[, pneumo_ratio := pneumo_ratio - 1]
  df[, pneumo_ratio := (pneumo_ratio + mask_min) / (max(pneumo_ratio) + mask_min), by = "location_id"]
  
  ggplot(df[location_id == 528], aes(x = date, y = pneumonia, col = "pneumonia")) + geom_line() + geom_line(aes(y = pneumo_ratio, col = "standardized")) +
    geom_hline(yintercept = 1, col = "black", lty = 2) + geom_hline(yintercept = unique(df[location_id == 528]$mask_min))
  
  df[, mask_use_seasonal := seasonal_upper_bound * pneumo_ratio]
  
  df[!is.na(mask_use_seasonal), 
     mask_use_seasonal := lapply(.SD, function(x) rescale(x, to = c(head(seasonal_lower_bound,1), head(seasonal_upper_bound,1)), from = range(x))),
     .SDcols = "mask_use_seasonal",
     by = "location_id"]
  
  return(df)
}

## Linear change between dates based on latitude

latitude <- get_covariate_estimates(covariate_id = 56, decomp_step = "iterative", year_id = 2019, gbd_round_id = 7)
# Need to fill non-GBD locations with national value
has_children <- hierarchy[level == 3 & most_detailed == 0, location_id]
missing_lat <- hierarchy[parent_id %in% has_children, location_id]
missing_lat <- c(setdiff(missing_lat, latitude$location_id), 3539, 60886, 60887)
missing_lat <- hierarchy[location_id %in% missing_lat, c("location_id","parent_id","location_name")]
missing_lat <- merge(missing_lat, latitude, by.x = "parent_id", by.y = "location_id")
latitude <- rbind(latitude, missing_lat, fill = T)

latitude_scale <- function(df, shift_date_x = "2021-11-01", shift_date_y = "2022-03-01", pct_max = 1, pct_min = 0.1, days_for_change = 30){
  
  # shift_dates <- as.Date(c(paste0("2021-", c(shift_date_x)),
  #                          paste0("2022-", c(shift_date_y))))
  shift_dates <- as.Date(c(shift_date_x, shift_date_y))
  shift_dates <- shift_dates[order(shift_dates)]

    
  df <- merge(df, latitude[,c("location_id","mean_value")], by = "location_id", all.x = T)
  
  ## In Northern Hemisphere, mask use goes up on 'shift_date_x' and down on 'shift_date_y'
  # and vice-versa for Southern Hemisphere
  df[, northern := ifelse(mean_value > 0, 1, -1)]
  df[, max_observed := max(mask_use), by = "location_id"]
  df[, highest_val := max_observed * pct_max]
  df[, min_value := max_observed * pct_min]
  
  northern <- df[northern == 1]
  southern <- df[northern != 1]

  northern[, start_val_1 := mask_use[date == shift_dates[1]], by = "location_id"]
  northern[, target_lvl_1 := highest_val - start_val_1, by = "location_id"]
  northern[date > shift_dates[1], shift_lvl_1 := start_val_1 + target_lvl_1 / days_for_change * as.numeric(date - shift_dates[1] + 1), by = "location_id"]
  northern[, shift_lvl_1 := ifelse(shift_lvl_1 > highest_val, highest_val, shift_lvl_1)]
  
  northern[, target_lvl_2 :=  highest_val - min_value]
  northern[date >= shift_dates[2], shift_lvl_2 := highest_val - target_lvl_2 / days_for_change * as.numeric(date - shift_dates[2] + 1), by = "location_id"]
  northern[, shift_lvl_2 := ifelse(shift_lvl_2 < min_value, min_value, shift_lvl_2)]
  
  northern[, seasonal_line := ifelse(date > shift_dates[2], shift_lvl_2,
                                     ifelse(date > shift_dates[1], shift_lvl_1, mask_use_july))]
  
  southern[, start_val_1 := mask_use[date == shift_dates[2]], by = "location_id"]
  southern[, target_lvl_1 := highest_val - start_val_1, by = "location_id"]
  southern[date > shift_dates[2], shift_lvl_1 := start_val_1 + target_lvl_1 / days_for_change * as.numeric(date - shift_dates[2] + 1), by = "location_id"]
  southern[, shift_lvl_1 := ifelse(shift_lvl_1 > highest_val, highest_val, shift_lvl_1)]
  
  southern[, seasonal_line := ifelse(date > shift_dates[2], shift_lvl_1, mask_use_july)]

  df <- rbind(northern, southern, fill = T)
  
}

latitude_scale_sine <- function(df, shift_date_x = "11-01", shift_date_y = "03-01", pct_max = 1, pct_min = 0.1, days_for_change = 30, pinned_var = "mask_use"){
  
  df[, pinned := get(pinned_var)]
  shift_dates <- c(paste0("2021-", c(shift_date_x, shift_date_y)),
                   paste0("2022-", c(shift_date_x, shift_date_y)),
                   paste0("2023-", c(shift_date_x, shift_date_y)))
  shift_dates <- as.Date(shift_dates)
  shift_dates <- shift_dates[shift_dates != "2021-03-01"]
  shift_dates <- shift_dates[order(shift_dates)]

  df <- merge(df, latitude[,c("location_id","mean_value")], by = "location_id", all.x = T)
  
  ## In Northern Hemisphere, mask use goes up on 'shift_date_x' and down on 'shift_date_y'
  # and vice-versa for Southern Hemisphere
  df[, northern := ifelse(mean_value > 0, 1, -1)]
  df[, max_observed := max(mask_use), by = "location_id"]
  df[, highest_val := max_observed * pct_max]
  df[, min_value := max_observed * pct_min]
  
  # Remove missing locations... good idea?
  df <- df[!is.na(highest_val)]
  
  df[, linear_shift := NA]
  df[, linear_shift := ifelse(date < shift_dates[1], pinned, linear_shift)]
  ## Set to either high or low
  northern <- df[northern == 1]
  southern <- df[northern != 1]
  
  # northern[, linear_shift := ifelse(date > shift_dates[1] + days_for_change, highest_val,
  #                                   ifelse(date > shift_dates[2] & date < shift_dates[3], NA,
  #                                          ifelse(date >= shift_dates[3], min_value,
  #                                                 ifelse(date > shift_dates[4] & date < shift_dates[5], NA,
  #                                                        ifelse(date >= shift_dates[5], highest_val, NA)))))]
  northern[, linear_shift := ifelse(date > shift_dates[1] + days_for_change & date < shift_dates[2], highest_val,
                                    ifelse(date > shift_dates[2] + days_for_change & date < shift_dates[3], min_value,
                                           ifelse(date > shift_dates[3] + days_for_change & date < shift_dates[4], highest_val,
                                                  ifelse(date > shift_dates[4] + days_for_change & date < shift_dates[5], min_value,
                                                         ifelse(date > shift_dates[5] + days_for_change, highest_val, linear_shift)))))]
  northern[, linear_shift := na.approx(linear_shift), by = "location_id"]

  southern[, linear_shift := ifelse(date > shift_dates[1] + days_for_change & date < shift_dates[2], min_value,
                                    ifelse(date > shift_dates[2] + days_for_change & date < shift_dates[3], highest_val,
                                           ifelse(date > shift_dates[3] + days_for_change & date < shift_dates[4], min_value,
                                                  ifelse(date > shift_dates[4] + days_for_change & date < shift_dates[5], highest_val,
                                                         ifelse(date > shift_dates[5] + days_for_change, min_value, linear_shift)))))]
  southern[, linear_shift := na.approx(linear_shift), by = "location_id"]
  
  df <- rbind(northern, southern)
  return(df)
  
}

# df <- copy(out_dt)
# date1 <- "2021-11-01"
# date2 <- "2022-03-01"
# test <- latitude_scale(df, pct_max = 0.8, min_value = 0.1, days_for_change = 30, shift_date_x = date1, shift_date_y = date2)
# loc_id <- c(528, 570, 95, 135, 11, 123)
# ggplot(test[location_id %in% loc_id], aes(x = date)) + geom_line(aes(y = mask_use, col = "mask use")) + geom_line(aes(y = seasonal_line, col = "seasonal")) +
#   facet_wrap(~location_name) + theme_bw() + geom_vline(xintercept = as.Date(date1), lty = 2) +
#   geom_vline(xintercept = as.Date(date2), lty = 2)

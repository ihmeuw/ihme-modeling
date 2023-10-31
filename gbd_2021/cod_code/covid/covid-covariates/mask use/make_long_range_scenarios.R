#################################################################
## Make and save long-range scenarios for mask use
#################################################################

source(paste0(code_dir, "FILEPATH/long_range_functions.R")) # sourced here because it needs hierarchy

long_range <- copy(out_dt)
#long_range <- copy(current_dt)
long_range[, date := as.Date(date)]

# 1). Mask use remains at same level
long_range[, mask_use_constant := mask_use]

# 2). Mask use decreases to 0 or some fraction of max observed on July 1
fraction_max_obs <- 0.5
long_range[, decrease_days := as.numeric(as.Date("2021-07-01") - as.Date("2021-06-01"))]
long_range[, decrease_days := as.numeric(decrease_days)]
long_range[, location_max_obs := max(mask_use), by = "location_id"]
long_range[, day_tracker := ifelse(date <= as.Date("2021-06-01"), 0, 
                                   ifelse(date > as.Date("2021-07-01"), 0, 1 / decrease_days))]
long_range[, decrease_val := cumsum(day_tracker), by = "location_id"]

long_range[, mask_use_july := mask_use * (1 - decrease_val)]
long_range[date > Sys.Date(), mask_use_july := ifelse(mask_use_july < location_max_obs * fraction_max_obs,
                                                      location_max_obs * fraction_max_obs,
                                                      mask_use_july)]

# 3). Mask use returns to highest observed during "Covid season"
## Pull in pneumonia seasonality
# add pneumonia_reference from pseumo_ref to pneumo
pneumo <- fread("FILEPATH/reference_scenario.csv")
setnames(pneumo, "pneumonia_reference", "pneumonia")
pneumo[, date := as.Date(date)]

# Duplicate 2022 to get 2023
extend_pneumo <- pneumo[date >= "2022-01-01"]
extend_pneumo[, date := date + 365]
pneumo <- rbind(pneumo, extend_pneumo)

long_range[, max_observed := max(mask_use[date < "2021-03-08"]), by = "location_id"]

long_range <- merge(long_range, pneumo[,c("location_id","date","pneumonia")], by = c("location_id","date"), all.x = T)

## Alternate function in development that scales the seasonal projections
long_checkpoint <- copy(long_range)
long_range <- pneumo_scale(long_range, time_period_mean = 90, mask_min = NA, pct_mask_max = 1, pct_min_bound = 0.1)
# All should be based on June 1
long_range[, mask_use_out := ifelse(date < as.Date("2021-06-01"), mask_use, mask_use_seasonal)]
# If less than decrease to July 1, set to that
long_range[, mask_use_out := ifelse(mask_use_out < mask_use_july, mask_use_july, mask_use_out)]

pneumo_80 <- pneumo_scale(long_checkpoint, time_period_mean = 90, mask_min = NA, pct_mask_max = 0.8, pct_min_bound = 0.1)
pneumo_50 <- pneumo_scale(long_checkpoint, time_period_mean = 90, mask_min = NA, pct_mask_max = 0.6, pct_min_bound = 0.4)

# All should be based on June 1
pneumo_50[, mask_use_out := ifelse(date < as.Date("2021-06-01"), mask_use, mask_use_seasonal)]
setnames(pneumo_50, "mask_use_out", "seasonal_50")




## Alternative with linear change on given dates ##
linear_long_range <- copy(out_dt)

linear_long_range <- latitude_scale(long_range, 
                                    pct_max = 1, 
                                    pct_min = 0.1, 
                                    days_for_change = 30, 
                                    shift_date_x = "2021-11-01", 
                                    shift_date_y = "2022-03-01")

linear_long_sine <- latitude_scale_sine(long_range, 
                                        pct_max = 1, 
                                        pct_min = 0.1, 
                                        days_for_change = 30, 
                                        pinned_var = "mask_use_july")

linear_long_80 <- latitude_scale_sine(long_range, 
                                      pct_max = 0.8, 
                                      pct_min = 0, 
                                      days_for_change = 30, 
                                      pinned_var = "mask_use_july")

setnames(linear_long_80, 
         "linear_shift", 
         "linear_shift_80")

linear_long_half <- latitude_scale_sine(long_range, 
                                        pct_max = 0.5, 
                                        pct_min = 0, 
                                        days_for_change = 30, 
                                        pinned_var = "mask_use_july")

setnames(linear_long_half, 
         "linear_shift", 
         "linear_shift_half")


long_range <- merge(long_range, linear_long_range[,c("location_id","date","seasonal_line")],
                    by = c("location_id","date"))

long_range <- merge(long_range, linear_long_sine[,c("location_id","date","linear_shift")],
                    by = c("location_id","date"), all.x = T)

long_range <- merge(long_range, linear_long_80[,c("location_id","date","linear_shift_80")],
                    by = c("location_id","date"), all.x = T)

long_range <- merge(long_range, linear_long_half[,c("location_id","date","linear_shift_half")],
                    by = c("location_id","date"), all.x = T)

long_range <- merge(long_range, pneumo_50[,c("location_id","date","seasonal_50")],
                    by = c("location_id","date"), all.x = T)

long_range[, seasonal_line := ifelse(date < "2021-11-01", mask_use_july, seasonal_line)]



# Added scenario for winter surge paper
# Hemispheric linear with upper = 50% max observed and low = reference scenario
long_range$hemispheric_linear_50max_ref <- pmax(long_range$mask_use, long_range$linear_shift_half)

# Added scenario for the White House presentation
# Hemispheric linear with upper = 80% max observed and low = reference scenario
long_range$hemispheric_linear_80max_ref <- pmax(long_range$mask_use, long_range$linear_shift_80)




# Check
#tmp <- long_range[location_id == 115]
#ggplot(tmp, aes(x=date)) + 
#  geom_line( aes(y=mask_use), linetype=2, color='blue') +
#  geom_line( aes(y=linear_shift_half), linetype=2, color='red') +
#  geom_line( aes(y=hemispheric_linear_50max_ref), color='purple')

## Save out each long range scenario
permanent_low <- long_range[, c("location_id","date","observed","mask_use_july")]
setnames(permanent_low, "mask_use_july", "mask_use")

hemispheric <- long_range[, c("location_id","date","observed","linear_shift")]
setnames(hemispheric, "linear_shift", "mask_use")

hemispheric_80 <- long_range[, c("location_id","date","observed","linear_shift_80")]
setnames(hemispheric_80, "linear_shift_80", "mask_use")  

hemispheric_half <- long_range[, c("location_id","date","observed","linear_shift_half")]
setnames(hemispheric_half, "linear_shift_half", "mask_use")  

periodic <- long_range[, c("location_id","date","observed","mask_use_out")]
setnames(periodic, "mask_use_out", "mask_use")

seasonal_50 <- pneumo_50[, c("location_id","date","observed","seasonal_50")]
setnames(seasonal_50, "seasonal_50", "mask_use")

linear_50max_ref <- long_range[, c("location_id","date","observed","hemispheric_linear_50max_ref")]
setnames(linear_50max_ref, "hemispheric_linear_50max_ref", "mask_use")

linear_80max_ref <- long_range[, c("location_id","date","observed","hemispheric_linear_80max_ref")]
setnames(linear_80max_ref, "hemispheric_linear_80max_ref", "mask_use")


write.csv(permanent_low, paste0(output_dir, "/permanent_low_july.csv"), row.names = F)
write.csv(hemispheric, paste0(output_dir, "/hemispheric_linear.csv"), row.names = F)
write.csv(hemispheric_half, paste0(output_dir, "/hemispheric_half_linear.csv"), row.names = F)
write.csv(hemispheric_80, paste0(output_dir, "/hemispheric_80_linear.csv"), row.names = F)
write.csv(periodic, paste0(output_dir, "/seasonal_periodic.csv"), row.names = F)
write.csv(seasonal_50, paste0(output_dir, "/seasonal_50.csv"), row.names = F)
write.csv(linear_50max_ref, paste0(output_dir, "/hemispheric_linear_50max_ref.csv"), row.names = F)
write.csv(linear_80max_ref, paste0(output_dir, "/hemispheric_linear_80max_ref.csv"), row.names = F)



pdf(paste0(output_dir, "/long_range_scenario_line_plots.pdf"))

for(loc_id in hierarchy[most_detailed == 1 & !(parent_id %in% c(11, 71, 196)), location_id]){
  
  p <- ggplot(permanent_low[location_id == loc_id], aes(x = date, y = mask_use)) +
    #geom_line(aes(col = "Decrease to July 1")) +
    #geom_line(data = seasonal_50[location_id == loc_id], aes(col = "Seasonal 50%")) +
    geom_line(data = linear_50max_ref[location_id == loc_id], aes(col = "Seasonal linear 50max ref")) +
    geom_line(data = linear_80max_ref[location_id == loc_id], aes(col = "Seasonal linear 80max ref")) +
    #geom_line(data = periodic[location_id == loc_id], aes(col = "Seasonal periodic")) +
    geom_line(data = out_dt[location_id == loc_id], aes(col = "Production")) + 
    #geom_line(data = hemispheric_80[location_id == loc_id], aes(col = "Hemispheric 80")) + 
    theme_classic() + scale_color_discrete("") + 
    ylab("Mask use") + 
    ggtitle(hierarchy[location_id == loc_id, location_name])
  print(p)
}
dev.off()

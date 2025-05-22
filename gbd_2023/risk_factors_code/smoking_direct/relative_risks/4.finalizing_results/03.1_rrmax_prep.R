## HEADER #################################################################
# Date: 1/25/2022
# Purpose: Producing a new RRmax file for forecasting based on final GBD risk curves
#          

## SET-UP #################################################################
rrmax_old_fp <- commandArgs(trailingOnly = T)[1]
RO_pairs_fp <- commandArgs(trailingOnly = T)[2]
rr_draws_fp <- commandArgs(trailingOnly = T)[3]
py_exp_fp <- commandArgs(trailingOnly = T)[4]
cd_exp_fp <- commandArgs(trailingOnly = T)[5]
visual_fp <- commandArgs(trailingOnly = T)[6]
save_fp <- commandArgs(trailingOnly = T)[7]
save_plot <- commandArgs(trailingOnly = T)[8]

library(data.table)
library(ggplot2)

# Functions
expand_causes <- function(data_in, cause_in, causes_out, drop_orig) {
  data_in_temp <- data_in[cause_id == cause_in]
  for (i in causes_out) {
    data_in_temp <- data_in_temp[, cause_id := i]
    data_in <- rbindlist(list(data_in, data_in_temp), use.names = TRUE)
  }
  if (drop_orig == TRUE) { 
    data_in <- data_in[cause_id != cause_in] 
  }
  return(data_in)
}

## SCRIPT ##################################################################
# Reading in files
rr_draws <- fread(rr_draws_fp)
RO_pairs <- fread(RO_pairs_fp)
rrmax_old <- fread(rrmax_old_fp)
py_exp <- fread(py_exp_fp)
cd_exp <- fread(cd_exp_fp)

# Getting rid of fractures in both rrmax_old and the rr_draws only because they have a different exposure definition. 
fracture_ids <- c(878, 923, 633)
old_fractures <- rrmax_old[cause_id %in% fracture_ids]
draws_fractures <- rr_draws[cause_id %in% fracture_ids]

rrmax_old <- rrmax_old[!(cause_id %in% fracture_ids)]
rr_draws <- rr_draws[!(cause_id %in% fracture_ids)]

# Getting rid of useless data
to_del <- c("location_id", "mortality", "morbidity", "year_id", "rei_id", "modelable_entity_id", "parameter")
rr_draws[, (to_del) := NULL]

# Figuring out units
for(i in unique(RO_pairs$cause_id)){
  units_of_interest <- RO_pairs[cause_id == i, current_exp_units]
  rr_draws[cause_id == i, units := units_of_interest]
}
for(i in unique(RO_pairs$modeled_cause)[which(!(unique(RO_pairs$modeled_cause) %in% unique(RO_pairs$cause_id)))]){
  units_of_interest <- RO_pairs[modeled_cause == i, current_exp_units]
  rr_draws[cause_id == i, units := units_of_interest]
}

dimensions <- unique(rr_draws[, .(age_group_id, sex_id, units, exposure, cause_id)])
# Rounding exposure values
setnames(py_exp, "percentile_95", "exposure")
setnames(cd_exp, "percentile_95", "exposure")
py_exp$units <- "py"
cd_exp$units <- "cig/day"
py_exp$percentile_1 <- NULL
cd_exp$percentile_1 <- NULL

# Rounding to the nearest modeled exposure value
exposure_options_py <- unique(rr_draws[units == "py", exposure])
py_exposures <- unique(py_exp$exposure)

versions <- data.table()
for(i in 1:nrow(py_exp)){
  subset <- py_exp[i]
  exposure_of_interest <- subset$exposure
  age_group_of_interest <- subset$age_group_id
  sex_id_of_interest <- subset$sex_id
  for(m in unique(dimensions[units == "py", cause_id])){
    if(m == 429 & sex_id_of_interest == 1){
      next
    } else if(m == 432 & sex_id_of_interest == 1){
      next
    }
    exposure_options <- dimensions[age_group_id == age_group_of_interest & sex_id == sex_id_of_interest & units == "py" & cause_id == m, exposure]
    diff <- min(abs(exposure_options - exposure_of_interest))
    versions_row <- data.table(exposure_of_interest, exposure_options[which(abs(exposure_options - exposure_of_interest) == diff)], m)
    versions <- rbindlist(list(versions, versions_row), use.names = T)
  }
}
setnames(versions, c("exposure_of_interest", "V2", "m"), c("exposure", "updated_exp", "cause_id"))
py_exp <- merge(py_exp, versions, by = "exposure", all = TRUE)

versions <- c()
for(i in 1:nrow(cd_exp)){
  subset <- cd_exp[i]
  exposure_of_interest <- subset$exposure
  age_group_of_interest <- subset$age_group_id
  sex_id_of_interest <- subset$sex_id
  for(m in unique(dimensions[units == "cig/day", cause_id])){
    if(m == 438 & sex_id_of_interest == 2){
      next
    } 
    exposure_options <- dimensions[age_group_id == age_group_of_interest & sex_id == sex_id_of_interest & units == "cig/day" & cause_id == m, exposure]
    diff <- min(abs(exposure_options - exposure_of_interest))
    versions_row <- data.table(exposure_of_interest, exposure_options[which(abs(exposure_options - exposure_of_interest) == diff)], m)
    versions <- rbindlist(list(versions, versions_row), use.names = T)
  }
}
setnames(versions, c("exposure_of_interest", "V2", "m"), c("exposure", "updated_exp", "cause_id"))
cd_exp <- merge(cd_exp, versions, by = "exposure", all = TRUE)

# Merging together the exposure values
all_exp <- rbindlist(list(py_exp, cd_exp), use.names = TRUE)

setnames(all_exp, c("exposure", "updated_exp"), c("old_exp", "exposure"))

# checking all exposure levels are valid
unique(all_exp[units == "py", exposure])[which(!(unique(all_exp[units == "py", exposure]) %in% unique(rr_draws[units == "py", exposure])))]
unique(all_exp[units == "cig/day", exposure])[which(!(unique(all_exp[units == "cig/day", exposure]) %in% unique(rr_draws[units == "cig/day", exposure])))]

merged_draws <- merge(rr_draws, all_exp, by = c("age_group_id", "sex_id", "exposure", "units", "cause_id"), all.y = TRUE)

unique(merged_draws[, cause_id])[which(!(unique(merged_draws[, cause_id]) %in% unique(rrmax_old[, cause_id])))]

expanded_draws <- expand_causes(data_in = merged_draws, cause_in = 297, causes_out = c(933, 934, 946, 947, 954), drop_orig = T)
expanded_draws <- expand_causes(data_in = expanded_draws, cause_in = 417, causes_out = c(418, 419, 420, 421, 996), drop_orig = T)
expanded_draws <- expand_causes(data_in = expanded_draws, cause_in = 487, causes_out = c(840, 841, 845, 846, 847, 848, 943), drop_orig = T)
expanded_draws <- expand_causes(data_in = expanded_draws, cause_in = 494, causes_out = c(495, 496, 497), drop_orig = T)

melted_draws <- melt(expanded_draws, measure.vars = paste0("draw_", 0:999),
                    variable.name = "draw", value.name = "rr")
setDT(melted_draws)
melted_draws[, draw := sub(".*_", "", draw)]

cleaned_newrrmax <- copy(melted_draws)
cleaned_newrrmax$units <- NULL
cleaned_newrrmax$exposure <- NULL
cleaned_newrrmax$old_exp <- NULL

rrmax_old[, draw := as.integer(draw)]
cleaned_newrrmax[, draw := as.integer(draw)]
setnames(rrmax_old, "rr", "old")

old_version <- copy(rrmax_old)
setnames(old_version, "old", "rr")
old_version$versions <- "old"
cleaned_newrrmax$versions <- "new"

combined_long <- rbindlist(list(old_version, cleaned_newrrmax), use.names = TRUE)

mean_combined <- dcast(combined_long, sex_id + age_group_id + cause_id + versions~ ., fun.agg = function(x) mean(x), value.var = "rr")

setnames(mean_combined, ".", "mean_rrmax")
mean_combined <- dcast(mean_combined, sex_id + age_group_id + cause_id ~ versions, value.var = "mean_rrmax")
mean_combined[, sex := ifelse(sex_id == 1, "male", "female")]
mean_combined <- merge(mean_combined, RO_pairs[, .(cause_name, cause_id)], by = "cause_id", all.x = TRUE)

if(save_plot == TRUE | save_plot == "TRUE"){
  pdf(visual_fp, width=12, height=8)
  print(ggplot(mean_combined, aes(x = new, y = old, color = as.factor(cause_name))) +
          geom_point() +
          labs(title = "Comparing the previous mean RR max values by age-sex to the corresponding GBD 2021 mean RR max", y = "Previous RRmax", x = "Updated RRmax", color = "Cause Name") + 
          facet_wrap(sex ~.))
  for(i in unique(mean_combined$sex)){
    print(ggplot(mean_combined[sex == i], aes(x = new, y = old, color = as.factor(cause_name))) +
            geom_point() + 
            labs(title = paste0("Comparing the previous mean RR max values by age-sex to the corresponding GBD 2021 mean RR max for ", i, "s"), y = "Previous RRmax", x = "Updated RRmax", color = "Cause Name") + 
            facet_wrap(age_group_id ~.))
  }
  dev.off()
}

## Adding fractures
melt_fractures <- melt(draws_fractures, measure.vars = paste0("draw_", 0:999),
                     variable.name = "draw", value.name = "rr")
setDT(melt_fractures)
melt_fractures[, draw := sub(".*_", "", draw)]
melt_fractures <- melt_fractures[parameter == "cat1"]
setnames(old_fractures, "rr", "old")
old_fractures[, draw := as.integer(draw)]
melt_fractures[, draw := as.integer(draw)]
fractures <- merge(old_fractures, melt_fractures[, .(draw, sex_id, age_group_id, rr, cause_id)], by = c("draw", "sex_id", "age_group_id", "cause_id"), all.x = TRUE)

cleaned_newrrmax$versions <- NULL
fractures$old <- NULL

to_save <- rbindlist(list(cleaned_newrrmax, fractures), use.names = TRUE)
fwrite(to_save, paste0(save_fp))



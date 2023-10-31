#
# This script compiles shocks from disasters and wars which are provided at
# the lowest available location levels and aggregates them up to the national level
# so that all locations are at country level and below are present.
#

library(readr)
library(data.table)
library(assertable)
library(haven)

library(mortdb)
library(mortcore)

args <- commandArgs(trailingOnly = T)

new_run_id <- as.numeric(args[1])
gbd_year <- as.numeric((args[2]))

lowest_locs <- setDT(get_locations(level = "lowest", gbd_year = gbd_year))
all_locs <- setDT(get_locations(level = "all"))
age_map <- setDT(get_age_map())

output_folder <- paste0("", new_run_id, "")

disaster_vr_file <- paste0("")
war_vr_file <- paste0("")

id_vars <- c("location_id", "year", "age_group_id", "sex")

disaster_dt <- setDT(read_dta(disaster_vr_file))
war_dt <- setDT(read_dta(war_vr_file))
setnames(war_dt, "war_deaths_best", "numkilled")

combined_dt <- rbindlist(list(disaster_dt[, .SD, .SDcols = c(id_vars, "numkilled")],
                              war_dt[, .SD, .SDcols = c(id_vars, "numkilled")]),
                         use.names = T)
combined_dt <- combined_dt[, .(numkilled = sum(numkilled)), by = id_vars]

## Remove parent locations and re-aggregate deaths from lowest levels
combined_dt <- combined_dt[location_id %in% lowest_locs$location_id, ]

## Make square frame to get squareness prior to agg_results call
agg_frame <- CJ(location_id = lowest_locs$location_id,
                year = unique(combined_dt$year),
                sex = unique(combined_dt$sex),
                age_group_id = unique(combined_dt$age_group_id))
combined_dt <- merge(combined_dt, agg_frame, all.y = T, by = id_vars)
combined_dt[is.na(numkilled), numkilled := 0]
aggregated_dt <- agg_results(combined_dt, id_vars = id_vars, end_agg_level = 3, loc_scalars = F, value_vars = "numkilled")

write_dta(aggregated_dt, paste0(output_folder, ""), version = 13)

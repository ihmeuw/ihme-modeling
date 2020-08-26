# --------------
# Purpose:  The goal is to calculate the country-specific proportion of Rh-incompatible pregnancies
#           that received Rhogam, and then calculate the count of Rh-incompatible pregnancies that
#           didn't receive Rhogam based on that proportion and the NMR of every country-year-sex
# --------------

rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH/j/"
  h <- paste0("FILEPATH", Sys.getenv("USER"),"/")
  my_libs <- paste0(h, "/cluster_packages")
}

library(data.table)
library(magrittr)
library(ggplot2)
library(readstata13)

source("FILEPATH/get_location_metadata.R"  )

working_dir = "FILEPATH"
out_dir = paste0(working_dir, "/01_B_rhogam_adjustment")

locs <- get_location_metadata(location_set_id = 9, gbd_round_id = 5)

# --------------------------
# Step A: Load and clean Rhogam dose data

rhogam_doses <- fread("FILEPATH/rhogam_temp.csv")
setnames(rhogam_doses, "Rh_immunoglobulin", "rhogam_doses")
rhogam_doses <- rhogam_doses[, .(location_name, rhogam_doses)]

rhogam_doses[ location_name == "Gambia", location_name := "The Gambia"]
rhogam_doses[ location_name == "Russia", location_name := "Russian Federation"]

#add the location level and location id columns based on the location name
rhogam_doses <- merge(rhogam_doses, locs, by = 'location_name', all.x = TRUE)

# drop Georgia the state, and only keep Georgia the country b/c we are running at country-level
rhogam_doses <- rhogam_doses[level != 4, ]
rhogam_doses <- rhogam_doses[, .(location_id, rhogam_doses)]


# --------------------------
# Step B. Match RH-Incompatible Pregnancies from script 01_A to Rhogam dose data;

rh_prev <- fread(paste0(working_dir, "/01_A_rh_prev/rh_incompatible_count_all_draws.csv")) # cols: "location_id" "year"        "sex"         "births";  has all the locations needed

#only keep pregnancies from 2010 and for both sexes since that's the only type of Rhogam dose data
nat_allsex_births <- rh_prev[sex == 3 & year == 2010, ]

# merge rhogam doses onto the prev dataset (keeps all locations from nat_allsex_births dataset)
nat_allsex_births <- merge(rhogam_doses, nat_allsex_births, by = "location_id", all = T) 

# apply the Sudan Rhogam proportion to South Sudan as well, since they were one country in 2010
nat_allsex_births[location_id == 435, rhogam_doses := nat_allsex_births[location_id == 522, rhogam_doses]]

nat_allsex_births[, rhogam_doses := as.double(rhogam_doses)]

# If there is no Rhogam data, assume coverage is 1.
max.rhogam_doses <- max(nat_allsex_births$rhogam_doses, na.rm = T)
nat_allsex_births[is.na(rhogam_doses), rhogam_doses := max.rhogam_doses * 100] 

#convert to long dataset with one row per draw
nat_allsex_births <- melt(nat_allsex_births, id.vars = c("location_id", "year", "sex", "births", "rhogam_doses"))

# --------------------------
# Step C. Calculate proportion of RH-Incompatible Pregnancies who receive Rhogam;

# value is the count of Rh incompatible pregnancies
nat_allsex_births[, rhogam_prop_ := rhogam_doses / value]

nat_allsex_births[rhogam_prop_ > 1 , rhogam_prop_ := 1]

nat_allsex_births <- dcast(nat_allsex_births[, -c("value")], formula = location_id + year + sex + births + rhogam_doses ~ paste0("rhogam_prop_", variable), value.var = "rhogam_prop_")
#nas introduced for superregions - do we care? no.

# add columns for location level, name, and ihme_loc_id
locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 6)
nat_allsex_births <- merge(nat_allsex_births, locs[, .(location_id, level, location_name, ihme_loc_id)], by = "location_id")

#only keep national locations
nat_allsex_births <- nat_allsex_births[level == 3, ]

#pare down columns
nat_allsex_births <- nat_allsex_births[, c( "rhogam_doses", "ihme_loc_id", paste0("rhogam_prop_draw_", 0:999))]

# --------------------------
# Step D. Apply Rhogam proportions to the full Rh-incompatible pregnancy counts

# Add ihme_loc_id to the Rh-incompatible prev data. Should get following columns:
# "location_id", "location_name", "year"        "sex"         "births"      "ihme_loc_id"
rh_prev <- merge(rh_prev, locs[, .(location_id, ihme_loc_id)], by = 'location_id') 

rh_prev[, ihme_loc_id := substr(ihme_loc_id, 1, 3)]

rh_prev <- rh_prev[!is.na(draw_1), ]

# melt
rh_prev <- melt(rh_prev, id.vars = c("location_id", "year", "sex", "births", "ihme_loc_id"), variable.name = "draw", value.name = "rh_prev")
rh_prev[, draw := gsub(draw, pattern = "draw_", replacement = "")]

# melt nat_allsex_births
nat_allsex_births <- melt(nat_allsex_births, id.vars = c("ihme_loc_id", "rhogam_doses"), variable.name = "draw", value.name = "rhogam_prop")
nat_allsex_births[, draw := gsub(draw, pattern = "rhogam_prop_draw_", replacement = "")]

# Add the country-specific multipliers from nat_allsex_births to the full prevalence data
rh_prev <- merge(rh_prev, nat_allsex_births, by = c("ihme_loc_id", "draw"), all.x = T) 

# Get NMR data and process. Output: location_id, sex, year, nmr
nmr <- "FILEPATH/qx_results_v2019_most_detailed.dta" %>% read.dta13 %>% data.table
nmr <- nmr[year >= 1980, .(location_id, sex, year, q_nn_med)]
setnames(nmr, "q_nn_med", "nmr")
nmr[, nmr := nmr * 1000]
nmr[sex == "male", sex := "1"][sex == "female", sex := "2"][sex == "both", sex := "3"]
nmr[, sex := as.integer(sex)]

nmr <- merge(nmr, locs, by = 'location_id')
nmr <- nmr[, .(location_id, sex, year, nmr)]

# Add nmr to the prevalence data by location, sex and year
rh_prev <- merge(rh_prev, nmr, by = c("location_id", "sex", "year"))

#drop regions and superregions
rh_prev <- rh_prev[!is.na(rhogam_prop), ]

#If NMR < 5, assume full rhogam coverage, regardless of data
rh_prev[nmr < 5, rhogam_prop := 1]

rh_prev[, rh_prev := rh_prev * (1 - rhogam_prop)]

rh_prev_wide <- dcast(rh_prev, location_id + sex + year + births ~ paste0("draw_", draw), value.var = "rhogam_prop")


# --------------------------
# Save outputs

write.csv(rh_prev, paste0(out_dir, "/rhogam_adjusted_pregnancies_all_draws.csv"), na = "", row.names = F)
save.dta13(rh_prev, paste0(out_dir, "/rhogam_adjusted_pregnancies_all_draws.dta"))

draw_cols <- paste0("draw_", 0:999)
rh_prev$mean <- rowMeans(rh_prev[,draw_cols,with=FALSE], na.rm = T)
rh_prev$lower <- rh_prev[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
rh_prev$upper <- rh_prev[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
rh_prev <- rh_prev[,-draw_cols,with=FALSE]

write.csv(rh_prev, paste0(out_dir, "/rhogam_adjusted_pregnancies_summary.csv"), na = "", row.names = F)

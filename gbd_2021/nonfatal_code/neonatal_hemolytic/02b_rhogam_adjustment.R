# --------------
# Title:    02_B_Rhogam_Adjustment.R (re-write of the stata version)
# Date:     2018-04-16
# Purpose:  The goal is to calculate the country-specific proportion of Rh-incompatible pregnancies
#           that received Rhogam, and then calculate the count of Rh-incompatible pregnancies that
#           didn't receive Rhogam based on that proportion and the NMR of every country-year-sex
# Requirements: Run on newest version of R
# Last Update:  2019-01-08
# --------------

rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "PATHNAME"
  h <-"PATHNAME"
  my_libs <- "PATHNAME"
} else {
  j<- "PATHNAME"
  h<-"PATHNAME"
  my_libs <- "PATHNAME"
}

library(data.table)
library(magrittr)
library(ggplot2)
library(readstata13)

source("PATHNAME/get_location_metadata.R"  )
source("PATHNAME/get_bundle_data.R"  )
source("PATHNAME/find_differences_in_groups.R")
source("PATHNAME/find_non_draw_cols.R")
source("PATHNAME/merge_on_location_metadata.R")

working_dir = "PATHNAME"

locs <- get_location_metadata(location_set_id = 9, gbd_round_id = 7)

# --------------------------
# Step A: Load Rhogam dose data

rhogam_doses <- get_bundle_data(bundle_id = 4715, gbd_round_id = 6, decomp_step = 'step4')

setnames(rhogam_doses, "cases", "rhogam_doses")
rhogam_doses <- rhogam_doses[, .(location_id, rhogam_doses)]


# --------------------------
# Step B. Match RH-Incompatible Pregnancies from script 01_A to Rhogam dose data;

#I have 6000 NAs in this rh_prev dataset. Why? There should be prev data from 01_A script
#rh_prev <- paste0(working_dir, "PATHNAME/rh_incompatible_count_all_draws.dta") %>% read.dta13 %>% data.table # cols: "location_id" "year"        "sex"         "births";  has all the locations needed
rh_prev <- fread("PATHNAME") # cols: "location_id" "year"        "sex"         "births";  has all the locations needed

#only keep pregnancies from 2010 and for both sexes since that's the only type of Rhogam dose data
nat_allsex_births <- rh_prev[sex == 3 & year == 2010, ]

# merge rhogam doses onto the prev dataset (keeps all locations from nat_allsex_births dataset)
# should result in columns: "location_id"  "rhogam_doses" "year"         "sex"          "births" , 899 rows 
nat_allsex_births <- merge(rhogam_doses, nat_allsex_births, by = "location_id", all = T) 

# apply the Sudan Rhogam proportion to South Sudan as well, since they were one country in 2010
nat_allsex_births[location_id == 435, rhogam_doses := nat_allsex_births[location_id == 522, rhogam_doses]]

nat_allsex_births[, rhogam_doses := as.double(rhogam_doses)]

# If there is no Rhogam data, assume coverage is 1.
# at this point, rhogam_doses is NA for any loc where we didn't have Rhogam data
# Replace NAs with implausibly large doses so that rhogam_prop_ later turns into 1 
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
locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
nat_allsex_births <- merge(nat_allsex_births, locs[, .(location_id, level, location_name, ihme_loc_id)], by = "location_id")

#only keep national locations
nat_allsex_births <- nat_allsex_births[level == 3, ]

# for some mysterious reason, in this loc set and version, england is missing its ihme_loc_id, so fill it in here
#nat_allsex_births <- nat_allsex_births[location_name %in% c("England"), ihme_loc_id := "ENG"] # "location_id"   "year"          "sex"           "births"        "rhogam_doses"  "level"         "location_name" "ihme_loc_id"  

# drop Northern Ireland, Scotland, Wales
#nat_allsex_births <- nat_allsex_births[ihme_loc_id != "", c( "rhogam_doses", "ihme_loc_id", paste0("rhogam_prop_draw_", 0:999))]
#pare down columns
nat_allsex_births <- nat_allsex_births[, c( "rhogam_doses", "ihme_loc_id", paste0("rhogam_prop_draw_", 0:999))]


# --------------------------
# Step D. Apply Rhogam proportions to the full Rh-incompatible pregnancy counts

# Add ihme_loc_id to the Rh-incompatible prev data. Should get following columns:
# "location_id", "location_name", "year"        "sex"         "births"      "ihme_loc_id"
rh_prev <- merge(rh_prev, locs[, .(location_id, ihme_loc_id)], by = 'location_id') 

# should make this matching of country-level multipliers to their
# respective subnats more robust in future
# rh_prev[, country_id := location_id]
# rh_prev[level > 3, country_id := parent_id]
# rh_prev[, country_name := location_name]
# rh_prev[ level > 3, country_name := parent_name]

rh_prev[, ihme_loc_id := substr(ihme_loc_id, 1, 3)]
#rh_prev[ihme_loc_id == "", ihme_loc_id := "ENG"]

#this line drops some of the super regions
rh_prev <- rh_prev[!is.na(draw_1), ]

# melt
rh_prev <- melt(rh_prev, id.vars = c("location_id", "year", "sex", "births", "ihme_loc_id"), variable.name = "draw", value.name = "rh_prev")
# this line takes ages to run. replaces the "draw_0" with "0" in each row
rh_prev[, draw := gsub(draw, pattern = "draw_", replacement = "")]

# melt nat_allsex_births
nat_allsex_births <- melt(nat_allsex_births, id.vars = c("ihme_loc_id", "rhogam_doses"), variable.name = "draw", value.name = "rhogam_prop")
nat_allsex_births[, draw := gsub(draw, pattern = "rhogam_prop_draw_", replacement = "")]

# Add the country-specific multipliers from nat_allsex_births to the full prevalence data
# "ihme_loc_id"   "location_id"   "year"          "sex"           "births"        "rhogam_doses"  "level"         "location_name"
rh_prev <- merge(rh_prev, nat_allsex_births, by = c("ihme_loc_id", "draw"), all.x = T) 
#at this point, the same rhogram proportion is associated with each India subnat as with the India nat loc (b/c matched on ihme_loc_id)

# Get NMR data and process. Output: location_id, sex, year, nmr
#nmr <- PATHNAME %>% read.dta13 %>% data.table
#nmr <- fread("PATHNAME")
#nmr <- fread("PATHNAME")
nmr <- "PATHNAME" %>% read.dta13 %>% data.table
nmr <- nmr[year >= 1980, .(location_id, sex, year, q_nn_med)]
setnames(nmr, "q_nn_med", "nmr")
nmr[, nmr := nmr * 1000]
nmr[sex == "male", sex := "1"][sex == "female", sex := "2"][sex == "both", sex := "3"]
nmr[, sex := as.integer(sex)]
#nmr[, year := year - 0.5]

nmr <- merge(nmr, locs, by = 'location_id')
nmr <- nmr[, .(location_id, sex, year, nmr)]

# Add nmr to the prevalence data by location, sex and year
# This drops US/CHN/BRA/IND, but the proportions remain on their subnats
rh_prev <- merge(rh_prev, nmr, by = c("location_id", "sex", "year"))

#drop regions and superregions
rh_prev <- rh_prev[!is.na(rhogam_prop), ]

#If NMR < 5, assume full rhogam coverage, regardless of data
rh_prev[nmr < 5, rhogam_prop := 1]

# draw_cols <- paste0("draw_", 0:999)
# rh_prev_wide$mean <- rowMeans(rh_prev_wide[,draw_cols,with=FALSE], na.rm = T)
# rh_prev_wide$lower <- rh_prev_wide[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
# rh_prev_wide$upper <- rh_prev_wide[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
# rh_prev_wide <- rh_prev_wide[,-draw_cols,with=FALSE]
# write.csv(rh_prev_wide, "PATHNAME/rhogam_coverage_summary.csv", na = "", row.names = F)

rh_prev[, rh_prev := rh_prev * (1 - rhogam_prop)]

rh_prev_wide <- dcast(rh_prev, location_id + sex + year + births ~ paste0("draw_", draw), value.var = "rh_prev")


# --------------------------
# Save outputs

write.csv(rh_prev_wide, paste0(working_dir, "/rhogam_adjusted_pregnancies_all_draws.csv"), na = "", row.names = F)
save.dta13(rh_prev_wide, paste0(working_dir, "/rhogam_adjusted_pregnancies_all_draws.dta"))

draw_cols <- paste0("draw_", 0:999)
rh_prev_wide$mean <- rowMeans(rh_prev_wide[,draw_cols,with=FALSE], na.rm = T)
rh_prev_wide$lower <- rh_prev_wide[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
rh_prev_wide$upper <- rh_prev_wide[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
rh_prev_wide <- rh_prev_wide[,-draw_cols,with=FALSE]

write.csv(rh_prev_wide, paste0(working_dir, "/rhogam_adjusted_pregnancies_summary.csv"), na = "", row.names = F)

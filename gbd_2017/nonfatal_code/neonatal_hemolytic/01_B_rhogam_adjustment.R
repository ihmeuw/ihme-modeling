
rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "J:"
  h <-"H:"
  my_libs <- paste0(h, "/local_packages")
} else {
  j<- "/home/j"
  h<-"/homes/hpm7"
  my_libs <- paste0(h, "/cluster_packages")
}

library(data.table)
library(magrittr)
library(ggplot2)
library(readstata13)



working_dir = "FILEPATH"
out_dir = paste0(working_dir, "/01_B_rhogam_adjustment")

locs <- get_location_metadata(location_set_id = 9, gbd_round_id = 5)


# --------------------------
# Step 1: Get NMR data and process. 

nmr <- "FILEPATH" %>% read.dta13 %>% data.table
nmr <- nmr[year >= 1980, .(location_id, sex, year, q_nn_med)]
setnames(nmr, "q_nn_med", "nmr")
nmr[, nmr := nmr * 1000]
nmr[sex == "male", sex := "1"][sex == "female", sex := "2"][sex == "both", sex := "3"]
nmr[, sex := as.integer(sex)]
nmr[, year := year - 0.5]

nmr <- merge(nmr, locs)
nmr <- nmr[, .(location_id, sex, year, nmr)]


# --------------------------
# Step 2: Get Rhogam doses

rhogam_doses <- fread("FILEPATH")
setnames(rhogam_doses, "Rh_immunoglobulin", "rhogam_doses")
rhogam_doses <- rhogam_doses[, .(location_name, rhogam_doses)]

rhogam_doses[ location_name == " Moldova", location_name := "Moldova"]
rhogam_doses[ location_name == "Congo ", location_name := "Congo"]
rhogam_doses[ location_name == "Gambia", location_name := "The Gambia"]
rhogam_doses[ location_name == "Russia", location_name := "Russian Federation"]

rhogam_doses <- merge(rhogam_doses, locs)

rhogam_doses <- rhogam_doses[level != 4, ]

rhogam_doses <- rhogam_doses[, .(location_id, rhogam_doses)]


# --------------------------
# RH-Incompatible Preganncies from part A;


rh_prev <- paste0(working_dir, "/FILEPATH/rh_incompatible_count_all_draws.dta") %>% read.dta13 %>% data.table # cols: "location_id" "year"        "sex"         "births";  has all the locations needed

nat_allsex_births <- rh_prev[sex == 3 & year ==2010, ] 

nat_allsex_births <- merge(rhogam_doses, nat_allsex_births, by = "location_id", all = T) # "location_id"  "rhogam_doses" "year"         "sex"          "births" , 899 rows 

nat_allsex_births[, rhogam_doses := as.double(rhogam_doses)]

max.rhogam_doses <- max(nat_allsex_births$rhogam_doses, na.rm = T)

nat_allsex_births[is.na(rhogam_doses), rhogam_doses := max.rhogam_doses * 100]

nat_allsex_births <- melt(nat_allsex_births, id.vars = c("location_id", "year", "sex", "births", "rhogam_doses"))

nat_allsex_births[, rhogam_prop_ := rhogam_doses / value]

nat_allsex_births[rhogam_prop_ > 1 , rhogam_prop_ := 1]

nat_allsex_births <- dcast(nat_allsex_births[, -c("value")], formula = location_id + year + sex + births + rhogam_doses ~ paste0("rhogam_prop_", variable), value.var = "rhogam_prop_")



nat_allsex_births <- merge(nat_allsex_births, locs[, .(location_id, level, location_name, ihme_loc_id)], by = "location_id")

nat_allsex_births <- nat_allsex_births[level == 3, ]

nat_allsex_births <- nat_allsex_births[location_name %in% c("England"), ihme_loc_id := "ENG"] # "location_id"   "year"          "sex"           "births"        "rhogam_doses"  "level"         "location_name" "ihme_loc_id"  

nat_allsex_births <- nat_allsex_births[ihme_loc_id != "", c( "rhogam_doses", "ihme_loc_id", paste0("rhogam_prop_draw_", 0:999))]

# --------------------------
# Merge proportion dataset onto the original

rh_prev <- merge(rh_prev, locs[, list(ihme_loc_id, location_id)]) # "location_id", "location_name", "year"        "sex"         "births"      "ihme_loc_id"

rh_prev[, ihme_loc_id := substr(ihme_loc_id, 1, 3)]

rh_prev[ihme_loc_id == "", ihme_loc_id := "ENG"]

rh_prev <- rh_prev[!is.na(draw_1), ]

# melt
rh_prev <- melt(rh_prev, id.vars = c("location_id", "year", "sex", "births", "ihme_loc_id"), variable.name = "draw", value.name = "rh_prev")
rh_prev[, draw := gsub(draw, pattern = "draw_", replacement = "")]


# melt nat_allsex_births
nat_allsex_births <- melt(nat_allsex_births, id.vars = c("ihme_loc_id", "rhogam_doses"), variable.name = "draw", value.name = "rhogam_prop")
nat_allsex_births[, draw := gsub(draw, pattern = "rhogam_prop_draw_", replacement = "")]

rh_prev <- merge(rh_prev, nat_allsex_births, by = c("ihme_loc_id", "draw"), all.x = T) # "ihme_loc_id"   "location_id"   "year"          "sex"           "births"        "rhogam_doses"  "level"         "location_name"

rh_prev <- merge(rh_prev, nmr, by = c("location_id", "sex", "year"))

rh_prev <- rh_prev[!is.na(rhogam_prop), ]

rh_prev[nmr < 5, rhogam_prop := 1]

rh_prev[, rh_prev := rh_prev * (1 - rhogam_prop)]

rh_prev <- dcast(rh_prev, location_id + sex + year + births ~ paste0("draw_", draw), value.var = "rh_prev")


write.csv(rh_prev, paste0(out_dir, "/rhogam_adjusted_pregnancies_all_draws.csv"), na = "", row.names = F)

save.dta13(rh_prev, paste0(out_dir, "/rhogam_adjusted_pregnancies_all_draws.dta"))


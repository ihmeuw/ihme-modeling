################################################################################
# Description: In certain years singapore census data is only given for the
#              resident population, but we have death counts available for both
#              resident and non-resident populations. Here we use the relative
#              age pattern of mortality between total and resident population to
#              generate a population scalar.
################################################################################

library(data.table)
library(readr)

rm(list=ls())

idcols <- c("location_id", "year_id", "age_group_id", "sex_id", "resident_status")

### RES/NONRES - SINGAPORE SCALE UP RES POP TO TOTAL POP USING RELATIVE AGE PATTENR MORTALITY FROM COD
file.copy(from = "FILEPATH",
          to = "FILEPATH")
sng_deaths <- fread("FILEPATH")
sng_deaths  <- melt(sng_deaths, measure = patterns("resident|total"), variable.name = "resident_status", value.name = "deaths")
sng_deaths[sex_id == 9, sex_id := 4]

# subset unknown deaths into different categories
unknown <- sng_deaths[age_group_id == 283 | sex_id == 4]
unknown_age <- sng_deaths[age_group_id == 283 & sex_id != 4][, age_group_id := NULL]
unknown_sex <- sng_deaths[age_group_id != 283 & sex_id == 4][, sex_id := NULL]
unknown_age_sex <- sng_deaths[age_group_id == 283 & sex_id == 4][, c("age_group_id", "sex_id") := NULL]

known <- sng_deaths[!(age_group_id == 283 | sex_id == 4)]

# calculate proportion of deaths for each category among deaths of known age and sex
known[, ageprop_bysex := deaths/sum(deaths), by = c("year_id", "sex_id", "resident_status")]
known[, sexprop_byage := deaths/sum(deaths), by = c("year_id", "age_group_id", "resident_status")]
known[, agesexprop_bytotal := deaths/sum(deaths), by = c("year_id", "resident_status")]
props <- unique(known[, c(idcols, "ageprop_bysex", "sexprop_byage", "agesexprop_bytotal"), with = F])
known[, c("ageprop_bysex", "sexprop_byage", "agesexprop_bytotal") := NULL]

# merge on proportions
unknown_age <- merge(unknown_age, props[, c(idcols, "ageprop_bysex"), with = F], by = setdiff(idcols, "age_group_id"), all = T, allow.cartesian = T)
unknown_sex <- merge(unknown_sex, props[, c(idcols, "sexprop_byage"), with = F], by = setdiff(idcols, "sex_id"), all = T, allow.cartesian = T)
unknown_age_sex <- merge(unknown_age_sex, props[, c(idcols, "agesexprop_bytotal"), with = F], by = setdiff(idcols, c("sex_id", "age_group_id")), all = T, allow.cartesian = T)

# apply proportions
unknown_age[, age_dist_deaths := deaths * ageprop_bysex][, c("deaths", "ageprop_bysex") := NULL]
unknown_sex[, sex_dist_deaths := deaths * sexprop_byage][, c("deaths", "sexprop_byage") := NULL]
unknown_age_sex[, agesex_dist_deaths := deaths * agesexprop_bytotal][, c("deaths", "agesexprop_bytotal") := NULL]

# account for zeroes
unknown_age[is.na(age_dist_deaths), age_dist_deaths := 0]
unknown_sex[is.na(sex_dist_deaths), sex_dist_deaths := 0]
unknown_age_sex[is.na(agesex_dist_deaths), agesex_dist_deaths := 0]

# combine together known and unknown distributed deaths
known <- Reduce(function(x, y) merge(x, y, by = idcols), list(known, unknown_age, unknown_sex, unknown_age_sex))
known <- melt(known, measure = patterns("deaths"), variable.name = "death_type", value.name = "deaths")
known <- known[, list(deaths = sum(deaths)), by = idcols]

readr::write_csv(known, "FILEPATH")

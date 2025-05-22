rm(list=ls())

library("dplyr")
library("data.table")
library("feather")

# set up directories

source(paste0("FILEPATH/get_outputs.R"))
source(paste0("FILEPATH/get_draws.R"))
source(paste0("FILEPATH/get_location_metadata.R"))
source(paste0("FILEPATH/get_population.R"))
source(paste0("FILEPATH/get_cause_metadata.R"))

### GET LIST OF INCLUDED CAUSES ###
arg <- commandArgs(trailingOnly = T)  #[-(1:5)]  # First args are for unix use only
causeList <- arg[1]
yearList <- arg[2]
version <- arg[3]
popType <- arg[4]
tempZone <- arg[5]
rrDir <- arg[6]
tmrelDir <- arg[7]
description <- arg[8]
outdir <- arg[9]
job.name <- arg[10]


if (length(arg) == 0) {
  causeList <- "diabetes"
  yearList <- 2010
  version <- "allYears_annualPop_20201221"
  popType <- "annual"
  tempZone <- 10
  rrDir <- "FILEPATH"
  tmrelDir <- "FILEPATH"
  description <- "2021_0.05-0.95_gbd_20230208"
}

for (a in c("causeList", "yearList", "version", "popType", "tempZone", "rrDir", "description", "outdir", "job.name")) {
  message(paste0(a, ": ", eval(parse(text = a))))
}

release_id <- 16
rrMaxPctile <- 0.95
tmreldir <- tmrelDir

### GET LOCATION METADATA ###
locMeta <- get_location_metadata(location_set_id = 35, release_id = release_id)

locs <- locMeta[most_detailed == 1, location_id]

limits <- fread(paste0(tmreldir, "/limits.csv"))
limits[, `:=` (min = min*10, max = max*10)]

### IMPORT RR DRAWS FROM MR-BRT MODELS ###
rr <- do.call(rbind, lapply(causeList, function(acause) {
  cbind(fread(paste0(rrDir, "/", acause, "/", acause, "_curve_samples.csv"))[annual_temperature == tempZone & acause %in% causeList, ], acause)}))

setnames(rr, "annual_temperature", "meanTempCat")

rr <- merge(rr, limits, by = "meanTempCat", all.x = T)
rr <- rr[daily_temperature >= min/10 & daily_temperature <= max/10, ]
rr[, dailyTempCat := as.integer(round(daily_temperature * 10))][, daily_temperature := NULL]
rr[, meanTempCat := NULL]

rr <- melt.data.table(rr, id.vars = c("dailyTempCat", "acause"), measure.vars = paste0("draw_", 0:999), value.name = "rr", variable.name = "draw")
rr[, draw := as.integer(gsub("draw_", "", as.character(draw)))]

rr[, rr := exp(rr)]

### IMPORT TMRELS ###
tmrel <- rbindlist(lapply(locs, function(loc_id) {
  fread(paste0(tmreldir, "/tmrel_", loc_id, ".csv"))[meanTempCat == tempZone, ][, location_id := loc_id]}), use.names = T)

tmrel <- melt.data.table(tmrel, id.vars = c("location_id", "year_id"),
                         measure.vars = grep("tmrel_", names(tmrel), value = T),
                         value.name = "dailyTempCat", variable.name = "draw")
tmrel[, dailyTempCat := as.integer(round(dailyTempCat*10))]
tmrel[, draw  := as.integer(gsub("tmrel_", "", as.character(draw)))]

rr <- rr[draw <= max(tmrel$draw), ]

rrsByYear <- function(y) {
  ### MERGE RRs & TMRELs, and adjust RRs ###
  tmrelTmp <- copy(tmrel)[year_id == y, ][, year_id := NULL]

  rrRef <- merge(rr, tmrelTmp, by = c("dailyTempCat", "draw"), all = F)[, .(dailyTempCat, draw, acause, rr, location_id)]
  setnames(rrRef, c("rr", "dailyTempCat"), c("rrRef", "tmrel"))

  temps <- rbindlist(lapply(locs, function(loc) {fread(paste0("FILEPATH/melt_", loc, "_", y, ".csv"))[temp_zone == tempZone, ][, location_id := loc]}), use.names = T)
  setnames(temps, 'temp_zone', 'meanTempCat')
  setnames(temps, 'daily_temp_cat', 'dailyTempCat')

  temps <- merge(temps, limits, by = "meanTempCat", all.x = T)
  temps[dailyTempCat < min, dailyTempCat := min][dailyTempCat > max, dailyTempCat := max]
  temps <- temps[, lapply(.SD, sum), by = c("dailyTempCat", "location_id", "draw"), .SDcols = "pop"]

  setnames(temps, "pop", "population")

  master <- merge(temps, rr, by = c("dailyTempCat", "draw"), all.x = T)
  master <- master[is.na(rr) == F, ]

  master <- merge(master, rrRef, by = c("draw", "acause", "location_id"), all.x = T)

  master[, rr := rr/rrRef]
  master[dailyTempCat < tmrel, risk := "cold"][dailyTempCat > tmrel, risk := "heat"]
  master[, c("tmrel", "rrRef") := NULL]

  master[, rr := as.integer(round(rr*1000))]
  master <- master[, lapply(.SD, sum), by = c("draw", "acause", "rr", "risk"), .SDcols = "population"]
  master[, rr := rr/1000]

  setnames(master, "rr", "rrMax")

  return(master)
}

master <- rbindlist(lapply(yearList, rrsByYear))

write.csv(master, file = paste0(outdir, job.name, ".csv"), row.names = F)
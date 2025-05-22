rm(list=ls())

library("data.table")
library("matrixStats")

source("FILEPATH/get_outputs.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/csv2objects.R")

# set demographics for which to build redistribution table
arg <- commandArgs(trailingOnly = T)
warning(paste("Args are", arg))

loc_id <- as.integer(arg[1])
  warning(paste("loc_id =", loc_id))
cause_list <- arg[2]
  warning(paste("cause_list =", cause_list))
acause_list <- arg[3]
  warning(paste("acause_list =", acause_list))
outdir <- arg[4]
  warning(paste("outdir =", outdir))
indir <- arg[5]
  warning(paste("indir =", indir))
tmrel_min <- as.numeric(arg[6])
  warning(paste("tmrel_min =", tmrel_min))
tmrel_max <- as.numeric(arg[7])
  warning(paste("tmrel_max =", tmrel_max))
year_list <- arg[8]
  warning(paste("year_list =", year_list))
config.file <- arg[9]
  warning(paste("config.file =", config.file))
saveGlobalToCC <- arg[10]
  warning(paste("saveGlobalToCC =", saveGlobalToCC))
codcorrect_version <- arg[11]
  warning(paste("codcorrect_version =", codcorrect_version))
job.name <- arg[12]
  warning(paste("job.name =", job.name))


cause_list <- as.integer(strsplit(cause_list, split = ",")[[1]])
acause_list <- strsplit(acause_list, split = ",")[[1]]
cause_link <- data.table(cause_id = cause_list, acause = acause_list)
year_list <- as.integer(strsplit(year_list, split = ",")[[1]])

warning("All arguments read and processed")

debug <- F

if (debug == T) {
  #gbd_round_id <- 7
  #codcorrect_step <- 'iterative'  #'step5'
  codcorrect_version <- 415
  release <- 16
  config.file <- "FILEPATH"

  csv2objects(config.file, exclude = ls())
  warning("csv converted to objects")

  cause_meta <- get_cause_metadata(cause_set_id = 4, release_id = release)
  cause_meta <- cause_meta[acause %in% causeList & level == 3, ][, .(cause_id, cause_name, acause)]

  cause_list <- cause_meta$cause_id
  acause_list <- cause_meta$acause
  cause_link <- data.table(cause_id = cause_list, acause = acause_list)

  year_list <- 1990:2023
  loc_id <- 161
  tmrel_min <- 6.6
  tmrel_max <- 34.6
  lTrim <- 0.05
  uTrim <- 0.95

} else {
  csv2objects(config.file, exclude = ls())
  warning("csv converted to objects")

  save.image(file = paste0(outdir, "/startingImage_", job.name, ".RData"))
  warning("Image saved")
}

### IMPORT COD ESTIMATES FOR SELECTED CAUSES ###

if (cod_source == ("get_draws")) {
  if (codcorrect_version == 'best' | is.na(codcorrect_version)) {
    cod <- get_draws("cause_id", cause_list, location_id = loc_id, metric_id = 1, measure_id = 1, age_group_id = 22, sex_id = 3, year_id = year_list,
                    source = "codcorrect", release_id = release)

  } else {
    cod <- get_draws("cause_id", cause_list, location_id = loc_id, metric_id = 1, measure_id = 1, age_group_id = 22, sex_id = 3, year_id = year_list,
                     source = "codcorrect", release_id = release, version = codcorrect_version)
  }

  cod <- cod[, c("age_group_id", "sex_id", "measure_id", "metric_id") := NULL]
  warning("CoD data loaded")

} else {
  cod <- fread(cod_source)[location_id == loc_id & year_id %in% year_list & acause %in% acause_list, ]
  cod <- merge(cod, cause_link, by = "acause", all.x = T)

  warning("CoD data loaded")
}

max_draw <- length(grep("draw_", names(cod))) - 1

# Convert deaths to weights
cod[, paste0("weight_", 0:max_draw) := lapply(.SD, function(x) {x/sum(x, na.rm = T)}), by = .(location_id, year_id), .SDcols = paste0("draw_", 0:max_draw)]
cod[, paste0("draw_", 0:max_draw) := NULL]

warning("CoD data converted to weights")

### IMPORT RR DRAWS FROM MR-BRT MODELS ###
rr <- do.call(rbind, lapply(acause_list, function(acause) {
  cbind(fread(paste0(indir, "/", acause, "/", acause, "_curve_samples.csv")), acause)}))

setnames(rr, "annual_temperature", "meanTempCat")
setnames(rr, "daily_temperature", "dailyTempCat")
rr <- rr[, .SD, .SDcols = c("meanTempCat", "dailyTempCat", "acause", paste0("draw_", 0:max_draw))]

warning("RR data loaded")

startSize <- nrow(rr)
rr <- rr[dailyTempCat >= tmrel_min & dailyTempCat <= tmrel_max & meanTempCat >= 6, ]
endSize <- nrow(rr)

warning(paste0("RR data trimmed to temp limits, from ", startSize, " to ", endSize))

limits <- read.csv(file.path(outdir, "limits.csv"))
setDT(limits)
rr <- merge(rr, limits, by = "meanTempCat", all.x = T)
rr <- rr[dailyTempCat >= min & dailyTempCat <= max,]

rr[, paste0("rr_", 0:max_draw) := lapply(.SD, exp), .SDcols = c(paste0("draw_", 0:max_draw))]
rr[, "rrMean" := apply(.SD, 1, mean), .SDcols = paste0("draw_", 0:max_draw)]

## clean up (drop unnecessary varibles and rename others)
rr[, c(paste0("draw_", 0:max_draw)) := NULL]
rr[, meanTempCat := as.integer(meanTempCat)]
rr[, acause := as.character(acause)]
rr[, dailyTempCat := as.integer(round(dailyTempCat*10))]

# merge in cause ids
rr <- merge(rr, cause_link, by = "acause", all.x = T)

warning("RRs merged to cause ids")

### MERGE COD & RR DATA ###
master <- merge(cod, rr, by = "cause_id", all = T, allow.cartesian = T)
warning("RRs merged to CoD Data")


### CALCULATE DEATH_WEIGHTED MEAN RRs ###
master[, id := .GRP, by = .(location_id, year_id, dailyTempCat, meanTempCat)]
setkey(master, id)

ids <- unique(master[, .(id, location_id, year_id, dailyTempCat, meanTempCat)])

rrWt <- master[, lapply(0:max_draw, function(x) {sum(get(paste0("rr_", x)) * get(paste0("weight_", x)))}), by = .(id)]
setnames(rrWt, paste0("V", (0:max_draw) + 1), paste0("rr_", 0:max_draw))

rrWt <- merge(ids, rrWt, by = "id", all = T)
setkey(rrWt, id)

rrWt[, dailyTempCat := as.numeric(dailyTempCat/10)]

warning("Death weighted RRs calculated")

tmrel <- rrWt[, lapply(.SD, function(x) {sum(dailyTempCat*(x == min(x)))}), by = c("location_id", "year_id", "meanTempCat"), .SDcols = paste0("rr_", 0:max_draw)]
names(tmrel) <- sub("rr_", "tmrel_", names(tmrel))
tmrel <- tmrel[is.na(location_id) == F]

warning("TMRELs calculated")

write.csv(tmrel, file = paste0(outdir, "/", job.name, ".csv"), row.names = F)

if (saveGlobalToCC == T & loc_id == 1) {
  write.csv(tmrel, file = paste0("FILEPATH", job.name, ".csv"), row.names = F)
}

warning("Draw file saved")

tmrel[, "tmrelMean" := apply(.SD, 1, mean), .SDcols=paste0("tmrel_", 0:max_draw)]
tmrel[, "tmrelLower" := apply(.SD, 1, quantile, c(.025)), .SDcols=paste0("tmrel_", 0:max_draw)]
tmrel[, "tmrelUpper" := apply(.SD, 1, quantile, c(.975)), .SDcols=paste0("tmrel_", 0:max_draw)]
tmrel[, paste0("tmrel_", 0:max_draw) := NULL]

write.csv(tmrel, file = paste0(outdir, "/", job.name, "_summaries.csv"), row.names = F)
warning("Summary file saved")

write("Complete", paste0(outdir, "/", job.name, "_complete.txt"))
rm(list=ls())

library("tidyverse")
library("data.table")
library("RODBC")
library("matrixStats")


source("FILEPATH/get_outputs.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_cause_metadata.R")


# set demographics for which to build redistribution table
arg <- commandArgs()[-(1:5)]  # First args are for unix use only
loc_id <- as.integer(arg[1])
cause_list <- arg[2]
acause_list <- arg[3]
outdir <- arg[4]
indir <- arg[5]
tmrel_min <- as.numeric(arg[6])
tmrel_max <- as.numeric(arg[7])

cause_list <- as.integer(strsplit(cause_list, split = ",")[[1]])
acause_list <- strsplit(acause_list, split = ",")[[1]]
cause_link <- data.table(cause_id = cause_list, acause = acause_list)

save.image(file = paste0(outdir, "/startingImage_", loc_id, ".RData"))

### IMPORT COD ESTIMATES FOR SELECTED CAUSES ###
cod <- get_draws("cause_id", cause_list, location_id = loc_id, metric_id = 1, measure_id = 1, age_group_id = 22, sex_id = 3,
                 year_id = 1990:2019, source="codcorrect", status="latest", gbd_round_id=6, decomp_step = "step4")


cod <- cod[, c("age_group_id", "sex_id", "measure_id", "metric_id") := NULL]
warning("CoD data loaded")



# Convert deaths to weights
cod[, paste0("weight_", 0:999) := lapply(.SD, function(x) {x/sum(x, na.rm = T)}), by = .(location_id, year_id), .SDcols = paste0("draw_", 0:999)]
cod[, paste0("draw_", 0:999) := NULL]


warning("CoD data converted to weights")

### IMPORT RR DRAWS FROM MR-BRT MODELS ###
rr <- do.call(rbind, lapply(acause_list, function(acause) {
  cbind(fread(paste0(FILEPATH, "/", acause, "/", acause, "_curve_samples.csv")), acause)}))


setnames(rr, "annual_temperature", "meanTempCat")
setnames(rr, "daily_temperature", "dailyTempCat")
warning("RR data loaded")

startSize <- nrow(rr)
rr <- rr[dailyTempCat>=tmrel_min & dailyTempCat<=tmrel_max & meanTempCat>=6, ]
endSize <- nrow(rr)

warning(paste0("RR data trimmed to temp limits, from ", startSize, " to ", endSize))


rr[, "rrMean" := apply(.SD, 1, mean), .SDcols=paste0("draw_", 0:999)]

## clean up (rename variables)
setnames(rr, old = c(paste0("draw_", 0:999)), new = c(paste0("rr_", 0:999)))
rr[, meanTempCat := as.integer(meanTempCat)]
rr[, acause := as.character(acause)]

# merge in cause ids
rr <- merge(rr, cause_link, by = "acause", all.x = T)
warning("RRs merged to cause ids")


### MERGE COD & RR DATA ###
master <- merge(cod, rr, by = "cause_id", all = TRUE, allow.cartesian = TRUE)
warning("RRs merged to CoD Data")




### CALCULATE DEATH-WEIGHTED MEAN RRs ###
master[, id := .GRP, by = .(location_id, year_id, dailyTempCat, meanTempCat)]
setkey(master, id)

ids <- unique(master[, .(id, location_id, year_id, dailyTempCat, meanTempCat)])

rrWt <- master[, lapply(0:999, function(x) {sum(get(paste0("rr_", x)) * get(paste0("weight_", x)))}), by = .(id)]
setnames(rrWt, paste0("V", 1:1000), paste0("rr_", 0:999))


rrWt <- merge(ids, rrWt, by = "id", all = TRUE)
setkey(rrWt, id)

warning("Death weighted RRs calculated")


tmrel <- rrWt[, lapply(.SD, function(x) {sum(dailyTempCat*(x==min(x)))}), by = .(location_id, year_id, meanTempCat), .SDcols = paste0("rr_", 0:999)]
names(tmrel) <- sub("rr_", "tmrel_", names(tmrel))

warning("TMRELs calculated")


write.csv(tmrel, file = paste0(outdir, "/tmrel_", loc_id, ".csv"), row.names = F)
warning("Draw file saved")


tmrel[, "tmrelMean" := apply(.SD, 1, mean), .SDcols=paste0("tmrel_", 0:999)]
tmrel[, "tmrelLower" := apply(.SD, 1, quantile, c(.025)), .SDcols=paste0("tmrel_", 0:999)]
tmrel[, "tmrelUpper" := apply(.SD, 1, quantile, c(.975)), .SDcols=paste0("tmrel_", 0:999)]
tmrel[, paste0("tmrel_", 0:999) := NULL]

write.csv(tmrel, file = paste0(outdir, "/tmrel_", loc_id, "_summaries.csv"), row.names = F)
warning("Summary file saved")


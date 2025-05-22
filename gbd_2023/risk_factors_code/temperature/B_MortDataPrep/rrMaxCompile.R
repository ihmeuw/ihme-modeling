rm(list=ls())

library("dplyr")
library("data.table")
library("feather")

### GET LIST OF INCLUDED CAUSES ###
arg <- commandArgs(trailingOnly = T)  #[-(1:5)]  # First args are for unix use only
cause <- arg[1]
yearStart <- arg[2]
yearEnd <- arg[3]
zone <- arg[4]
outdir <- arg[5]
job.name <- arg[6]

rrMaxPctile <- 0.95

rrMax <- rbindlist(lapply(yearStart:yearEnd, function(year) {
  fread(paste0(outdir, "rrMax_", cause, "_", zone, "_", year, ".csv"))}))

rrMax <- rbind(rrMax, copy(rrMax)[, risk := "all"])[is.na(risk) == F, ]

rrMax <- rrMax[order(acause, risk, draw, rrMax), ]
rrMax[, pct := cumsum(population) / sum(population), by = .(acause, risk, draw)]
rrMax[, pctDif := abs(pct - rrMaxPctile)]
rrMax[, isRrMax := pctDif == min(pctDif), by = .(acause, risk, draw)]

rrMax <- rrMax[isRrMax == 1, ][, .(acause, risk, draw, rrMax)]

rrMax[, meanTempCat := zone]

write.csv(rrMax, file = paste0(outdir, "rrMaxDraws_", cause, "_zone", zone, ".csv"), row.names = F)
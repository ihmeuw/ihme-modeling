## Launches relative risk max calculations.

## SYSTEM SETUP ----------------------------------------------------------
# Clear memory
rm(list=ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]


# Drives 
j_drive <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h_drive <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"
k_drive <- if (os == "Linux") "/FILEPATH/" else if (os == "Windows") "K:/"


# Base filepaths
work_dir <- paste0(j_drive, '/FILEPATH/')
share_dir <- '/FILEPATH/' # only accessible from the cluster


## LOAD DEPENDENCIES -----------------------------------------------------
library("dplyr") #library("tidyverse")
library("data.table")
library("feather")
library(argparse)
source(paste0(k_drive, "/FILEPATH/r/get_outputs.R"))
source(paste0(k_drive, "/FILEPATH/r/get_draws.R"))
source(paste0(k_drive, "/FILEPATH/r/get_location_metadata.R"))
source(paste0(k_drive, "/FILEPATH/r/get_population.R"))
source(paste0(k_drive, "/FILEPATH/current/r/get_cause_metadata.R"))



## DEFINE ARGS -----------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--causeList", help = "list of causes to run for",
                    default = 'lri', type = "character")
parser$add_argument("--outdir", help = "directory to output results to",
                    default = '/FILEPATH/config20210706', type = "character")
parser$add_argument("--popType", help = "source of population numbers, either fixed or annual",
                    default = 'annual', type = "character")
parser$add_argument("--tempZone", help = "which temperature zone is being modeled for",
                    default = 6, type = "integer")
parser$add_argument("--rrDir", help = "home of relative risk draws",
                    default = "/FILEPATH/updated_with_gamma_0.05-0.95/", type = "character")
parser$add_argument("--tmrelDir", help = "home of TMREL draws",
                    default = "/FILEPATH/config20210706_20210706", type = "character")
parser$add_argument("--description", help = "description of run",
                    default = "updated_with_gamma_0.05-0.95_gbd_20210706", type = "character")
parser$add_argument("--release_id", help = "ID of GBD release",
                    default = 10, type = "integer")
parser$add_argument("--yearStart", help = "first year of run",
                    default = 1990, type = "integer")
parser$add_argument("--yearEnd", help = "last year of run",
                    default = 2020, type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

for (a in c("causeList", "outdir", "popType", "tempZone", "rrDir", "description")) {
  warning(paste0(a, ": ", eval(parse(text = a))))
}

yearList <- yearStart:yearEnd

rrMaxPctile <- 0.95

tmreldir <- tmrelDir



### GET CAUSE AND LOCATION META-DATA ###
causeMeta <- get_cause_metadata(cause_set_id=4, release_id=release_id)
causeMeta <- causeMeta %>% filter(acause %in% causeList & level==3) %>% dplyr::select(cause_id, cause_name, acause, sort_order)

cause_list <- causeMeta$cause_id
acause_list <- causeMeta$acause
cause_link <- data.table(cause_id = cause_list, acause = acause_list)


### GET LOCATION METADATA ###
locMeta <- get_location_metadata(location_set_id = 35, release_id=release_id)



### IMPORT RR DRAWS FROM MR-BRT MODELS ###
rrRaw <- do.call(rbind, lapply(acause_list, function(acause) {
  cbind(fread(paste0(rrDir, "/", acause, "/", acause, "_curve_samples.csv")), acause)}))

setnames(rrRaw, "annual_temperature", "meanTempCat")

rrRaw <- rrRaw[meanTempCat==tempZone, ]
rrRaw <- rrRaw[acause %in% causeList,]

rrRaw[, dailyTempCat := as.integer(round(daily_temperature*10))][, daily_temperature := NULL]
rrRaw <- rrRaw[, lapply(.SD, mean), by = c("meanTempCat", "acause", "dailyTempCat"), .SDcols = paste0("draw_", 0:999)]

rrRaw[, paste0("draw_", 0:999) := lapply(.SD, exp), .SDcols = paste0("draw_", 0:999)]

rrRaw[, id := 1:.N]
rrRaw[, "rrMean"  := apply(.SD, 1, mean), .SDcols=paste0("draw_", 0:999)]
rrRaw[, "rrLower" := apply(.SD, 1, quantile, c(.025)), .SDcols=paste0("draw_", 0:999)]
rrRaw[, "rrUpper" := apply(.SD, 1, quantile, c(.975)), .SDcols=paste0("draw_", 0:999)]



rrClean <- copy(rrRaw)
rrClean[, paste0("draw_", 0:999) := NULL]


tempLimits <- unique(rrClean[, c("meanTempCat", "dailyTempCat")])
tempLimits <- unique(tempLimits[, `:=` (minTemp = min(dailyTempCat), maxTemp = max(dailyTempCat)), by = c("meanTempCat")][, dailyTempCat := NULL])


### IMPORT TMRELS ###
tmrel <- do.call(rbind, lapply(locMeta[most_detailed==1, location_id], function(loc_id) {fread(paste0(tmreldir, "/tmrel_", loc_id, "_summaries.csv"))}))
tmrel <- tmrel[year_id %in% yearList,]
tmrel <- tmrel[, tmrel := as.integer(round(tmrelMean*10))][, c("tmrelMean", "tmrelLower", "tmrelUpper") := NULL]

### MERGE RRs & TMRELs, and adjust RRs ###
rrClean <- merge(rrClean, tmrel, by = "meanTempCat", all.x = T, allow.cartesian = T)
rrClean[, refRR := sum(rrMean * (dailyTempCat==tmrel)), by = c("meanTempCat", "acause", "year_id", "location_id")]
rrClean[refRR==0, refRR := sum(rrMean * (dailyTempCat==(tmrel+1))), by = c("meanTempCat", "acause",  "year_id", "location_id")]

rrClean[, `:=` (rrMean = rrMean/refRR, rrLower = rrLower/refRR, rrUpper = rrUpper/refRR)]
rrClean[dailyTempCat < tmrel, risk := "cold"][dailyTempCat > tmrel, risk := "heat"]
rrClean[, c("tmrel", "refRR") := NULL]

temps <- do.call(rbind, lapply(yearList, function(y) {read_feather(paste0("/FILEPATH/tempCollapsed_", y, "_", tempZone, ".feather"))}))
setDT(temps)
temps[, year_id := as.integer(year_id)]

temps <- merge(temps, tempLimits, by = "meanTempCat", all.x = T)
temps[dailyTempCat<minTemp, dailyTempCat := minTemp][dailyTempCat>maxTemp, dailyTempCat := maxTemp]
temps[, c("minTemp", "maxTemp") := NULL]

master <- merge(temps, rrClean, by = c("meanTempCat", "dailyTempCat", "year_id", "location_id"), all.x = T, allow.cartesian = T)

### FIND RRmax ###
master <- master[is.na(rrMean)==F, ]

masterBkup <- copy(master)
master <- copy(masterBkup)

master <- master[order(meanTempCat, acause, rrMean), ]
master[, pct := cumsum(population) / sum(population), by = .(meanTempCat, acause)]
master[, pctDif := abs(pct-rrMaxPctile)]
master[, isRrMax := pctDif==min(pctDif), by = .(meanTempCat, acause)]

rrmax <- master[isRrMax==1, ][, .SD, .SDcols = c("meanTempCat", "acause", "id", "rrMean")][, risk := "all"]

master <- master[order(meanTempCat, acause, risk, rrMean), ]
master[, pct := cumsum(population) / sum(population), by = .(meanTempCat, acause, risk)]
master[, pctDif := abs(pct-rrMaxPctile)]
master[, isRrMax := pctDif==min(pctDif), by = .(meanTempCat, acause, risk)]

rrmax <- rbind(rrmax, master[isRrMax==1 & is.na(risk)==F, ][, .SD, .SDcols = c("meanTempCat", "acause", "id", "rrMean", "risk")])
setnames(rrmax, "rrMean", "rrMaxMean")

rrmax[, index := 1:.N, by = c("meanTempCat", "acause", "risk")]
rrmax <- rrmax[index==1, ]

rrMaxRows <- merge(rrmax, rrRaw, by = c("id", "meanTempCat"), all.x =T)
rrMaxRows[, paste0("draw_", 0:999) := lapply(.SD, function(x) {x * rrMaxMean / rrMean}), .SDcols = paste0("draw_", 0:999)]
rrMaxRows <- rrMaxRows[, .SD, .SDcols = c("meanTempCat", "risk", "rrMaxMean", paste0("draw_", 0:999))]

names(rrMaxRows) <- gsub("^draw_", "rrMax_", names(rrMaxRows))

write.csv(rrMaxRows, file = paste0(outdir, "/rrMax/rrMaxDraws_", causeList, "_zone", tempZone, ".csv"), row.names = F)



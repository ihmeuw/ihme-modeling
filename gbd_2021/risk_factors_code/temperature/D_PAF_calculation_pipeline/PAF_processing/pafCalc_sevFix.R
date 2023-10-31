## Launches PAF calculations for temperature.

## SYSTEM SETUP ----------------------------------------------------------
# Clear memory
rm(list=ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]


# Drives 
j <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"


# Base filepaths
work_dir <- paste0(j, '/FILEPATH/')
share_dir <- '/FILEPATH/' # only accessible from the cluster


## LOAD DEPENDENCIES -----------------------------------------------------
library(data.table)
library(dplyr)
library(argparse)
source("/FILEPATH/get_demographics_template.R")
source("/FILEPATH/get_cause_metadata.R")


# ## DEFINE ARGS -----------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--loc_ids", help = "location ID(s) to process",
                    default = 102, type = "integer")
parser$add_argument("--year_list", help = "year to investigate",
                    default = "1990,1995,2000,2005,2010,2015,2020", type = "character")
parser$add_argument("--outDir", help = "file to output to",
                    default = "/FILEPATH/version_13.0", type = "character")
parser$add_argument("--proj", help = "project to run on",
                    default = 'gbd', type = "character")
parser$add_argument("--scenario", help = "name of scenario to run",
                    default = 'rcp45', type = "character")
parser$add_argument("--job_name", help = "name of job",
                    default = 'paf_6', type = "character")
parser$add_argument("--release_id", help = "release ID to run for",
                    default = 10, type = "integer")
parser$add_argument("--config_version", help = "name of config file version",
                    default = 'config20220728b', type = "character")
parser$add_argument("--exp_outdir", help = "output path for era2melt step",
                    default = '/FILEPATH/', type = "character")
parser$add_argument("--rrMax_outRoot", help = "directory to which RRmax results were output",
                    default = "/FILEPATH/rr/", type = "character")
parser$add_argument("--rrDir", help = "folder to pull relative risk samples from",
                    #default = "/FILEPATH/rr/updated_with_gamma_0.05-0.95/", type = "character")
                    default = "/FILEPATH/rr/updated_with_gamma_0.05-0.95/", type = "character")
parser$add_argument("--tmrelRoot", help = "folder to pull TMREL inputs from",
                    default = "/FILEPATH/inputs/tmrel/", type = "character")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

# # For use with pafCalc-launch.R, since that can't work with the argparse setup
# arg <- commandArgs(trailingOnly = T)  #[-(1:5)]  # First args are for unix use only
# loc_ids   <- arg[1]
# year_list <- arg[2]
# outDir    <- arg[3]
# proj      <- arg[4]
# scenario  <- arg[5]
# job_name  <- arg[6]
# release_id <- arg[7]
# config_version <- arg[8]
# exp_outdir <- arg[9]
# rrMax_outRoot <- arg[10]
# rrDir     <- arg[11]
# tmrelRoot <- arg[12]


## BODY ------------------------------------------------------------------
start <- Sys.time()
dx <- FALSE


debug = F
maxDraw <- 999


if (proj=="forecasting") {
  #release_id <- 6
  tmrelVersion <- "VERSIONNAME"
  tempMeltDir <-   paste0("/FILEPATH/", scenario, "/collapsed/")
} else {
  #tmrelVersion <- "config20220728b_20220829b"
  tmrelVersion <- paste0(config_version, "_", substr(config_version, 7, nchar(config_version)))
  tempMeltDir <- exp_outdir
  rrMaxVersion <- config_version
  rrMaxDir <- paste0(rrMax_outRoot, rrMaxVersion, "/rrMax/")
}


warning(paste0("Location   = ", loc_ids))
warning(paste0("Year       = ", year_list))
warning(paste0("Output dir = ", outDir))
warning(paste0("Project = ", proj))
warning(paste0("Scenario = ", scenario))
warning(paste0("Job name   = ", job_name))

year_list <- as.integer(strsplit(year_list, split = ",")[[1]])

tmrelDir <- paste0(tmrelRoot, tmrelVersion)


acause_list <- c( "lri", "cvd_ihd", "cvd_stroke", "cvd_htn", "resp_copd",  "diabetes", "ckd", "inj_trans_road",
                  "inj_trans_other","inj_drowning", "inj_mech", "inj_animal", "inj_disaster", "inj_othunintent",
                  "inj_suicide", "inj_homicide")


## Read in relative risk draws for all included causes ---------
# Read and append RR draw files #
rr <- do.call(rbind, lapply(acause_list, function(acause) {
  cbind(fread(paste0(rrDir, "/", acause, "/", acause, "_curve_samples.csv")), acause)}))
warning("RRs loaded")

# Clean up variable names and formats #
setnames(rr, "annual_temperature", "meanTempCat")
setnames(rr, "daily_temperature", "dailyTempCat")

rr[, meanTempCat := as.integer(meanTempCat)]
rr[, acause := as.character(acause)]
rr[, dailyTempCat := as.integer(round(dailyTempCat*10))] # multiply daily temp by 10 to convert to integers and avoid problems with floating point precision differences
rr[, paste0("rr_", 0:maxDraw) := lapply(.SD, exp), .SDcols = c(paste0("draw_", 0:maxDraw))]

# Dx code to plot raw RR curves #
if (dx==TRUE) {
  rr[, "rrMean" := apply(.SD, 1, mean), .SDcols=paste0("rr_", 0:maxDraw)]
  rr %>% filter(meanTempCat<13) %>% ggplot(aes(x = dailyTempCat, y = rrMean, color = as.factor(meanTempCat))) + geom_line() + facet_wrap(~acause) + theme_minimal()
}

rr <- rr[, .SD, .SDcols = c("meanTempCat", "dailyTempCat", "acause", paste0("rr_", 0:maxDraw))]
rr <- melt(rr,  id.vars = c("meanTempCat", "dailyTempCat", "acause"), measure.vars = paste0("rr_", 0:maxDraw), variable.name = "draw", value.name = "rr", variable.factor = F)
rr[, draw := as.integer(gsub("rr_", "", draw))]
warning("RRs reshaped to long")


## Read in TMRELs and merge with RRs ---------------------------
if (proj=="forecasting") {
  tmrel <- do.call(rbind, lapply(loc_ids, function(loc_id) { fread(paste0(tmrelDir, "/tmrel_", loc_id, "_", year_list, ".csv"))}))
} else {
  tmrel <- do.call(rbind, lapply(loc_ids, function(loc_id) { fread(paste0(tmrelDir, "/tmrel_", loc_id, ".csv"))[year_id %in% year_list, ]}))
}
warning("TMRELs loaded")

tmrel[, location_id := as.integer(location_id)]
tmrel <- melt(tmrel,  id.vars = c("location_id", "year_id", "meanTempCat"), measure.vars = paste0("tmrel_", 0:maxDraw), variable.name = "draw", value.name = "tmrel", variable.factor = F)
tmrel[, draw := as.integer(gsub("tmrel_", "", draw))]
tmrel[, tmrel := as.integer(round(tmrel)*10)] # multiply daily temp by 10 to convert to integers and avoid problems with floating point precision differences
warning("TMRELs reshaped and prepped")

rr <- merge(rr, tmrel, by = c("meanTempCat", "draw"), all = T, allow.cartesian = T)
warning("RRs merged to TMRELs")

## Shift the RR curves to equal 1.0 at the TMREL ---------------
rr[, rrRef := sum(rr * (dailyTempCat==tmrel), na.rm = T), by = c("meanTempCat", "acause", "draw", "location_id", "year_id")]
rr[, rr := rr / rrRef]
warning("RRs rescaled to the TMREL")


# Dx code to plot scaled RR curves #
if (dx==TRUE) {
  rrMean <- rr %>% group_by(meanTempCat, dailyTempCat, acause, location_id, year_id) %>% summarise(rrMean = mean(rr))
  rrMean %>% filter(meanTempCat<13) %>%
    ggplot(aes(x = dailyTempCat, y = rrMean, color = as.factor(meanTempCat))) +
    geom_line() + geom_hline(yintercept = 1, color = "black") +
    facet_wrap(~acause) + theme_minimal()

  rrMean %>% filter(meanTempCat<13) %>%
    ggplot(aes(x = dailyTempCat, y = rrMean, color = as.factor(acause))) +
    geom_line() + geom_hline(yintercept = 1, color = "black") + geom_vline(aes(xintercept = tmrelMean)) +
    facet_wrap(~meanTempCat) + theme_minimal()
}



## Find the min and max modelled daily temperatures for each zone ----
tempLims <- rr %>% group_by(meanTempCat) %>% summarize(minTemp = min(dailyTempCat), maxTemp = max(dailyTempCat))
setDT(tempLims)
warning("Temperature limits extracted")


## Read in temperature flat file -------------------------------
temp <- do.call(rbind, lapply(loc_ids, function(loc_id) {
  do.call(rbind, lapply(year_list, function(y) {data.table(location_id = loc_id, year_id = y, fread(paste0(tempMeltDir, "melt_", loc_id, "_", y, ".csv")))}))}))
temp <- temp[draw %in% 0:maxDraw]

warning("Temperature data loaded")


## Truncate temperature zones and daily temps within zones -----
temp[meanTempCat<6, meanTempCat := 6][meanTempCat>28, meanTempCat := 28]
temp <- merge(temp, tempLims, by = "meanTempCat", all.x = T)
temp[, dailyTempCatRaw := dailyTempCat][dailyTempCatRaw<minTemp, dailyTempCat := minTemp][dailyTempCatRaw>maxTemp, dailyTempCat := maxTemp]
temp[, c("minTemp", "maxTemp") := NULL]
warning("Temperature data truncated")

if (dx==T) {
  temp %>% dplyr::select(meanTempCat, dailyTempCat, dailyTempCatRaw) %>% unique() %>%
    ggplot(aes(x = dailyTempCatRaw, y = dailyTempCat)) + geom_point() + facet_wrap(vars(meanTempCat)) + theme_minimal()
}

## Collapse pixel-days into person-time by zone and daily temperature ----
temp <- temp[, lapply(.SD, function(x) {sum(x, na.rm = T)}), by = c("meanTempCat", "dailyTempCat", "draw", "year_id", "location_id"), .SDcols = "pop"]
temp[, pr := lapply(.SD, function(x) {x/sum(x, na.rm = T)}), .SDcols = "pop", by = c("draw", "year_id", "location_id")]
temp[, location_id := as.integer(location_id)]
warning("Temperature data collapsed")

# Dx code to plot temperature exposure distributions #
if (dx==TRUE) {
  temp %>% ggplot(aes(x = dailyTempCat, y = pop)) + geom_col() +
    facet_wrap(~meanTempCat, scales = "free") + theme_minimal()
}


## Import RRmax ------------------------------------------------
if (proj!="NAME") {
  rrMax <- do.call(rbind, lapply(unique(temp$meanTempCat), function(tempZone) {
    do.call(rbind, lapply(acause_list, function(cause) {
      data.table(fread(paste0(rrMaxDir, "rrMaxDraws_", cause, "_zone", tempZone, ".csv")), meanTempCat = tempZone, acause = cause)}))}))

  warning("RR Max loaded")

  rrMax <- melt(rrMax, id.vars = c("meanTempCat", "risk", "acause"), measure.vars = paste0("rrMax_", 0:maxDraw), variable.name = "draw", value.name = "rrMax", variable.factor = F)
  rrMax[, draw := as.integer(gsub("rrMax_", "", draw))]
  warning("RR Max reshaped to long")
}

#save.image(file = paste0(outDir, "/startingImage_", job_name, ".RData"))


## Merge temperature and RR ------------------------------------
pafs <- merge(temp, rr, by = c("meanTempCat", "dailyTempCat", "draw", "location_id", "year_id"), all.x = T)
warning("RRs merged to temperatures")

# Classify days as hot or cold #
pafs[, risk := ifelse(dailyTempCat < tmrel, "cold", ifelse(dailyTempCat > tmrel, "heat", NA))]

# Create zero population rows for heat if there are no heat rows (this prevents the creation of empty draw files) #
if (length(which(pafs$risk=="heat"))==0) {
  heat <- copy(pafs)[, index := 1:.N, by = c("meanTempCat", "draw", "location_id", "year_id", "acause")][index==1, ]
  heat[, risk := "heat"][, c("pop", "pr") := 0][, index := NULL]
  pafs <- rbind(pafs, heat, fill = T)
}

# Create zero population rows for cold if there are no cold rows (this prevents the creation of empty draw files) #
if (length(which(pafs$risk=="cold"))==0) {
  cold <- copy(pafs)[, index := 1:.N, by = c("meanTempCat", "draw", "location_id", "year_id", "acause")][index==1, ]
  cold[, risk := "cold"][, c("pop", "pr") := 0][, index := NULL]
  pafs <- rbind(pafs, cold)
}
warning("Days classified as heat or cold")

# Create df for calculating sevs, adding copies of all rows for use in calculating non-optimal temp sevs #
if (proj!="NAME") {
  sevs <- rbind(pafs, copy(pafs)[, risk := "all"])
  warning("SEV DT created")
}


## Calculate PAFs ----------------------------------------------
pafs <- pafs[is.na(risk)==F & is.na(acause)==F, ]
pafs <- pafs[, lapply(.SD, function(x) {as.double(sum( ifelse(x>=1, pr*(x-1)/x, pr*-1*((1/x)-1)/(1/x))))}), .SDcols = "rr", by = c("acause", "risk", "draw", "year_id", "location_id")]
warning("PAFs calculated")

pafs[, draw := paste0("draw_", draw)]
pafs <- dcast(pafs, location_id + year_id + acause + risk ~ draw, value.var = "rr", drop = F)
warning("PAFs reshaped to wide")

pafs[, paste0("draw_", 0:maxDraw) := lapply(.SD, function(x) {ifelse(is.na(x)==T, 0, x)}), .SDcols = paste0("draw_", 0:maxDraw)]
warning("Filled missing PAF draws")

## Calculate PAF draw summaries --------------------------------
pafs[, "pafMean" := apply(.SD, 1, mean), .SDcols=paste0("draw_", 0:maxDraw)]
pafs[, "pafLower" := apply(.SD, 1, quantile, c(.025)), .SDcols=paste0("draw_", 0:maxDraw)]
pafs[, "pafUpper" := apply(.SD, 1, quantile, c(.975)), .SDcols=paste0("draw_", 0:maxDraw)]
warning("PAF summaries calculated")


## Get demographic levels and causes needed for PAF upload -----
# Get all of the levels of age and sex #
demog <- get_demographics_template(gbd_team = "cod", release_id = release_id)[, c("location_id", "year_id") := NULL]
demog <- unique(demog)
demog[, merge := 1]
warning("Demographic template pulled")

# Find all of the most detailed causes for which the level 3 is in our cause list #
causeMeta <- get_cause_metadata(cause_set_id = 2, release_id = release_id)
causeMeta <- cbind(causeMeta, sapply(1:nrow(causeMeta), function(i) {as.integer(strsplit(causeMeta[i, path_to_top_parent], split = ",")[[1]][4])}))
names(causeMeta)[ncol(causeMeta)] <- "l3_id"

causeList3 <- causeMeta[acause %in% acause_list & level==3, ][, .(cause_id, acause)]
names(causeList3) <- c("l3_id", "l3_acause")

causeList3 <- merge(causeList3, causeMeta, by = "l3_id", all.x = T)[most_detailed==1, ][, .(l3_acause, cause_id, acause)]
warning("Export cause list created")


## PREP THE PAF OUTPUT FILE ----------------------------------------------
pafs[, measure_id := 4][, merge := 1]
pafs <- merge(pafs, demog, by = "merge", all = T, allow.cartesian = T)
pafs[, merge := NULL]
warning("PAFs merged to demographic template")

setnames(pafs, "acause", "l3_acause")
pafs <- merge(pafs, causeList3, by = "l3_acause", all = T, allow.cartesian = T)
pafs[, l3_acause := NULL]
warning("PAFs merged to export cause list")

heatPafs <- pafs[risk=="heat", ][, risk := NULL]
coldPafs <- pafs[risk=="cold", ][, risk := NULL]
warning("PAFs split into heat and cold")



## CALCULATE SEVS --------------------------------------------------------
# Merge in RR max #
if (proj!="NAME") {
  sevs <- merge(sevs, rrMax, by = c("meanTempCat", "risk", "acause", "draw"), all.x = T)
  warning("SEV table merged to RRmax")

  sevs[, pr := lapply(.SD, function(x) {x/sum(x, na.rm = T)}), .SDcols = "pop", by = c("draw", "year_id", "location_id", "risk", "acause")]

  sevs[, sev := ifelse(rrMax<=1 | rr<=1, 0, pr * (rr - 1)/(rrMax-1))]
  sevs[, rr := pr * rr]

  sevs <- sevs[, lapply(.SD, function(x) {as.numeric(sum(x, na.rm = T))}), by = c("location_id", "year_id", "risk", "acause", "draw"), .SDcols = c("sev", "rr", "pop")]
  warning("SEVs calculated")

  sevs[, sev := ifelse(sev<0, 0, sev)]
  sevs[, sev := ifelse(sev>1, 1, sev)]

  sevs[, draw := paste0("draw_", draw)]
  sevs <- dcast(sevs, location_id + year_id + acause + risk ~ draw, value.var = "sev", drop = F)
  warning("SEVs reshaped to wide")

  sevs[, paste0("draw_", 0:maxDraw) := lapply(.SD, function(x) {ifelse(is.na(x)==T, 0, x)}), .SDcols = paste0("draw_", 0:maxDraw)]
  warning("Filled missing SEV draws")

  sevs[, `:=` (age_group_id = 22, sex_id = 3)]
  sevs[risk=="all", rei_id := 331][risk=="heat", rei_id := 337][risk=="cold", rei_id := 338][, risk := NULL]
  warning("SEVs cleaned up for export")
}




## SAVE OUTPUT -----------------------------------------------------------
for (loc_id in loc_ids) {
  warning(paste0("Begining export for location ", loc_id))
  for (year in year_list) {
    write.csv(heatPafs[location_id==loc_id & year_id==year, ], file = paste0(outDir, "/heat/", loc_id, "_", year, ".csv"), row.names = F)
    write.csv(coldPafs[location_id==loc_id & year_id==year, ], file = paste0(outDir, "/cold/", loc_id, "_", year, ".csv"), row.names = F)
    if (proj!="NAME") {
      write.csv(sevs[location_id==loc_id & year_id==year, ], file = paste0(outDir, "/sevs/raw/", loc_id, "_", year, ".csv"), row.names = F)
    }
    warning(year)

    write.csv("Done", file = paste0(outDir, "/paf_", loc_id, "_", year, ".csv"), row.names = F)
    warning("Done file saved")
  }
}


end <- Sys.time()
warning(runtime <- difftime(end, start))

warning("Complete")



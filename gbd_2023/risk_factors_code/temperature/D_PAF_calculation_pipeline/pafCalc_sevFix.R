rm(list=ls())

start <- Sys.time()
dx <- F


# If this is a batch job, then the arguments will be passed in from the command line
# otherwise, hard code the desired arguments for testing and development
INTERACTIVE = commandArgs()[2] == '--interactive'

if (!INTERACTIVE) { # If launched from cmd line use args
  arg <- commandArgs(trailingOnly = T)  #[-(1:5)]  # First args are for unix use only
  loc_ids         <- as.integer(arg[1])
  year_list       <- arg[2]
  outDir          <- arg[3]
  proj            <- arg[4]
  scenario        <- arg[5]
  extrapolate     <- as.numeric(arg[6])
  config.file     <- arg[7]
  calcSevs        <- as.logical(arg[8])
  tmrelDir        <- gsub("/$", "", arg[9])  # gsub removes trailing slash, if one exists
  rrDir           <- gsub("/$", "", arg[10])
  tempMeltDir     <- gsub("/$", "", arg[11])
  rrMaxDir        <- gsub("/$", "", arg[12])
  continuousZones <- as.logical(arg[13])
  release_id      <- as.integer(arg[14])
  maxDraw         <- as.integer(arg[15])
  job.name        <- arg[16]

} else {
  loc_ids <- 7
  year_list <- '2061'
  outDir <- "FILEPATH"
  proj <- "PROJECT"
  scenario <- "ssp119"
  extrapolate <- 10
  config.file <- "FILEPATH"
  calcSevs <- F
  tmrelDir <- "FILEPATH"
  rrDir <- "FILEPATH"
  tempMeltDir <- file.path("FILEPATH", scenario)
  rrMaxDir <- "FILEPATH"
  continuousZones <- T
  release_id <- 9
  maxDraw <- 499
  job.name <- paste('paf', loc_ids, year_list, scenario, sep = '_')

}

debug = F

if (proj == 'forecasting') {
  fixedTmrel <- T
  fixedTmrelYear <- 2021
} else {
  fixedTmrel <- F
}

message(paste0("Location   = ", loc_ids))
message(paste0("Year       = ", year_list))
message(paste0("Output dir = ", outDir))
message(paste0("Project = ", proj))
message(paste0("Scenario = ", scenario))
message(paste0("Extrapolate = ", extrapolate))
message(paste0("Calculate SEVs = ", calcSevs))
message(paste0("TMREL Dir = ", tmrelDir))
message(paste0("RR Dir = ", rrDir))
message(paste0("Temp Dir = ", tempMeltDir))
message(paste0("Continuous zones = ", continuousZones))
message(paste0("Release ID = ", release_id))
message(paste0("Max draw = ", maxDraw))
message(paste0("Job name   = ", job.name))
message(paste0("Config file = ", config.file))



year_list <- as.integer(strsplit(year_list, split = ",")[[1]])



library("data.table")
library("dplyr")

source("FILEPATH/get_demographics_template.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/csv2objects.R")



csv2objects(config.file, exclude = ls())
acause_list <- causeList

### READ IN RELATIVE RISK DRAWS FOR ALL INCLUDED CAUSES ###
# Read and append RR draw files #
rr <- do.call(rbind, lapply(acause_list, function(acause) {
  cbind(fread(file.path(rrDir, acause, paste0(acause, "_curve_samples.csv"))), acause)}))
warning("RRs loaded")


# Clean up variable names and formats #
setnames(rr, "annual_temperature", "meanTempCat")
setnames(rr, "daily_temperature", "dailyTempCat")

rr[, meanTempCat := as.integer(meanTempCat)]
rr[, acause := as.character(acause)]
rr[, dailyTempCat := as.integer(round(dailyTempCat * 10))] # multiply daily temp by 10 to convert to integers and avoid problems with floating point precision differences
rr[, paste0("rr_", 0:maxDraw) := lapply(.SD, exp), .SDcols = c(paste0("draw_", 0:maxDraw))]

# Dx code to plot raw RR curves #
if (dx == T) {
  rr[, "rrMean" := apply(.SD, 1, mean), .SDcols = paste0("rr_", 0:maxDraw)]
  rr %>% filter(meanTempCat < 13) %>% ggplot(aes(x = dailyTempCat, y = rrMean, color = as.factor(meanTempCat))) +
    geom_line() + facet_wrap(~acause) + theme_minimal()
}

rr <- rr[, .SD, .SDcols = c("meanTempCat", "dailyTempCat", "acause", paste0("rr_", 0:maxDraw))]
rr <- melt(rr,  id.vars = c("meanTempCat", "dailyTempCat", "acause"), measure.vars = paste0("rr_", 0:maxDraw),
           variable.name = "draw", value.name = "rr", variable.factor = F)
rr[, draw := as.integer(gsub("rr_", "", draw))]
warning("RRs reshaped to long")


### READ IN TMRELS AND MERGE WITH RRs ###
if (proj == "forecasting") {
  if (fixedTmrel == T) {
    tmrel <- rbindlist(lapply(year_list, function(y) {
      rbindlist(lapply(loc_ids, function(loc_id) {
        fread(file.path(tmrelDir, paste0("tmrel_", loc_id, ".csv"))
              )[year_id == fixedTmrelYear]}))[, year_id := y]}))

  } else {
    tmrel <- rbindlist(lapply(year_list, function(y) {
      rbindlist(lapply(loc_ids, function(loc_id) {
        fread(file.path(tmrelDir, paste0("tmrel_", loc_id, "_", y, ".csv")))})
        )[, year_id := y]}))
  }

} else {
  tmrel <- rbindlist(lapply(loc_ids, function(loc_id) {
    fread(file.path(tmrelDir, paste0("tmrel_", loc_id, ".csv"))
          )[year_id %in% year_list, ]}))
}
warning("TMRELs loaded")

tmrel[, location_id := as.integer(location_id)]
tmrel <- melt(tmrel,  id.vars = c("location_id", "year_id", "meanTempCat"),
              measure.vars = paste0("tmrel_", 0:maxDraw), variable.name = "draw",
              value.name = "tmrel", variable.factor = F)
tmrel[, draw := as.integer(gsub("tmrel_", "", draw))]
tmrel[, tmrel := as.integer(round(tmrel) * 10)] # multiply daily temp by 10 to convert to integers and avoid problems with floating point precision differences
warning("TMRELs reshaped and prepped")

rr <- merge(rr, tmrel, by = c("meanTempCat", "draw"), all = T, allow.cartesian = T)
warning("RRs merged to TMRELs")

### SHIFT THE RR CURVES TO EQUAL 1.0 AT THE TMREL ###
rr[, rrRef := sum(rr * (dailyTempCat == tmrel), na.rm = T), by = c("meanTempCat", "acause", "draw", "location_id", "year_id")]
rr[, rr := rr / rrRef]
warning("RRs rescaled to the TMREL")




# Dx code to plot scaled RR curves #
if (dx == T) {
  rrMean <- rr %>% group_by(meanTempCat, dailyTempCat, acause, location_id, year_id) %>% summarise(rrMean = mean(rr))
  rrMean %>% filter(meanTempCat < 13) %>%
    ggplot(aes(x = dailyTempCat, y = rrMean, color = as.factor(meanTempCat))) +
    geom_line() + geom_hline(yintercept = 1, color = "black") +
    facet_wrap(~acause) + theme_minimal()

  rrMean %>% filter(meanTempCat < 13) %>%
    ggplot(aes(x = dailyTempCat, y = rrMean, color = as.factor(acause))) +
    geom_line() + geom_hline(yintercept = 1, color = "black") + geom_vline(aes(xintercept = tmrelMean)) +
    facet_wrap(~meanTempCat) + theme_minimal()
}



### FIND THE MIN AND MAX MODELLED DAILY TEMPERATURES FOR EACH ZONE ###
rrLims <- rr %>% group_by(meanTempCat) %>%
  summarize(minRrTemp = min(dailyTempCat), maxRrTemp = max(dailyTempCat))
setDT(rrLims)

dataLims <- fread(file.path(outDir, "limits.csv"))
dataLims[, `:=` (minDataTemp = round((min - extrapolate) * 10),
                 maxDataTemp = round((max + extrapolate) * 10))
         ][, c("min", "max") := NULL]

tempLims <- merge(rrLims, dataLims, by = "meanTempCat", all = T)
tempLims[, minTemp := pmax(minDataTemp, minRrTemp)
         ][, maxTemp := pmin(maxDataTemp, maxRrTemp)]
tempLims <- tempLims[, .(meanTempCat, minTemp, maxTemp)]

warning("Temperature limits extracted")



### READ IN TEMPERATURE FLAT FILE ###
temp <- do.call(rbind, lapply(loc_ids, function(loc_id) {
  do.call(rbind, lapply(year_list, function(y) {
    data.table(location_id = loc_id, year_id = y,
               fread(file.path(tempMeltDir,
                               paste0("melt_", loc_id, "_", y, ".csv"))))}))}))
temp <- temp[draw %in% 0:maxDraw]

# If we have fewer temperature draws then we need we'll resample
temp_draw_values <- unique(temp$draw)

if (max(temp_draw_values) < maxDraw) {
  # Updated files are in °C, but others are in °K -- reconcile here
  temp[, dailyTempCat := as.integer(round(dailyTempCat + 2731.5))]

  # Determine how many draws we need to add
  n_missing_draws <- maxDraw - max(temp_draw_values)

  # Sample new draws from existing, creating a link of old draw number to new draw number
  draw_sample <- data.table(old_draw = sample(temp_draw_values, n_missing_draws, replace = T),
                            new_draw = max(temp_draw_values) + 1:maxDraw)

  # Create new draws by sampling from existing draws
  sampled_draws <- rbindlist(lapply(seq_len(nrow(draw_sample)), function(i) {
    temp_draw <- temp[draw == draw_sample$old_draw[i], ][, draw := draw_sample$new_draw[i]]
  }))

  # Add new draws to the existing draws
  temp <- rbind(temp, sampled_draws)
}


if (("population" %in% names(temp)) == T) setnames(temp, "population", "pop")
if (("temp_zone" %in% names(temp)) == T) setnames(temp, "temp_zone", "meanTempCat")
if (("daily_temp_cat" %in% names(temp)) == T) setnames(temp, "daily_temp_cat", "dailyTempCat")

warning("Temperature data loaded")



### TRUNCATE TEMPERATURE ZONES AND DAILY TEMPS WITHIN ZONES ###
temp[meanTempCat < 6, meanTempCat := 6][meanTempCat > 28, meanTempCat := 28]
temp <- merge(temp, tempLims, by = "meanTempCat", all.x = T)
temp[, dailyTempCatRaw := dailyTempCat
     ][dailyTempCatRaw < minTemp, dailyTempCat := minTemp
       ][dailyTempCatRaw > maxTemp, dailyTempCat := maxTemp]
temp[, c("minTemp", "maxTemp") := NULL]
warning("Temperature data truncated")

if (dx == T) {
  temp %>% dplyr::select(meanTempCat, dailyTempCat, dailyTempCatRaw) %>% unique() %>%
    ggplot(aes(x = dailyTempCatRaw, y = dailyTempCat)) + geom_point() +
    facet_wrap(vars(meanTempCat)) + theme_minimal()
}


### CONTINUOUS ZONES ###
if (continuousZones == T) {
  temp <- rbind(copy(temp)[, `:=` (meanTempCat = floor(meanTempContinuous),
                                   pop = pop * (1 - (meanTempContinuous %% 1)))],
                copy(temp)[, `:=` (meanTempCat = ceiling(meanTempContinuous),
                                   pop = pop * (meanTempContinuous %% 1))])

  temp[meanTempCat < 6, meanTempCat := 6][meanTempCat > 28, meanTempCat := 28]
}

### COLLAPSE PIXEL-DAYS INTO PERSON-TIME BY ZONE AND DAILY TEMPERATURE ###
temp <- temp[, lapply(.SD, function(x) {sum(x, na.rm = T)}),
             by = c("meanTempCat", "dailyTempCat", "draw", "year_id", "location_id"),
             .SDcols = "pop"]
temp[, pr := lapply(.SD, function(x) {x/sum(x, na.rm = T)}), .SDcols = "pop",
     by = c("draw", "year_id", "location_id")]
temp[, location_id := as.integer(location_id)]
warning("Temperature data collapsed")

# Dx code to plot temperature exposure distributions #
if (dx == T) {
  temp %>% ggplot(aes(x = dailyTempCat, y = pop)) + geom_col() +
    facet_wrap(~meanTempCat, scales = "free") + theme_minimal()
}


### IMPORT RRMAX ###
if (proj != "forecasting") {
  rrMax <- rbindlist(lapply(unique(temp$meanTempCat), function(tempZone) {
    rbindlist(lapply(acause_list, function(cause) {
      fread(file.path(rrMaxDir, paste0("rrMaxDraws_", cause, "_zone", tempZone, ".csv")))}))}))

  warning("RR Max loaded")

}



# Create file to plot distribution of RRs #
rrSummary   <- copy(rr)[, lapply(.SD, mean, na.rm = T),
                        by = c("meanTempCat", "dailyTempCat", "location_id", "year_id", "acause"),
                        .SDcols = "rr"]
tempSummary <- copy(temp)[, lapply(.SD, mean, na.rm = T),
                          by = c("meanTempCat", "dailyTempCat", "location_id", "year_id"),
                          .SDcols = "pop"]

rrDist <- merge(rrSummary, tempSummary,
                by = c("meanTempCat", "dailyTempCat", "location_id", "year_id"),
                all.y = T)

for (loc_id in loc_ids) {
  for (year in year_list) {
    write.csv(rrDist[location_id == loc_id & year_id == year, ],
              file.path(outDir, "rrDist", paste0("rrDist_", loc_id, "_", year, ".csv")),
              row.names = F)
  }
}

rm(rrSummary, tempSummary, rrDist)





### MERGE TEMPERATURE AND RR ###
pafs <- merge(temp, rr,
              by = c("meanTempCat", "dailyTempCat", "draw", "location_id", "year_id"),
              all.x = T)
warning("RRs merged to temperatures")



# Classify days as hot or cold #
pafs[, risk := ifelse(dailyTempCat < tmrel, "cold",
                      ifelse(dailyTempCat > tmrel, "heat", NA))]

# Create zero population rows for heat if there are no heat rows (this prevents the creation of empty draw files) #
if (length(which(pafs$risk == "heat")) == 0) {
  heat <- copy(pafs)[, index := 1:.N,
                     by = c("meanTempCat", "draw", "location_id", "year_id", "acause")
                     ][index == 1, ]
  heat[, risk := "heat"][, c("pop", "pr") := 0][, index := NULL]
  pafs <- rbind(pafs, heat, fill = T)
}

# Create zero population rows for cold if there are no cold rows (this prevents the creation of empty draw files) #
if (length(which(pafs$risk == "cold")) == 0) {
  cold <- copy(pafs)[, index := 1:.N,
                     by = c("meanTempCat", "draw", "location_id", "year_id", "acause")
                     ][index == 1, ]
  cold[, risk := "cold"][, c("pop", "pr") := 0][, index := NULL]
  pafs <- rbind(pafs, cold)
}
warning("Days classified as heat or cold")

# Create df for calculating sevs, adding copies of all rows for use in calculating non-optimal temp sevs #
if (proj != "forecasting") {
  sevs <- rbind(pafs, copy(pafs)[, risk := "all"])
  warning("SEV DT created")
}


### CALCULATE PAFs ###
pafs <- pafs[is.na(risk) == F & is.na(acause) == F, ]
pafs <- pafs[, lapply(.SD, function(x) {
  as.double(sum( ifelse(x >= 1, pr*(x-1)/x, pr*-1*((1/x)-1)/(1/x))))}),
  .SDcols = "rr", by = c("acause", "risk", "draw", "year_id", "location_id")]
warning("PAFs calculated")

pafs[, draw := paste0("draw_", draw)]
pafs <- dcast(pafs, location_id + year_id + acause + risk ~ draw,
              value.var = "rr", drop = F)
warning("PAFs reshaped to wide")


pafs[, paste0("draw_", 0:maxDraw) := lapply(.SD, function(x) {
  ifelse(is.na(x) == T, 0, x)}), .SDcols = paste0("draw_", 0:maxDraw)]
warning("Filled missing PAF draws")

### CALCULATE PAF DRAW SUMMARIES ###
pafs[, "pafMean" := apply(.SD, 1, mean), .SDcols = paste0("draw_", 0:maxDraw)]
pafs[, "pafLower" := apply(.SD, 1, quantile, c(.025)), .SDcols = paste0("draw_", 0:maxDraw)]
pafs[, "pafUpper" := apply(.SD, 1, quantile, c(.975)), .SDcols = paste0("draw_", 0:maxDraw)]
warning("PAF summaries calculated")




### GET DEMOGRAPHIC LEVELS AND CAUSES NEEDED FOR PAF UPLOAD ###
# Get all of the levels of age and sex #
demog <- get_demographics_template(gbd_team = "cod", release_id = release_id
                                   )[, c("location_id", "year_id") := NULL]
demog <- unique(demog)
demog[, merge := 1]
warning("Demographic template pulled")

# Find all of the most detailed causes for which the level 3 is in our cause list #
causeMeta <- get_cause_metadata(cause_set_id = 2, release_id = release_id)
causeMeta <- cbind(causeMeta, sapply(1:nrow(causeMeta), function(i) {
  as.integer(strsplit(causeMeta[i, path_to_top_parent], split = ",")[[1]][4])}))
names(causeMeta)[ncol(causeMeta)] <- "l3_id"

causeList3 <- causeMeta[acause %in% acause_list & level == 3, ][, .(cause_id, acause)]
names(causeList3) <- c("l3_id", "l3_acause")

causeList3 <- merge(causeList3, causeMeta, by = "l3_id", all.x = T
                    )[most_detailed == 1, ][, .(l3_acause, cause_id, acause)]
warning("Export cause list created")


### PREP THE PAF OUTPUT FILE ###
pafs[, measure_id := 4][, merge := 1]
pafs <- merge(pafs, demog, by = "merge", all = T, allow.cartesian = T)
pafs[, merge := NULL]
warning("PAFs merged to demographic template")

setnames(pafs, "acause", "l3_acause")
pafs <- merge(pafs, causeList3, by = "l3_acause", all = T, allow.cartesian = T)
pafs[, l3_acause := NULL]
warning("PAFs merged to export cause list")

heatPafs <- pafs[risk == "heat", ][, risk := NULL]
coldPafs <- pafs[risk == "cold", ][, risk := NULL]
warning("PAFs split into heat and cold")



### CALCULATE SEVS ###
# Merge in RR max #
if (proj != "forecasting") {
  sevs <- merge(sevs, rrMax, by = c("meanTempCat", "risk", "acause", "draw"),
                all.x = T)
  sevs <- sevs[!(is.na(risk) == T & dailyTempCat == tmrel), ]
  warning("SEV table merged to RRmax")

  sevs[, pr := lapply(.SD, function(x) {x/sum(x, na.rm = T)}), .SDcols = "pop",
       by = c("draw", "year_id", "location_id", "risk", "acause")]

  sevs[, sev := ifelse(rrMax <= 1 | rr <= 1, 0, pr * (rr - 1)/(rrMax - 1))]
  sevs[, rr := pr * rr]

  sevs <- sevs[, lapply(.SD, function(x) {as.numeric(sum(x, na.rm = T))}),
               by = c("location_id", "year_id", "risk", "acause", "draw"),
               .SDcols = c("sev", "rr", "pop")]
  warning("SEVs calculated")

  sevs[, sev := ifelse(sev < 0, 0, sev)]
  sevs[, sev := ifelse(sev > 1, 1, sev)]

  sevs[, draw := paste0("draw_", draw)]
  sevs <- dcast(sevs, location_id + year_id + acause + risk ~ draw,
                value.var = "sev", drop = F)
  warning("SEVs reshaped to wide")

  sevs[, paste0("draw_", 0:maxDraw) := lapply(.SD, function(x) {
    ifelse(is.na(x) == T, 0, x)}), .SDcols = paste0("draw_", 0:maxDraw)]
  warning("Filled missing SEV draws")

  sevs[, `:=` (age_group_id = 22, sex_id = 3)]
  sevs[risk == "all", rei_id := 331
       ][risk == "heat", rei_id := 337
         ][risk == "cold", rei_id := 338
           ][, risk := NULL]
  warning("SEVs cleaned up for export")
}




### SAVE THE OUTPUT FILES ###
for (loc_id in loc_ids) {
  warning(paste0("Begining export for location ", loc_id))
  for (year in year_list) {
    out_file <- paste0(loc_id, "_", year, ".csv")

    write.csv(heatPafs[location_id == loc_id & year_id == year, ],
              file = file.path(outDir, "heat", out_file), row.names = F)
    write.csv(coldPafs[location_id == loc_id & year_id == year, ],
              file = file.path(outDir, "cold", out_file), row.names = F)

    if (proj != "forecasting") {
      write.csv(sevs[location_id == loc_id & year_id == year, ],
                file = file.path(outDir, "sevs", "raw", out_file), row.names = F)
    }
    warning(year)

    write.csv(pafs[, paste0("draw_", 0:maxDraw) := NULL][location_id == loc_id & year_id == year, ],
              file = file.path(outDir, "summaries", out_file), row.names = F)


    clean_summary <- pafs[location_id == loc_id & year_id == year & age_group_id == 10 & sex_id == 1, ]
    clean_summary[, c(paste0("draw_", 0:maxDraw), 'age_group_id', 'sex_id') := NULL]

    write.csv(clean_summary,
              file = file.path(outDir, "clean_summaries", out_file), row.names = F)

  write.csv("Done", file = file.path(outDir, paste0("paf_", out_file)), row.names = F)
  warning("Done file saved")
  }
}


end <- Sys.time()
warning(runtime <- difftime(end, start))

warning("Complete")
##########################################################################
## Author: USERNAME
## Date: April 10th, 2017
## Purpose: Get Severity Splits for Headache
##########################################################################

## SET-UP
rm(list=ls())

if (Sys.info()[1] == "Linux"){
  j <- "FILEPATH"
  h <- "FILEPATH"
} else if (Sys.info()[1] == "Windows"){
  j <- "FILEPATH"
  h <- "FILEPATH"
}

pacman::p_load(data.table, ggplot2, boot, readr, openxlsx)

## SET OBJECTS
date <- gsub("-", "_", Sys.Date())
freq_dir <- paste0(j, "FILEPATH")
functions_dir <- paste0(j, "FILEPATH")
save_dir <- paste0("FILEPATH")
probme <- 20190
defme <- 20191
draws <- paste0("draw_", 0:999)

## SOURCE FUNCTION
source(paste0(functions_dir, "get_draws.R"))

## GET RESULTS
files <- list.files(freq_dir)
files <- files[grepl("results2", files)]
dates <- substr(files, 8, 17)
dates <- gsub("_", "-", dates)
last_date <- dates[which.max(as.POSIXct(dates))]
last_date <- gsub("-", "_", last_date)
timesym <- fread(paste0(freq_dir, "/results", last_date, ".csv"))
timesym <- timesym[type %in% c("both_migraine", "definite_migraine")]
deftime <- copy(timesym[type == "definite_migraine"])
timesym <- data.table(variable = draws,
                      timesymboth = rnorm(1:1000, mean = timesym[type == "both_migraine", time_ictal], sd = timesym[type == "both_migraine", time_ictal_se]),
                      timesymdef = rnorm(1:1000, mean = timesym[type == "definite_migraine", time_ictal], sd = timesym[type == "definite_migraine", time_ictal_se]))

## GET GLOBAL DRAWS
prob <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = probme, source = "epi", measure_id = 5, location_id = 1, 
                  year_id = 2010, age_group_id = 22, sex_id = 3, metric_id = 3)
prob <- melt(prob, measure.vars = draws, value.name = "probable")
prob <- prob[, .(variable, probable)]
def <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = defme, source = "epi", measure_id = 5, location_id = 1, 
                 year_id = 2010, age_group_id = 22, sex_id = 3, metric_id = 3)
def <- melt(def, measure.vars = draws, value.name = "definite")
def <- def[, .(variable, definite)]
draw_dt <- merge(prob, def, by = "variable")
draw_dt[, both := probable + definite]
draw_dt[, probable := probable / both]
draw_dt[, definite := definite / both]

## CALCULATION
draw_dt <- merge(draw_dt, timesym, by = "variable")
draw_dt[, timesymprob := (timesymboth - definite * timesymdef) / probable]
draw_dt[timesymprob < 0, timesymprob := 0]
draw_dt[timesymprob > 1, timesymprob := 1]

## FORMAT AND SAVE DRAWS
draw_dt <- draw_dt[, .(variable, timesymdef, timesymprob)]
write_rds(draw_dt, paste0(save_dir, date, ".rds"))

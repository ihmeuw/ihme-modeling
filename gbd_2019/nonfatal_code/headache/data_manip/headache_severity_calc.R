##########################################################################
## Purpose: Get Severity Splits for Headache
##########################################################################

## SET-UP
rm(list=ls())
pacman::p_load(data.table, ggplot2, boot, readr, openxlsx)

## SET OBJECTS
probme <- 1
defme <- 2
draws <- paste0("draw_", 0:999)
step <- "step4"

## SOURCE FUNCTION
source(paste0("FILEPATH", "get_draws.R"))
source(paste0("FILEPATH", "get_population.R"))

## GET RESULTS
files <- list.files("FILEPATH")
files <- files[grepl("results2", files)]
dates <- substr(files, 8, 17)
dates <- gsub("_", "-", dates)
last_date <- dates[which.max(as.POSIXct(dates))]
last_date <- gsub("-", "_", last_date)
timesym <- fread("FILEPATH")

timesym <- timesym[type %in% c("probable_migraine", "definite_migraine", "probable_tth", "definite_tth")]

probsize <- (timesym[type == "probable_migraine", time_ictal] * (1 - timesym[type == "probable_migraine", time_ictal])) / ((timesym[type == "probable_migraine", time_ictal_se])^2)
defsize <- (timesym[type == "definite_migraine", time_ictal] * (1 - timesym[type == "definite_migraine", time_ictal])) / ((timesym[type == "definite_migraine", time_ictal_se])^2)
probsize_tth <- (timesym[type == "probable_tth", time_ictal] * (1 - timesym[type == "probable_tth", time_ictal])) / ((timesym[type == "probable_tth", time_ictal_se])^2)
defsize_tth <- (timesym[type == "definite_tth", time_ictal] * (1 - timesym[type == "definite_tth", time_ictal])) / ((timesym[type == "definite_tth", time_ictal_se])^2)
probsize <- round(probsize, digits = 0)
defsize <- round(defsize, digits = 0)
probsize_tth <- round(probsize_tth, digits = 0)
defsize_tth <- round(defsize_tth, digits = 0)
timesym <- data.table(variable = draws,
                      timesymprob = rbinom(n = 1000, prob = timesym[type == "probable_migraine", time_ictal], size = probsize),
                      timesymdef = rbinom(n = 1000, prob = timesym[type == "definite_migraine", time_ictal], size = defsize),
                      timesymprob_tth = rbinom(n = 1000, prob = timesym[type == "probable_tth", time_ictal], size = probsize_tth),
                      timesymdef_tth = rbinom(n = 1000, prob = timesym[type == "definite_tth", time_ictal], size = defsize_tth))
timesym$timesymprob <- timesym$timesymprob/probsize
timesym$timesymdef <- timesym$timesymdef/defsize
timesym$timesymprob_tth <- timesym$timesymprob_tth/probsize_tth
timesym$timesymdef_tth <- timesym$timesymdef_tth/defsize_tth

raw_dt <- timesym
write_rds(draw_dt, "FILEPATH")

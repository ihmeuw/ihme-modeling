##################################################
## Project: CVPDs
## Script purpose: Prep Flu net data for Joy to use in RegMod
## Date: May 25, 2021
## Author: USERNAME
##################################################
rm(list=ls())

username <- Sys.info()[["user"]]
date <- gsub("-", "_", Sys.Date())
gbd_round_id <- 7
decomp_step <- "step3"

# ARGUMENTS -------------------------------------
rm(list=ls())

# load packages, install if missing

packages <- c("data.table","magrittr","ggplot2", "lubridate", "gridExtra", "grid", "stringr",
              "msm", "wCorr", "timeDate", "dplyr", "readxl", "openxlsx")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    library(p, character.only = T, lib.loc = "/FILEPATH")
  } else {
    library(p, character.only = T)
  }
}

# Arguments -------------------------------------------------------------

date <- Sys.Date()
save_path <- "/FILEPATH/aggregated_weekly_avg_2010_to_2021.csv"

# Directories -------------------------------------------------------------

source("/FILEPATH/collapse_point.R")
source(file.path("FILEPATH/get_outputs.R"))
source(file.path("FILEPATH/get_location_metadata.R"))
hierarchy <- get_location_metadata(35, gbd_round_id = 7, decomp_step = "iterative")


# Read Data --------------------------------------------------------------

all.files <- list.files(path = "/FILEPATH/extractions/flunet/" ,pattern = "_2021.xlsx", full.names = T) # data through 2021!
temp <- lapply(all.files, read_excel, skip = 5)
flu_data_new <- rbindlist(temp)
flu_data_new <- as.data.table(flu_data_new)

all.files <- list.files(path = "/FILEPATH/extractions/flunet/" ,pattern = "_2017.xlsx", full.names = T)
temp <- lapply(all.files, read_excel, skip = 5)
flu_data_old <- rbindlist(temp)
flu_data_old <- as.data.table(flu_data_old)
# drop 2017 from files of older years so not counted twice (it's also in newer years file)
flu_data_old <- flu_data_old[Year < 2017]

flu_data <- rbind(flu_data_new, flu_data_old)

# Calculate Ratios -----------------------------------------------------
setnames(flu_data, c("Country, area or territory", "Year", "Total number of influenza positive viruses"), c("location_name", "year_id", "cases"))
flu_data[, cases := as.numeric(cases)]
small_flu_data <- flu_data[,.(location_name, year_id, `Start date`, `End date`, cases, `ILI activity`)]
small_flu_data[, `Start date`:= as.Date(`Start date`)]
small_flu_data[, month := month(`Start date`)]
small_flu_data[, week := week(`Start date`)]
small_flu_data <- small_flu_data[,.(location_name, year_id, week, month, cases)]

my_small_flu <- copy(small_flu_data) # keep 2021 when prepping for use in RegMod

# calculate the average weekly flu cases in a month so we can get an accurate ratio without worrying about different years having different numbers of weeks reported which could cause problems if tried to sum
wk_avg_flu <- my_small_flu[,.(wk_avg = mean(cases, na.rm=TRUE)),.(location_name, year_id, month)]
# drop May 2021 becuase not over yet
wk_avg_flu <- wk_avg_flu[!(year_id == 2021 & month == 5)]

wk_avg_flu_wide <- dcast(wk_avg_flu, ...~year_id, value.var = "wk_avg")

# merge on location info and remove location name so matches measles format
wk_avg_flu_wide <- merge(wk_avg_flu_wide, hierarchy[location_id != 533 & location_id != 25344,.(ihme_loc_id, location_id, super_region_id, super_region_name, location_name)], by="location_name") # not the subnats georgia and niger
wk_avg_flu_wide[, location_name := NULL]
wk_avg_flu_wide[, month := month.name[month]]


fwrite(wk_avg_flu_wide, save_path)

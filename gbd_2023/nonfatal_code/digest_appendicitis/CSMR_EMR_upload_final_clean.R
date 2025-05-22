###############################################################################
## Purpose: - Upload post-CoDCorrect death counts and EMR for by loc/year/sex/age"
###############################################################################

rm(list=ls())
# SOURCE FUNCTIONS AND LIBRARIES --------------------------------------------------
pacman::p_load(cowplot,data.table, ggplot2, RMySQL, dplyr, readr, tidyverse, openxlsx, tidyr, plyr, stringr, readxl, foreign, maptools, RColorBrewer, grid, gridExtra, ggplot2, sp, reshape2, rgdal, timeDate, scales, lubridate, lattice, viridis, zoo, ggrepel, data.table, labeling, forcats)
invisible(sapply(list.files("/share/cc_resources/libraries/current/r/", full.names = T), source))
functions <- c("get_age_metadata", "get_outputs", "get_location_metadata", "get_ids", "get_covariate_estimates", "get_envelope", "get_pct_change", "get_draws", "get_population", "get_elmo_ids", "get_bundle_version", "get_crosswalk_version", "get_cod_data")
source("FILEPATH")
library(reticulate)
library(data.table)

xw_df <- get_crosswalk_version(OBJECT)
dt_all <- read.xlsx("FILEPATH")
dt_all$input_type <- as.double(dt_all$input_type)
xw_df$input_type <- as.double(xw_df$input_type)

#only select year c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022, 2023, 2024) for CSMR
dt_csmr_selected <- dt_all %>%
  filter(year_start %in% c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022, 2023, 2024))

combined_data <- bind_rows(dt_csmr_selected, xw_df)

bundle_id <- OBJECT 
acause <- "digest_appendicitis"
save_outdir <- paste0("FILEPATH")

########################################################################################
########################################################################################

#Version1 : outlier csmr from Bolivia, Peru,from age 1-10 for both sexes, and all the ages for Zimbabwe for female
dt_combined <- as.data.table(combined_data)

df1<- dt_combined[
  measure == "mtspecific" &
    location_id %in% c(121, 123) &
    age_start >= 1 & age_end <= 10 &
    sex %in% c("Male", "Female"),
  is_outlier := 1
]

df2 <- df1[
  measure == "mtspecific" &
    location_id == 198 & 
    sex == "Female",
  is_outlier := 1
]

#check if outliered successfully 
subset_dt1 <- df1[
  measure == "mtspecific" &
    location_id %in% c(121, 123) &
    age_start >= 1 & age_end <= 15 &
    sex %in% c("Male", "Female")]

#check if outliered successfully 
subset_dt2 <- df2[
  measure == "mtspecific" &
    location_id == 198 & 
    sex == "Female"]

df2$seq <- NA
filepath <- paste0(save_outdir, "FILEPATH_", date, ".xlsx")
write.xlsx(df2, filepath, rowNames = FALSE, sheetName = "extraction")
filepath <- paste0(save_outdir, "FILEPATH_", date, ".xlsx")

#Upload  new data 
description <- paste0('DESCRIPTION') 
result <- save_crosswalk_version(OBJECT, filepath, description)
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))

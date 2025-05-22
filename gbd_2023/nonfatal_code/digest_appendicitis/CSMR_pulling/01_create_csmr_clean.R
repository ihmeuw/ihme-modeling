## Purpose: - Pull post-CoDCorrect death counts for by loc/year/sex/age:
rm(list=ls())

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
pacman::p_load(cowplot,data.table, ggplot2, dplyr, readr, tidyverse, openxlsx, tidyr, plyr, readxl, foreign, maptools, RColorBrewer, grid, gridExtra, ggplot2, sp, reshape2, rgdal, timeDate, scales, lubridate, lattice, viridis, zoo, ggrepel, data.table)
library(cowplot)
library(ggpubr)
invisible(sapply(list.files("FILEPATH", full.names = T), source))
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")


# GET ARGS ----------------------------------------------------------------
args<-commandArgs(trailingOnly = TRUE)
save_dir <- args[1]
location <-args[2]

draw_cols <- paste0("draw_", 0:999)


#define parameters
date <- gsub("-", "_", Sys.Date())
cod_corr_ver_2023 <- OBJECT

years <- c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022, 2023, 2024)
sexes <- c(1,2)
age_groups <- c(2:3, 6:20, 30:32, 34, 388, 389, 238, 235)


#pull death rates
df <- get_draws(gbd_id_type = 'cause_id',
                gbd_id = OBJECT, # appendicitis
                source = 'codcorrect',
                measure_id = 1, #death
                sex_id = c(1,2),
                location_id = location, 
                metric_id = 1, #number of deaths
                release_id = 16,
                year_id = years,
                age_group_id = age_groups,
                version_id = cod_corr_ver_2023)


#PULL POP COUNTS
pop <- get_population(age_group_id = unique(df$age_group_id), 
                      location_id = location, 
                      sex_id = sexes, 
                      year_id = years, 
                      release_id = 16)

#merge pop counts onto main dataframe by loc/sex/age/year_bucket_start
df <- merge(df, pop, by = c('location_id', 'sex_id', 'age_group_id', 'year_id'))


# CALCULATE MORTALITY RATE AS DEATH DRAWS / POP

df[, (draw_cols) := lapply(.SD, function(x) x / population ), .SDcols = draw_cols]

# COLLAPSE EACH ROW TO MEAN, UPPER, LOWER
df[, mean := rowMeans(.SD), .SDcols = draw_cols]
df$lower <- df[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
df$upper <- df[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
df <- df[, -(draw_cols), with=FALSE]


# FORMAT FOR EPI UPLOAD
# put deaths in cases column, population in sample size column
df[, seq := NA]
setnames(df, c('year_id'), c('year_start'))
df <- df[sex_id == 1, sex := 'Male']
df <- df[sex_id == 2, sex := 'Female']
df <- df[, -c('sex_id', 'population')]
df[, year_end := year_start]

#Convert the age group ids to starting age years
ages <- get_age_spans()
age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235)
ages <- ages[age_group_id %in% age_using,]
setnames(ages, c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))

df <- merge(df, ages, by = "age_group_id", all.x = TRUE)
df <- df[, -c('age_group_id')]


df[, cases := '']
df[, sample_size := '']
df[, source_type := 'Facility - inpatient']
df[, age_demographer := 1]
df[, measure := 'mtspecific']
df[, unit_type := 'Person']
df[, unit_value_as_published := 1]
df[, representative_name := 'Nationally representative only']
df[, urbanicity_type := 'Unknown']
df[, recall_type := 'Not Set']
df[, extractor := 'meiwang8']
df[, is_outlier := 0]
df[, underlying_nid := '']
df[, sampling_type := '']
df[, recall_type_value := '']
df[, input_type := '']
df[, standard_error := '']
df[, effective_sample_size := '']
df[, design_effect := '']
df[, response_rate := '']
df[, uncertainty_type_value := 95]
df[, uncertainty_type := 'Confidence interval']


upload_filepath <- paste0("FILEPATH") 
write.csv(df, upload_filepath, row.names = FALSE)
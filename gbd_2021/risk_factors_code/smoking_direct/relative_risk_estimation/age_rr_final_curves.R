#---------------------------------------------------
# Purpose: create age_specific relative risk curves
#---------------------------------------------------

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives
j <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"

code_dir <- 'FILEPATH'
output_dir <- "FILEPATH" 
save_dir <- "FILEPATH"
age_rr_dt_dir <- 'FILEPATH'

library(dplyr)
library(ggplot2)
library(data.table)
library(mrbrt001, lib.loc = "FILEPATH")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_age_metadata.R")
source(paste0(code_dir, "age_rr_utils.R"))
source("FILEPATH/helper_functions.R")
np <- import("numpy")
np$random$seed(as.integer(123))

# Set up arguments
if(interactive()){
  ro_pair <- "ihd"
} else {
  args <- commandArgs(trailingOnly = TRUE)
  ro_pair <- args[1]
}

ages <- get_age_metadata(19)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
ages <- ages[,.(age_start, age_end, age_group_id)]

# get the reference age group
data <- readRDS(paste0(save_dir, "01_template_models/", ro_pair, ".RDS"))
df_data <- data$df_data
age_ref <- df_data$age_ref %>% mean
age_ref_group <- ages[age_start <= age_ref & age_end >= age_ref, age_group_id]

# load rr draws and age pattern draws
rr_draws <- fread(paste0(output_dir, ro_pair, "_draws.csv"))
age_pattern_draws <- fread(file.path(age_rr_dt_dir, paste0("attenuation_pct_draws_", ro_pair, "_",age_ref_group,".csv")))

# change variable name
setnames(rr_draws, "draw_1000", "draw_0")

# apply attenuation factors
plot_path <- paste0(age_rr_dt_dir, "/age_spec_smoking_", ro_pair,".pdf")
age_spec_risk_curve <- apply_age_pattern(ro_pair =ro_pair, risk_curve_draws_df = rr_draws, age_pattern_draws_df = age_pattern_draws, draws_in_log = T, return_draws_log = F, plot = T, plot_path = plot_path)

# re-shape the dataset
age_spec_rr <- melt(age_spec_risk_curve, id.vars = c("exposure", "age_group_id"), variable.name = "draw", value.name = "rr")
age_spec_rr[, draw:= as.numeric(draw)-1]

# rr of 2:8 should not be used in pafs
age_spec_rr_full <- copy(age_spec_rr)
for(age_id in c(6:8)){
  temp <- age_spec_rr[age_group_id==9]
  temp[, age_group_id := age_id]
  age_spec_rr_full <- rbindlist(list(temp, age_spec_rr_full), use.names = T)
}
age_spec_rr_full[, age_group_id] %>% unique

# add sex_id
age_spec_rr_full_m <- copy(age_spec_rr_full)
age_spec_rr_full_f <- copy(age_spec_rr_full)
age_spec_rr_full_m[, sex_id := 1]
age_spec_rr_full_f[, sex_id := 2]

age_spec_rr_full <- rbindlist(list(age_spec_rr_full_m, age_spec_rr_full_f), use.names = T)

setorder(age_spec_rr_full, "exposure","draw", "sex_id", "age_group_id")

# save the draws 
write.csv(age_spec_rr_full, paste0("FILEPATH/draws_long/", ro_pair, ".csv"), row.names = F)

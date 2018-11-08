library(data.table)
library(readr)
library(stringr)
library(ggplot2)
library(boot)

rm(list=ls())


# Get settings ------------------------------------------------------------

main_dir <- commandArgs(trailingOnly=T)[1]
ihme_loc <- commandArgs(trailingOnly=T)[2]

source("settings.R")
get_settings(main_dir)

age_groups <- fread(paste0(temp_dir, "/../database/age_group_ids.csv"))
location_hierarchy <- fread(paste0(temp_dir, "/../database/location_hierarchy.csv"))

loc_name <- location_hierarchy[ihme_loc_id == ihme_loc, location_name]
loc_id <- location_hierarchy[ihme_loc_id == ihme_loc, location_id]

# source shared functions
source("modeling/helper_functions.R")


# Set up id vars for assertion checks later -------------------------------

fertility_1_id_vars <- list(ihme_loc_id = ihme_loc,
                            year_id = seq(min(years), max(years), 1),
                            age = seq(0, terminal_age, 1),
                            draw = 0:999)
fertility_5_id_vars <- list(ihme_loc_id = ihme_loc,
                            year_id = seq(min(years), max(years), 5),
                            age = seq(0, terminal_age, 5),
                            draw = 0:999)


# Prep mean fertility values ----------------------------------------------

fertility_dir <- paste0("/share/fertilitypop/fertility/gbd_2017/modeling/asfr/va", input_fertility_version, "/loop2/results/population_input")

fertility_1_data <- fread(paste0(fertility_dir, "/one_by_one/", ihme_loc, "_fert_1by1.csv"))
fertility_5_data <- fread(paste0(fertility_dir, "/five_by_five/", ihme_loc, "_fert_5by5.csv"))

fertility_1_matrix <- format_as_matrix(fertility_1_data, "value_mean", ages = fertility_1_id_vars$age)
fertility_5_matrix <- format_as_matrix(fertility_5_data, "value_mean", ages = fertility_5_id_vars$age)


# Prep sd values in log space ---------------------------------------------

# need to prep sd in log space because we are modeling fertility in log space

calc_input_sd <- function(model_age_int) {
  draws <- load_fertility_input_data(model_age_int, load_draws = T)
  draws <- draws[between(age, fertility_start_age, fertility_end_age)]
  draws[, value := log(value)]
  sd <- draws[, list(value_sd = sd(value)), by = c("ihme_loc_id", "year_id", "age")]
  return(sd)
}

fertility_1_sd <- calc_input_sd(1)
fertility_5_sd <- calc_input_sd(5)

fertility_1_matrix_sd <- format_as_matrix(fertility_1_sd, "value_sd",
                                          ages = seq(fertility_start_age, fertility_end_age, 1))
fertility_5_matrix_sd <- format_as_matrix(fertility_5_sd, "value_sd",
                                          ages = seq(fertility_start_age, fertility_end_age, 5))


# Calculate values of interest --------------------------------------------


# Save prepped data -------------------------------------------------------

write_csv(fertility_1_sd, path = paste0(temp_dir, "/inputs/fertility_sd_1.csv"))
write_csv(fertility_5_sd, path = paste0(temp_dir, "/inputs/fertility_sd_5.csv"))

save(fertility_1_matrix, fertility_5_matrix,
     fertility_1_matrix_sd, fertility_5_matrix_sd,
     file=paste0(temp_dir, "/inputs/fertility_matrix.rdata"))

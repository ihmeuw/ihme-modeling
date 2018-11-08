#######################################################################################
#' Author: 
#' 3/2/18
#' Purpose: Master script for launching duration calculations
#'          1) Setup environment
#'          2) Launch syphilis code (different as we wait the 4 phases together for an aggreagate mean duration)
#'          3) Launch duration code for other stis
#'
#' OUTPUTS: Durations by location year, with uncertainty
#'
#######################################################################################


library("ihme", lib.loc = "FILEPATH")
ihme::setup()

library(magrittr)
library(dplyr)
library(ggplot2)

source_functions(get_covariate_estimates = T, get_location_metadata = T)

# set up directoris
base_dir <- "BASE_FILEPATH"
out_dir  <- paste0(base_dir, "FILEPATH/")
root_dir <- "ROOT_DIR"

# Create new directories if they dont already exist
dir.create(out_dir)

directories <- c("EX1", "EX2", "EX3", "EX4", "EX5", "EX6", "EX17")
lapply(directories, function(inner_dir) { dir.create(paste0(out_dir, inner_dir)) })

# read in WHO duration assumptions
constants <- readr::read_csv(paste0("FILEPATH/sti_symptomatic_splits.csv"))

# Calculate 1000 HAQ draws from a binom distribution ----------------------
haq <- get_covariate_estimates(1099)

# scale HAQ from 0 - 1
haq <- haq %>% 
  mutate(mean_scaled  = (mean_value -  min(mean_value)) / (max(mean_value) - min(mean_value)),
         upper_scaled = (upper_value - min(mean_value)) / (max(mean_value) - min(mean_value)),
         lower_scaled = (lower_value - min(mean_value)) / (max(mean_value) - min(mean_value)),
         se = (upper_scaled - lower_scaled) / (2 * 1.96)) %>% 
  select(location_id, location_name, year_id, haq = mean_scaled, se)

# expand each row 1000 times so we can take 1000 draws


# function to cross two data frames. 
cross <- function(data1, data2) {
  df1_prep <- mutate(data1, merge_var = 1)
  df2_prep <- mutate(data2, merge_var = 1)
  
  left_join(df1_prep, df2_prep, by = "merge_var") %>% 
    select(-merge_var)
}

draw_names <- as.data.frame(paste0("draw_", 0:999))
draw_names <- setNames(draw_names, "draw_name")

haq_long <- cross(haq, draw_names)

# take 1000 draws of a binomial distribution
binomial_draws <- function(size, std_error, mean) {
  n <- mean * (1 - mean) / std_error ^ 2
  rbinom(size, round(n), mean) / round(n)
}

haq_long <- as.data.table(haq_long)

# size has to be size of the data set
haq_long[, haq_draw := rnorm(n = nrow(haq_long), mean = haq, sd = se)]

# Save as a csv for each location -----------------------------------------
# This cuts down on file size and facilitates parallelization

# Grab location IDs
location_data <- get_location_metadata(location_set_id = 35)
locations <- unique(location_data[level >= 3, location_id]) 

# specify a datatable (dt), location (vectorizable), and an out_dir. Verbose is optional
# if you want live progress on which locations have been saved
#   out_dir: the out directory where results are saved
#   folder: the folder in the out_dir you want to save results in
#   file_name_tail: label the files like {location_id}_FILE_NAME_TAIL, for better recognition
save_location_subset <- function(dt, location, out_dir, folder = "", file_name_tail = "", verbose = FALSE) {
  if (verbose) {
    message(paste0("Saving for location id ", location))
  }
  
  location_subset <- dt[location_id == location]
  readr::write_csv(location_subset, paste0(out_dir, folder, location, file_name_tail, ".csv"))
}

invisible(parallel::mclapply(sort(locations), function(loc) save_location_subset(dt = haq_long, location = loc, 
                                                               out_dir = out_dir, folder = "EX/",
                                                               file_name_tail = "_haq", verbose  = T)))



# Draw calculation for syphilis duration inputs -----------------------------
syphilis_names <- c("primary syphilis", "secondary syphilis", "latent syphilis", "tertiary syphilis")
syphilis <- constants[constants$infection %in% syphilis_names, ]

# get 1000 draws for EACH syphilis stage
# UNCERTAINTY ASSUMPTION: we're assuming a standard error of 10% the value on all these assumptions
se_factor <- 0.1
size      <- nrow(syphilis) * 1000 # this really is number of rows in the dataset. Things get freaky otherwise
syphilis_draws <- cross(syphilis, draw_names)

# take 1000 draws for each duration input, assuming an uncertainty of 10% of the means
# using binomial distributions for proportions and normal for the durations treated/untreated
syphilis_draws <- syphilis_draws %>% 
  mutate(sympt_draw     = binomial_draws(size, std_error = symptomatic * se_factor, mean = symptomatic),
         dur_no_rx_draw = rnorm(size, sd = duration_untreated * se_factor, mean = duration_untreated),
         dur_rx_draw    = rnorm(size, sd = duration_treated   * se_factor, mean = duration_treated),
         symp_treat_max_draw = binomial_draws(size, std_error = symptomatic_treated_in_a * se_factor, mean = symptomatic_treated_in_a),
         symp_treat_min_draw = binomial_draws(size, std_error = symptomatic_treated_in_c * se_factor, mean = symptomatic_treated_in_c)) %>% 
  select(infection, sex, draw_name, sympt_draw:symp_treat_min_draw, seroreversion)

rnorm_prop <- function(n, mean, sd) {
  draws <- rnorm(n, mean, sd)
  if_else(draws > 1, difference(draws), draws)
}

difference <- function(x) {
  difference <- x - 1 
  1 - difference
}

syphilis_draws <- as.data.table(syphilis_draws)
syphilis_draws[is.na(sympt_draw), sympt_draw := rnorm_prop(1000, 1, se_factor)]


# DIAGNOSTICS
ggplot(syphilis_draws) +
  geom_histogram(aes(sympt_draw, fill = as.factor(paste(infection, sex))))

readr::write_csv(syphilis_draws, paste0(out_dir, "EX/syphilis_draws.csv"))


# Draw calculation for other stis duration inputs -------------------------
other <- constants[!constants$infection %in% c(syphilis_names, "syphilis"), ]

other_draws <- cross(other, draw_names)
size <- nrow(other_draws)

size <- 6000

other_draws <- other_draws %>% 
  mutate(sympt_draw     = binomial_draws(size, std_error = symptomatic * se_factor, mean = symptomatic),
         dur_no_rx_draw = rnorm(size, sd = duration_untreated * se_factor, mean = duration_untreated),
         dur_rx_draw    = rnorm(size, sd = duration_treated   * se_factor, mean = duration_treated),
         symp_treat_max_draw = binomial_draws(size, std_error = symptomatic_treated_in_a * se_factor, mean = symptomatic_treated_in_a),
         symp_treat_min_draw = binomial_draws(size, std_error = symptomatic_treated_in_c * se_factor, mean = symptomatic_treated_in_c)) %>% 
  select(infection, sex, draw_name, sympt_draw:symp_treat_min_draw)

readr::write_csv(other_draws, paste0(out_dir, "EX/other_sti_draws.csv"))

# Launch syphilis calculations by location --------------------------------

submit_for <- function(location_id, root_dir, code, job_name) {
  jobname <- paste0(job_name, location_id)
  args <- list(out_dir, location_id)
  
  ihme::qsub(jobname, code = paste0(root_dir, code), pass = args,
             slots = 3, submit = T, proj = "proj_custom_models",
             shell = paste0(j_root, "SHELL_SCRIPT"))
}

invisible(lapply(sort(locations), submit_for, root_dir = root_dir, code = "01_a_syphilis_duration.R", job_name = "syph_"))


# Launch other sti calculations by location -------------------------------

invisible(lapply(sort(locations), submit_for, root_dir = root_dir, code = "01_b_other_duration.R", job_name = "other_"))


# Launch script to calculate and upload durations -------------------------

# need to wait for jobs to finish 

bundle_ids <- c(96, 97, 98, 453)
infections <- c("chlamydia",   "gonorrhea" ,  "trichomonas", "syphilis")

submit_for <- function(acause, bundle_id, root_dir, code) {
  jobname <- paste0("up_", acause)
  args <- list(out_dir, acause, bundle_id)
  
  ihme::qsub(jobname, code = paste0(root_dir, code), pass = args,
             slots = 10, submit = T, proj = "proj_custom_models",
             shell = paste0(j_root, "SHELL_SCRIPT"))
}

invisible(lapply(infections, submit_for, bundle_id = bundle_ids, root_dir = root_dir, code = "02_duration_means.R"))




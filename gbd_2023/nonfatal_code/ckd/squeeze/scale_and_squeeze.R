# ------------------------------------------------------------------------------
# Project: Non-fatal GBD

# Purpose: 
# Squeeze estimates for stages 3, 4 and 5 into the stage 3-5 envelope.
# Squeeze the CKD due to diabetes type 1 and type 2 models into the CKD due to diabetes parent model.
# Scale CKD due to hypertension, glom, and other (etiologies) models to sum to 100% across YASL.

# This script will take any number of MEID's and scale them to one.
# 1. Get all draws
# 2. Draw Level Sum
# 3. Cause + Draw Level division from Draw Sums
# 4. Save as CSV in specified directory
# ------------------------------------------------------------------------------

# ---LOAD LIBRARIES-------------------------------------------------------------

suppressMessages(library(tidyverse))
library(purrr)
library(stringr)

# ---PARSE ARGUMENTS------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)

location_id <- as.numeric(args[1])
codebook_path <- args[2]
testing <- args[3]
release_id <- as.numeric(args[4])
general_funcs <- args[5]
squeeze_funcs <- args[6]
output_dir <- args[7]
best_version_id <- args[8]

# ---SOURCE FUNCTIONS-----------------------------------------------------------

source(general_funcs)
source(squeeze_funcs)
source("FILEPATH/get_draws.R")
source("FILEPATH/get_demographics.R")

# ---LOG SETUP MESSAGES---------------------------------------------------------

message("Starting scale and squeeze")
message(paste("Path to map:", codebook_path))
message(paste("Testing:", testing))
message(paste("Location ID:", location_id))

# ---READ CODEBOOK--------------------------------------------------------------

mapping <- read.csv(codebook_path, stringsAsFactors = FALSE)

# ---GET CODEBOOK PARAMETERS----------------------------------------------------
# This is where errors are most likely to appear. Causes have different
# requirements for get draws
# ------------------------------------------------------------------------------

message("Getting parameters from mapping")
# Use get_demographics here because it is cleaner than using get_age_metadata.
gbd_demographics <- get_demographics(gbd_team = "epi", release_id = release_id)
age_ids <- gbd_demographics$age_group_id
sex_ids <- as.numeric(unlist(strsplit(as.character(mapping$sex_id[1]),',')))

if (mapping$measure_id[1] == "None") {
  measure_ids <- "None"
} else {
  measure_ids <- as.numeric(unlist(strsplit(as.character(mapping$measure_id[1]),',')))
}

if (testing) {
  message("Testing with")
  draws <- 100
  downsampling <- TRUE
  message(paste("Number of draws:", draws))
  message(paste("Downsampling:", downsampling))
} else {
  draws <- "None"
  message("Number of draws: default 1000")
  downsampling <- NULL
}

message("Age group ids:")
message(paste(age_ids, " "))
message("Sex ids: ")
message(paste(sex_ids, collapse =" "))
message(paste("Measure ids:", measure_ids, ""))
message(paste("Release_id:", release_id))

message("Filtering parent and child mapping sections")
child_map <- mapping %>% dplyr::filter(parent == "child")
message("Modelable entity ids:")
message(paste(child_map$gbd_id, " "))
message(paste("GBD type:", child_map$gbd_id_type[1]))

# ---GET DRAWS------------------------------------------------------------------
# Getting draws for all etiologies and envelopes.
# ------------------------------------------------------------------------------

message("Getting draws")
if (best_version_id) {
  version_ids <- "None"
  message("Pulling best model versions")
} else {
  version_ids <- child_map$model_version_id
  message("Pulling specified model versions")
  message(paste(version_ids, " "))
}

draws_list <- get_list_of_draws(gbd_ids = child_map$gbd_id,
                                gbd_id_type = child_map$gbd_id_type[1],
                                location = location_id,
                                age_ids = age_ids,
                                sex_id = sex_ids,
                                source = child_map$source[1],
                                measure_id = measure_ids,
                                release_id = release_id,
                                n_draws = draws,
                                downsample = downsampling,
                                version_ids = version_ids)

message('Model version ids are')
for (item in seq_along(draws_list)) {
  message(unique(draws_list[[item]]$model_version_id))
}

# ---SCALE DRAWS TO ONE---------------------------------------------------------

message("Scaling draws")
draws_list <- scale_draws_to_one_and_return_draws(draws_list,
                                                  return_wide = FALSE)

# ---SQUEEZING DRAWS -----------------------------------------------------------

if ("parent" %in% mapping$parent) {
  # Model will run a squeeze only if "parent" exists in the mapping file column
  # parent
  message("Has a parent model, running squeeze, not a scale")
  parent_map <- mapping %>% filter(parent == "parent")

  if (best_version_id) {
    version_ids <- "None"
    message("Pulling best model versions")
  } else {
    version_ids <- parent_map$model_version_id
    message("Pulling specified model versions")
    message(version_ids)
  }

  parent_draws <- get_draws(gbd_id = parent_map$gbd_id,
                            gbd_id_type = parent_map$gbd_id_type[1],
                            location_id = location_id,
                            age_group_id = age_ids,
                            sex_id = sex_ids,
                            source = parent_map$source[1],
                            measure_id = measure_ids,
                            release_id = release_id,
                            n_draws = draws,
                            downsample = downsampling,
                            version_id = version_ids)

  message('Model version ids for Envelope is')
  message(unique(parent_draws$model_version_id))
  
  # Processing squeeze and scaling
  parent <- convert_draws_long(parent_draws)
  # Send scaled draws and multiply by the parent to get the squeezed proportions
  draws_list <- squeeze_scaled_draws(child_draws_dataframe_list = draws_list,
                                    parent_draw_dataframe = parent)

} else {
  message("Has no parent model in map, running scale, not squeeze")
}

# ---Convert draws back to wide format for upload-------------------------------

message("Converting back to wide format")
draws_wide <- map(.x = draws_list,
                  .f = convert_draws_wide)

# ---SAVING DRAWS---------------------------------------------------------------

# Creates directory to save if it doesn't exist already
for (i in paste0(output_dir, child_map$target_me_id)) {
  if (!file.exists(i)) {
    dir.create(i, recursive=T)
    message(paste('Creating', i))
  } else {
    message(paste(i ,'already exists'))
  }
}

message("Writing squeezed estimates to disk")
# Saves using a custom function, hopefully faster but in practice no different
save <- map2(
  .x = draws_wide,
  .y = paste0(output_dir, child_map$target_me_id, "/"),
  .f = ~save_draws_to_file_with_measure_as_csv(dataframe = .x,
                                  directory_to_save = .y,
                                  change_measure_id = "None"))

message("Squeeze/Scale draws complete")
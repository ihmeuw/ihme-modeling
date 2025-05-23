###########################################################################################
##                                                                                       ##
## Purpose: Scale subnational stillbirth estimates to national estimates                 ##
##                                                                                       ##
## Rationale:                                                                            ##
##    - Stillbirth data availability makes national estimates more reliable              ##
##    - Rescaling enforces consistency and relies on the national estimates              ##
##      (rather than aggregating the subnational estimates to create national estimates) ##
##                                                                                       ##
###########################################################################################

rm(list = ls())
Sys.umask(mode = "0002")

library(data.table)
library(readr)
library(stringr)

library(mortcore)
library(mortdb)

user <- "USERNAME"

##################
## Get settings ##
##################

args <- commandArgs(trailingOnly = TRUE)
new_settings_dir <- args[1]

if (interactive()) {
  version_estimate <- "Run id"
  main_std_def <- "28_weeks"
  new_settings_dir <- paste0("FILEPATH/new_run_settings_", main_std_def, ".csv")
}

load(new_settings_dir)
list2env(new_settings, envir = environment())

if (interactive()) {

  parent <- "BRA"

} else {

  parent <-  ifelse(is.na(as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))), commandArgs(trailingOnly = T)[2],
                    fread(paste0(estimate_dir, version_estimate, "/FILEPATH/parent_locs.csv"))
                    [as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID")), loc])

}

source(paste0(working_dir, "FILEPATH/transformation_functions.R"))

###################
## Get locations ##
###################

all_locs <- mortdb::get_locations(level = "estimate", gbd_year = gbd_year)

model_locs <- fread(paste0(estimate_dir, version_estimate, "/FILEPATH/locs_w_regions.csv"))

parent_locs <- unique(model_locs[grepl("_", ihme_loc_id), gsub("_.*", "", ihme_loc_id)])

rake_locs <- parent_locs[!parent_locs %in% agg_locs]

child_locs <- model_locs[grepl(parent, ihme_loc_id), ihme_loc_id]
child_locs <- child_locs[!child_locs %in% c("CHN_354", "CHN_361", "")]

#############################
## Get parent & child data ##
#############################

child_data <- assertable::import_files(
  paste0(estimate_dir, version_estimate, "/FILEPATH/gpr_", child_locs, "_sim_", main_std_def, ".csv"),
  FUN = fread
)

child_data[, ihme_loc_id := str_remove(ihme_loc_id, "[b]")]
child_data[, ihme_loc_id := str_remove_all(ihme_loc_id, "[']")]

child_data[, year_id := floor(year)]

child_data <- merge(
  child_data,
  all_locs[, c("ihme_loc_id", "level")],
  by = "ihme_loc_id",
  all.x = TRUE
)

#######################################
## Transform into stillbirth numbers ##
#######################################

pred_data <- fread(
  paste0(estimate_dir, version_estimate, "/FILEPATH/stillbirth_prediction_data_", main_std_def, ".csv")
)

pred_data <- pred_data[, c("ihme_loc_id", "year", "q_nn_med", "births"), with = FALSE]

pred_data <- merge(
  pred_data,
  all_locs[, c("ihme_loc_id", "location_id")],
  by = c("ihme_loc_id"),
  all.x = TRUE
)

child_data <- merge(
  child_data,
  pred_data,
  by = c("ihme_loc_id", "year"),
  all.x = TRUE
)

if (model == "SBR/NMR") {

  child_data[, sb := lograt_to_sb(mort, q_nn_med, births)]

} else if (model == "SBR + NMR") {

  child_data[, sb := (mort - q_nn_med) * births]

} else if (model == "SBR") {

  child_data[, sb := exp(mort)]

}

data <- copy(child_data)

#################
## Aggregating ##
#################

if (parent %in% agg_locs) {

  most_granular <- max(data$level)

  if (parent == "CHN") parent <- "CHN_44533"

  if (parent == "GBR") {

    agg_input <- data[level == most_granular | location_id %in% c(433, 434, 4636), c("location_id", "year", "sim", "sb")]

  } else {

    agg_input <- data[level == most_granular, c("location_id", "year", "sim", "sb")]

  }

  data_sub <- data[, c("location_id", "year", "sim", "ihme_loc_id", "q_nn_med", "births")]

  raked <- mortcore::agg_results(
    agg_input,
    id_vars = c("location_id", "year", "sim"),
    value_vars = "sb",
    end_agg_level = 3,
    loc_scalars = FALSE,
    tree_only = parent,
    location_set_id = 21,
    gbd_year = gbd_year
  )

  raked <- raked[!(location_id %in% unique(agg_input$location_id))]

  raked <- merge(
    raked,
    data_sub,
    by = c("location_id", "year", "sim"),
    all.x = TRUE
  )

}

############
## Raking ##
############

if (parent == "GBR" & split_rake_agg_GBR) {

  rake_locs <- append(rake_locs, "GBR")

  data <- rbind(
    data[!(ihme_loc_id %in% c("GBR", "GBR_4749"))],
    raked,
    fill = TRUE
  )

}

if (parent %in% rake_locs) {

  if (parent == "CHN") {

    raked <- mortcore::scale_results(
      data[, !c("level")],
      id_vars = c("location_id", "year", "sim"),
      value_var = "sb",
      location_set_id = 21,
      gbd_year = gbd_year,
      exclude_parent = "CHN"
    )

  } else {

    raked <- mortcore::scale_results(
      data[, !c("level")],
      id_vars = c("location_id", "year", "sim"),
      value_var = "sb",
      location_set_id = 21,
      gbd_year = gbd_year
    )
  }

}

###############################################
## Add back variables and output new version ##
###############################################

## make value in same format as gpr output from previous step

if (model == "SBR/NMR") {

  raked[, mort := sb_to_lograt(sb, q_nn_med, births)]

} else if (model == "SBR + NMR") {

  raked[, mort := (sb / births) + q_nn_med]

} else if (model == "SBR") {

  raked[, mort := log(sb)]

}

for (i in unique(raked$ihme_loc_id)) {

  readr::write_csv(
    raked[raked$ihme_loc_id == i, c("ihme_loc_id", "year", "sim", "mort")],
    paste0(estimate_dir, version_estimate, "/FILEPATH/gpr_", i , "_sim_", main_std_def, ".csv")
  )

}

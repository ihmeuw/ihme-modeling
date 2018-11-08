################################################################################
## Description: Submit shells scripts to run all code necessary to produce
##              estimates of adult mortality. This code must be run on the
##              cluster and assumes that you have already done everything up
##              through compiling the data (i.e. producing 'raw.45q15.txt')
################################################################################


############
## Settings
############
rm(list=ls());
library(foreign); library(data.table); library(readr); library(assertable)
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")


start <- 1
end <- 10

singularity_image <- "FILEPATH"
gbd_year <- 2017
upload_estimates <- T
mark_best <- F
cluster_project <- "CLUTER_PROJECT"
run_comment <- "COMMENT HERE"

hiv.uncert <- 0       ## 1 or 0 logical for whether we're running with draws of HIV
hiv.update <- 0      ## Do you need to update the values of the hiv sims? Affects 01b
num.holdouts <- 100
hivdraws <- 250     ## How many draws to run, must be a multiple of 25 (for job submission)
if (hiv.uncert == 0) hivdraws <- 1
hivscalars <- T  # use scalars for HIV (only T on the first run of the loop e.g. use on delta 1, but not on delta 2)
#these options are for the HIV sims - but the code isn't set up to do parameter selection and HIV sims at the same time, which would be way too many jobs

if (gbd_year == 2010) { # assumes subsequent GBD round ids after 2015 are yearly, and the ids increment in order
  gbd_round_id <- 1
} else if (gbd_year == 2013) {
  gbd_round_id <- 2
} else if (gbd_year >= 2015) {
  gbd_round_id <- gbd_year - 2012
} else {
  stop(paste0("gbd_year: ", gbd_year, " is invalid, please double-check."))
}

##############
## Helper Functions
##############

create_linear_model_targets = function(type, locations, gbd_year) {
  if (type == "national_all") {
    targets = data.table(locations)
    targets[level != 3, primary := F]
    targets[(level == 3 & location_id != 6) | location_id %in% c(354, 361, 44533), primary := T]
    targets[, secondary := T]
    targets = targets[, list(location_id, ihme_loc_id, primary, secondary)]

  } else if (type == "prev_current_rounds") {

    # get previous round locations. These locations will be the primary locations
    locations_prev <- as.data.table(get_locations(level = "estimate", gbd_type = "ap_old", gbd_year = gbd_year - 1))

    if (gbd_year == 2016) {
      locations_prev <- get_locations(level = "estimate", gbd_year = gbd_year - 1)
      kenya_prov <- as.data.table(get_locations())
      kenya_prov <- kenya_prov[substr(ihme_loc_id, 1, 3) == "KEN" & level == 4, ]
      locations_prev <- rbind(locations_prev, kenya_prov, use.names = T)

      locations_curr <- as.data.table(get_locations(gbd_type = "ap_old", level = "estimate", gbd_year = 2016))
      old_ap <- locations_curr[locations_curr$ihme_loc_id == "IND_44849", ]
      rm(locations_curr)
      locations_prev <- rbind(locations_prev, old_ap, use.names = T)

    } else if (gbd_year == 2017) {
      locations_prev = locations_prev[!grepl("SAU_", ihme_loc_id)]
      locations <- as.data.table(get_locations(gbd_type = "ap_old", level = "estimate", gbd_year = gbd_year))
    }

    locations_prev$primary <- TRUE

    # Current round's locations, these will be the secondary locations
    locations$secondary <- TRUE

    targets <- as.data.table(merge(locations, locations_prev, by = c("location_id", "ihme_loc_id"), all = T))
    targets <- targets[, list(location_id, ihme_loc_id, primary, secondary)]
    targets[is.na(primary), primary := FALSE]

  } else {
    stop(paste0("The argument passed to type: ", type, ", did not match an existing option."))
  }

  return(unique(targets))
}

##############
## Create IDs and establish parent-child relationships
##############

# get best 5q0 and ddm run_id for use in data formatting step
m5q0_version_id <- get_proc_version(model_name = "5q0", model_type = "estimate", run_id = "best", gbd_year = gbd_year)
ddm_version_id <- get_proc_version(model_name = "ddm", model_type = "estimate", run_id = "best", gbd_year = gbd_year)

# Get best 45q15 data run_id
child_version_data_run_id <- get_proc_version(model_name = "45q15", model_type = "data", run_id = "best", gbd_year = gbd_year)
version_id <- gen_new_version(model_name = "45q15",
                                     model_type = "estimate",
                                     comment = run_comment)

# get most recent population version id
population_version_id <- get_proc_version(model_name = "population", model_type = "estimate", run_id = "best", gbd_year = gbd_year)

# Link current run to current best 45q15 data run_id
gen_parent_child(child_process = "45q15 estimate",
                 child_id = version_id,
                 parent_runs = list("45q15 data" = child_version_data_run_id,
                 "population estimate" = population_version_id,
                 "ddm estimate" = ddm_version_id,
                 "5q0 estimate" = m5q0_version_id)
)

archive_outlier_set(model_name = "45q15", estimate_run_id = version_id)

# Create directory structure
output_dir <- "FILEPATH"

# pull and save covariates for use in 01b and for versioning
lag_distributed_income_per_capita <- get_covariate_estimates(covariate_id = 57, gbd_round_id = gbd_round_id)
education_yrs_per_capita <- get_covariate_estimates(covariate_id = 33, gbd_round_id = gbd_round_id)
hiv <- get_covariate_estimates(covariate_id = 1196, gbd_round_id = gbd_round_id)

write_csv(hiv, path = "FILEPATH")
write_csv(lag_distributed_income_per_capita, path = "FILEPATH")
write_csv(education_yrs_per_capita, path = "FILEPATH")

# version external inputs
external_input_version_ids = list(ldi_pc = unique(lag_distributed_income_per_capita[, model_version_id]),
                                  mean_edu = unique(education_yrs_per_capita[, model_version_id]),
                                  hiv = unique(hiv[, model_version_id])
                                  )

# gen_external_input_map(process_name = "45q15 estimate", run_id = version_id, external_input_versions = external_input_version_ids)

## get countries we want to produce estimates for
codes <- get_locations(gbd_type = "ap_old", level = "estimate", gbd_year = gbd_year)
old_ap <- codes[codes$ihme_loc_id == "IND_44849", ]
write.csv(codes, "FILEPATH")
codes <- codes[codes$location_id == 44849 | codes$level_all == 1, ] # Eliminate those that are estimates but that are handled uniquely (ex: IND six minor territories)
codes$region_name <- gsub(" ", "_", gsub(" / ", "_", gsub(", ", "_", gsub("-", "_", codes$region_name))))
codes <- merge(codes, data.frame(sex=c("male", "female")))
codes <- codes[order(codes$region_name, codes$ihme_loc_id, codes$sex), ]

# save population for use in the run
population <- get_population(location_id = unique(codes$location_id), year_id = 1950:gbd_year, sex_id = c(1:2), age_group_id = c(8:16), run_id = population_version_id)
assert_values(data = population, colnames = "population", test = "gt", test_val = 0)
# test that all location/sex/year/age combinations needed exist for population (old andhra prdesh is excluded in this test as this data is added on in 01b_format_data.r)
pop_ids_to_check <- list(location_id = unique(codes[codes$location_id != 44849, "location_id"]), sex_id = c(1:2), year_id = 1950:gbd_year, age_group_id = c(8:16))
assert_ids(data = population, id_vars = pop_ids_to_check)

write_csv(population, "FILEPATH")

# 2015 locations plus Kenya provinces, to be used in 02_fit_prediction_model
locations_2015_ken <- get_locations(level = "estimate", gbd_year = 2015)
kenya_prov <- as.data.table(get_locations())
kenya_prov <- kenya_prov[substr(ihme_loc_id, 1, 3) == "KEN" & level == 4, ]
locations_2015_ken <- rbind(locations_2015_ken, kenya_prov, use.names = T)
locations_2015_ken <- rbind(locations_2015_ken, old_ap, use.names = T)
write.csv(locations_2015_ken, "FILEPATH")

# Saves a file to be used in 02_fit_prediction_model so it can determine
# which set of locations to run in the primary linear model
# and which set of locations to run in the secondary linear model.
# In the 02 code, locations with results from the primary model overwrite
# the results from the secondary model
write_csv(x = create_linear_model_targets(type = "prev_current_rounds",
                                      locations = codes,
                                      gbd_year = gbd_year),
          path = "FILEPATH")


###############
## Submit jobs
###############
# Step 1: Format data
if (start <= 1 & end >= 1) {
  # Format the input data
  jname <- "am01b"
  slots <- 5
  args <- list(version_id, hiv.uncert, hiv.update, hivscalars, child_version_data_run_id, gbd_year, working_dir, ddm_version_id, m5q0_version_id, gbd_round_id)
  shell <- singularity_image
  script <- paste0(working_dir, "01b_format_data.r")
  qsub(jname, script, pass = args, slots = slots, submit = T,
       proj = cluster_project, shell = shell)

  # calculate data density and select parameters for input data
  jname <- "am01c"
  slots <- 5
  holds <- "am01b"
  args <- list(version_id, ddm_version_id)
  shell <- paste0(working_dir, "python3_shell.sh")
  script <- paste0(working_dir, "calculate_data_density_select_parameters.py")
  qsub(jname, script, hold = holds, pass = args, slots = slots, submit = T,
      proj = cluster_project, shell = shell)

}

# Step 2: First stage linear model
step2_jobs <- c()
if (start <= 2 & end >= 2) {
  slots <- 5
  holds <- "am01c"
  shell <- singularity_image
  script <- paste0(working_dir, "02_fit_prediction_model.r")

  for (i in 1:(hivdraws)) {
    jname <- paste("am02", i, sep="_")
    args <- list(version_id, i, hiv.uncert, gbd_year, "FILEPATH")
    qsub(jname, script, hold = holds, pass = args, slots = slots, submit = T,
         proj = cluster_project, shell = shell)

    step2_jobs <- c(step2_jobs, jname)
  }
}

# Step 7: Second Stage Model
step7a_jobs <- c()
if (start <= 7 & end >= 7) {

  # Submit second stage model
  slots <- 4
  holds <- "am02_1"
  shell <- singularity_image
  script <- paste0(working_dir, "07a_fit_second_stage.R")

  for (i in 1:(hivdraws)) {
    jname <- paste("am07a", i, sep = "_")
    args <- list(version_id, i, hiv.uncert, gbd_year)

    qsub(jname, script, hold = holds, pass = args, slots = slots, submit = T,
         proj = cluster_project, shell = shell)

    step7a_jobs <- c(step7a_jobs, jname)
  }
}


############
## Run GPR for each country-sex
#note: make sure chosen draw files have been made for new locations
############
if(hivdraws == 1) {
  hhh <- 1
} else {
  hhh <- hivdraws / 25
}

step7b_jobs <- c()
if (start <= 7.5 & end >= 7.5) {
  slots <- 3
  if (length(step7a_jobs) == 0) {
    holds = NULL
  } else {
    holds <- paste(step7a_jobs, collapse = ",")
  }
  shell <- paste0(working_dir, "python_shell.sh")
  script <- paste0(working_dir, "07b_fit_gpr.py")

  for (cc in sort(unique(codes$ihme_loc_id))) {
    for (i in 1:hhh) {
      jname <- paste("am07b", cc, i, sep = "_")
      args <- list(version_id, cc, hiv.uncert, i)
      qsub(jname, script, hold = holds, pass = args, slots = slots, submit = T,
           proj = cluster_project, shell = shell)

      step7b_jobs <- c(step7b_jobs, jname)
    }
  }
}

############
## Compile GPR results
############
step8_jobs <- c()
step8a_jobs <- c()
if (start <= 8 & end >= 8) {
  # Calling the raking code, merge on stage 1 and 2 results for locations being raked
  # Should only be called on parent locations
  parents <- codes[codes$level_1 == 1 & codes$level_2 == 0, ]
  # for(cc in unique(parents$ihme_loc_id)) {
  for(cc in unique(parents$ihme_loc_id)) {
    jname <- paste("am08", cc, sep="_")
    slots <- 5
    if (cc ==  "GBR") {
      slots <- 8
    }

    if (length(step7b_jobs) == 0) {
      holds = NULL
    } else {
      holds <- paste(step7b_jobs, collapse = ",")
    }

    args <- list(version_id, cc, hiv.uncert, gbd_round_id, gbd_year)

    shell <- singularity_image
    script <- paste0(working_dir, "08_rake_gpr_results.R")

    qsub(jname, script, hold = holds, pass = args, slots = slots, submit = T,
         proj = cluster_project, shell = shell)

    step8_jobs <- c(step8_jobs, jname)
  }

  # compile and merge on stage 1 and 2 results to all locations. Saves unraked versions of raked locations
  for (ihme_loc_id in unique(codes$ihme_loc_id)) {
  # for (ihme_loc_id in c("MEX_4660")) { # rerun jobs that fail
    Sys.sleep(0.10)
    jname <- paste("am08a", ihme_loc_id, sep="_")
    slots <- 5
    if (length(step8_jobs) == 0) {
      holds = NULL
    } else {
      holds <- paste(step7b_jobs, collapse = ",")
    }

    args <- list(output_dir, ihme_loc_id)

    shell <- singularity_image
    script <- paste0(working_dir, "08a_compile_unraked_gpr_results.R")

    qsub(jname, script, hold = holds, pass = args, slots = slots, submit = T,
         proj = cluster_project, shell = shell)

    step8a_jobs <- c(step8a_jobs, jname)
  }
  step8_jobs <- c(step8_jobs, step8a_jobs)

  jname <- "am08b"
  slots <- 5
  holds <- paste(step8_jobs, collapse = ",")
  args <- list(version_id, hiv.uncert)

  shell <- singularity_image
  script <- paste0(working_dir, "08b_compile_all.r")

  qsub(jname, script, hold = holds, pass = args, slots = slots, submit = T,
       proj = cluster_project, shell = shell)
}

##############
## Make graphs
##############
if (start <= 9 & end >= 9) {

  jname <- "am09"
  slots <- 2
  holds <- "am08b"
  args <- list(version_id, child_version_data_run_id, ddm_version_id)
  shell <- singularity_image
  script <- paste0(working_dir, "graph_all_stages_plus_opposite_sex.r")

  qsub(jname, script, hold = holds, pass = args, slots = slots, submit = T,
       proj = "proj_mortenvelope", shell = shell)

}

##############
## Upload estimates to database
##############
if (start <= 10 & end >= 10 & upload_estimates == T) {
  jname <- "am10"
  slots <- 2
  holds <- "am08b"
  args <- list(version_id, child_version_data_run_id, output_dir, gbd_year)
  shell <- singularity_image
  script <- paste0(working_dir, "10_upload_results.R")

  qsub(jname, script, hold = holds, pass = args, slots = slots, submit = T,
       proj = cluster_project, shell = shell)
}

if (mark_best & start <= 10 & end >= 10) {
  update_status(model_name = "45q15", model_type = "estimate", run_id = version_id, new_status= "best")
}

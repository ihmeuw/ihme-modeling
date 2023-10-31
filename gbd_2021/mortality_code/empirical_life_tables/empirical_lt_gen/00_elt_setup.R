
# Meta --------------------------------------------------------------------

# Title: Set up ELT process
# Description:
#  1) Create directories
#  2) Copy external files


# Set environment ---------------------------------------------------------

rm(list = ls())


# Load libraries ----------------------------------------------------------

library(data.table)
library(argparse)
library(assertable)

library(mortdb, lib.loc = "FILEPATH")


# Set parameters ----------------------------------------------------------

# Get input arguments
parser <- ArgumentParser()
parser$add_argument("--version_id", type = "integer", required = TRUE,
                    help = "The version_id for this run")

args <- parser$parse_args()
empir_run_id <- args$version_id

# Set global script parameters
root <- "FILEPATH"

# Get process lineage
parent_lineage <- get_proc_lineage("life table empirical", "data", run_id = empir_run_id)
parent_pop_run <- parent_lineage[parent_process_name == "population estimate" & exclude == 0, parent_run_id]
parent_ddm_run <- parent_lineage[parent_process_name == "ddm estimate" & exclude == 0, parent_run_id]

parent_lineage_ddm <- get_proc_lineage("ddm", "estimate", run_id = parent_ddm_run)
parent_vrp_run <- parent_lineage_ddm[parent_process_name == "death number empirical data" & exclude == 0, parent_run_id]



# Create folder structure -------------------------------------------------

# Set working folders
main_input_folder <- paste0(root, "FILEPATH")
main_folder       <- paste0("FILEPATH", empir_run_id)
run_input_folder  <- paste0(main_folder, "/inputs")
run_output_folder <- paste0(main_folder, "/outputs")

# Generate folder structure
Sys.umask("002")
dir.create(main_folder)
dir.create(run_input_folder)
dir.create(run_output_folder)
dir.create(paste0(run_output_folder, "/loc_specific"))


# Prep external files -----------------------------------------------------

# copy J files to share
copy_results <- file.copy(
  paste0(main_input_folder, c("/empir_lt_db/inputs/hmd_ax_extension.csv", "/ax_par.dta")),
  paste0(run_input_folder, c("/hmd_ax_extension.csv", "/ax_par_from_qx.dta"))
)

if (!all(file.exists(paste0(run_input_folder, c("/hmd_ax_extension.csv", "/ax_par_from_qx.dta"))))) {
  stop("Problem copying files from j")
}

# Save age pooling data from population
pop_age_pools <- fread(paste0(
  "FILEPATH", parent_pop_run, "/versions_best.csv"
))

setnames(pop_age_pools, "drop_age", "pop_pool_age")
pop_age_pools <- pop_age_pools[, .(ihme_loc_id, pop_pool_age)]

# Assert that we only have one age pool for each location
dup_age_pools <- pop_age_pools[, .N, by = "ihme_loc_id"][N > 1]
if (nrow(dup_age_pools) > 0) {
  stop(paste0(
    "More than one population age pool exists for the following locations:\n",
    paste0(capture.output(dup_age_pools), collapse = "\n")
  ))
}

readr::write_csv(pop_age_pools, paste0(run_input_folder, "/pop_age_pools.csv"))

# Get VR prep pre-split data ----------------------------------------------

vrp_run_dir <- fs::path("FILEPATH", parent_vrp_run)

noncod_pre_split <- fread(fs::path(vrp_run_dir, "outputs/noncod_VR_no_shocks.csv"))
cod_pre_split <- fread(fs::path(vrp_run_dir, "outputs/cod_VR_no_shocks.csv"))

vrp_data <- rbindlist(list(noncod = noncod_pre_split, cod = cod_pre_split), idcol = "origin")
readr::write_csv(vrp_data, fs::path(run_input_folder, "vrp_pre_split.csv"))


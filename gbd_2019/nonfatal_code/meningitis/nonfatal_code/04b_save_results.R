#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code specifically for save_results
## Author:		
## Last updated:	2/27/2019
## Description:	Save results parallelization of 04a_dismod_prep_wmort
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
library(argparse, lib.loc = "filepath")
library(data.table)

# Get arguments from parser
parser <- ArgumentParser()
parser$add_argument("--root_j_dir", help = "base directory on J", default = NULL, type = "character")
parser$add_argument("--root_tmp_dir", help = "base directory on clustertmp", default = NULL, type = "character")
parser$add_argument("--date", help = "timestamp of current run (i.e. 2014_01_17)", default = NULL, type = "character")
parser$add_argument("--step_num", help = "step number of this step (i.e. 01a)", default = NULL, type = "character")
parser$add_argument("--step_name", help = "name of current step (i.e. first_step_name)", default = NULL, type = "character")
parser$add_argument("--hold_steps", help = "steps to wait for before running", default = NULL, nargs = "+", type = "character")
parser$add_argument("--last_steps", help = "step numbers for final steps that you are running in the current run (i.e. 04b)", default = NULL,  nargs = "+", type = "character")
parser$add_argument("--code_dir", help = "code directory", default = NULL, type = "character")
parser$add_argument("--in_dir", help = "directory for external inputs", default = NULL, type = "character")
parser$add_argument("--cause", help = "potential values are: meningitis_pneumo, meningitis_hib, meningitis_meningo, meningitis_other", default = NULL, type = "character")
parser$add_argument("--group", help = "potential values are: hearing, vision, and epilepsy", default = NULL, type = "character")
parser$add_argument("--state", help = "healthstate in in_dir/04b_outcome_prev_womort/meningitis_dimension.csv", default = NULL, type = "character")
parser$add_argument("--ds", help = "specify decomp step", default = 'step1', type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)

# directory for output on the J drive
out_dir <- paste0(root_j_dir, "/", step_num, "_", step_name)
# directory for output on clustertmp
tmp_dir <- paste0(root_tmp_dir, "/", step_num, "_", step_name)

# User specified options -------------------------------------------------------
# Source GBD 2019 Shared Functions
source(paste0(code_dir, "helper_functions/source_functions.R"))
k <- # filepath
sourceDir(paste0(k, "current/r/"))

functional <- 'meningitis'

# Inputs -----------------------------------------------------------------------
dim.dt <- fread(file.path(in_dir, paste0(step_num, '_', step_name), 'meningitis_dimension.csv'))
dim.dt <- dim.dt[healthstate %in% c("epilepsy_any", "_unsqueezed", "_parent")]
dim.dt <- dim.dt[grouping %in% c("_epilepsy", "_vision", "_hearing", "long_modsev")]

# Save results --------------------------------------------------------------------
ids <- unique(dim.dt[grouping == group & healthstate == state, modelable_entity_id])
for (id in ids) {
  if (group == "_hearing" | group == "_vision") {
    save_results_epi(
      input_dir = file.path(tmp_dir, '03_outputs', '01_draws', group, '_unsqueezed'),
      input_file_pattern = "{measure_id}_{location_id}_{year_id}_{sex_id}.csv",
      modelable_entity_id = id,
      description = paste("Custom", functional, date, "unsqueezed - custom code results for", ds),
      measure_id = 5,
      gbd_round_id = 6,
      sex_id = c(1,2),
      decomp_step = ds,
      mark_best = T)
  } else if(group == "_epilepsy") {
      save_results_epi(
        input_dir = file.path(root_tmp_dir, '04a_dismod_prep_wmort', '04_ODE', 'save_results_epilepsy'),
        input_file_pattern = '{location}.csv',
        modelable_entity_id = id,
        description = paste("Custom", functional, date, "ODE epilepsy results results for", ds),
        measure_id = 5,
        gbd_round_id = 6,
        sex_id = c(1,2),
        decomp_step = ds,
        mark_best = T)
  }

  ## Write check here
  file.create(paste0(tmp_dir, "/02_temp/01_code/checks/", "finished_save_", id, ".txt"), overwrite=T)
}

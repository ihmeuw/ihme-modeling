#####################################################################################################################################################################################
#####################################################################################################################################################################################
##                                                                                                                                                                                 ##
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code                                                                                        ##
## Author:		                                                                                                              ##
## Last updated:	4/27/16                                                                                                                                                          ##
## Description:	Parallelization of 09_collate_outputs                                                                                                                              ##
##                                                                                                                                                                                 ##
#####################################################################################################################################################################################
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
library(argparse)
library(data.table)

# Get arguments from parser
parser <- ArgumentParser()
parser$add_argument("--date", help = "timestamp of current run (i.e. 2014_01_17)", default = NULL, type = "character")
parser$add_argument("--step_num", help = "step number of this step (i.e. 01a)", default = NULL, type = "character")
parser$add_argument("--step_name", help = "name of current step (i.e. first_step_name)", default = NULL, type = "character")
parser$add_argument("--code_dir", help = "code directory", default = NULL, type = "character")
parser$add_argument("--in_dir", help = "directory for external inputs", default = NULL, type = "character")
parser$add_argument("--out_dir", help = "directory for this steps checks", default = NULL, type = "character")
parser$add_argument("--tmp_dir", help = "directory for this steps intermediate draw files", default = NULL, type = "character")
parser$add_argument("--root_j_dir", help = "base directory on J", default = NULL, type = "character")
parser$add_argument("--root_tmp_dir", help = "base directory on clustertmp", default = NULL, type = "character")
parser$add_argument("--cause", help = "potential values are: meningitis_pneumo, meningitis_hib, meningitis_meningo, meningitis_other", default = NULL, type = "character")
parser$add_argument("--group", help = "potential values are: hearing, vision, and epilepsy", default = NULL, type = "character")
parser$add_argument("--state", help = "healthstate in in_dir/.../meningitis_dimension.csv", default = NULL, type = "character")
parser$add_argument("--meid", help = "meid for custom model", default = NULL, type = "character")
parser$add_argument("--ds", help = "specify decomp step", default = 'step1', type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)


# User specified options -------------------------------------------------------
functional <- "meningitis"
metric <- 5
source("current/r/save_results_epi.R")

# Upload results ---------------------------------------------------------------
# upload prev and incidence for acute meningitis (BACTERIAL AND VIRAL)
if (meid == 24068 | meid == 24161) {
  measures <- c(5,6)
} else {
  measures <- 5
}

my_filedir <- file.path(tmp_dir, "03_outputs", "01_draws", group, state)
my_file_pat <- "{measure_id}_{location_id}_{year_id}_{sex_id}.csv"
my_desc <- paste("Custom meningitis", group, state, date, "for_decomp_step", ds, sep = "_")
save_results_epi(input_dir = my_filedir,
                 input_file_pattern = my_file_pat,
                 modelable_entity_id = meid,
                 description = my_desc,
                 measure_id = measures,
                 decomp_step = ds,
                 gbd_round_id = 6,
                 mark_best = T)

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir, "/02_temp/01_code/checks/", "finished_save_", meid,".txt"), overwrite=T)
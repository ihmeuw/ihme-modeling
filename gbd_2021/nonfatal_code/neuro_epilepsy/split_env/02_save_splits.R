# ------------------------------------------------------------------------------
# Author: Emma Nichols, updated by Paul Briant 
# Purpose: Save estimates for the five models split out of the epilepsy 
# impairment envelope.
# ------------------------------------------------------------------------------

user <- Sys.info()[['user']]

# ---LOAD LIBRARIES-------------------------------------------------------------

library(data.table)
library(magrittr)
library(readr)

# ---PARSE QSUB ARGUMENTS-------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)
meid <- args[1]
gbd_round_id <- args[2]
decomp_step <- args[3]
results_dir <- args[4]
functions_dir <- args[5]
file_pattern <- args[6]
mark_best <- args[7]
bundle_id <- args[8]
crosswalk_version_id <- args[9]
description <- args[10]

# ---SOURCE FUNCTIONS-----------------------------------------------------------

source(paste0(functions_dir, "save_results_epi.R"))

# ---SAVE RESULTS---------------------------------------------------------------

save_results_epi(modelable_entity_id = meid, 
                 input_dir = results_dir, 
                 input_file_pattern = file_pattern, 
                 description = description, 
                 mark_best = mark_best, 
                 gbd_round_id = gbd_round_id, 
                 decomp_step = decomp_step,
                 bundle_id = bundle_id,
                 crosswalk_version_id = crosswalk_version_id)

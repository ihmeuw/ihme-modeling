# Purpose: Save Crosswalk Version - ST-GPR
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common   IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPATH")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
  location_id <- 214
}

library(data.table)
library(openxlsx)
source("FILEPATH/save_crosswalk_version.R")

# set-up run directory

run_file <- fread(paste0(data_root, "FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")
draws_dir <- paste0(run_dir, "FILEPATH")
interms_dir <- paste0(run_dir, "FILEPATH")

decomp_step <- ADDRESS
gbd_round_id <- ADDRESS

#############################################################################################
###                                     Main Script                                       ###
#############################################################################################

st_gpr_input <- fread(paste0(interms_dir, "FILEPATH"))

st_gpr_input[, underlying_nid := NA]
st_gpr_input[, seq:= NA]
st_gpr_input[, sex := 'Both']
setnames(st_gpr_input, 'data', 'val')
st_gpr_input[, unit_value_as_published := 1]
st_gpr_input[, seq:= NA]
st_gpr_input[, age_start := 0]
st_gpr_input[, age_end := 125]
st_gpr_input[, year_end := year_id]
st_gpr_input[, year_start := year_id]
st_gpr_input[, input_type_id := 3]
st_gpr_input[, lower := val - (1.96 * (variance ^ 1/2))]
st_gpr_input[, upper := val + (1.96 * (variance ^ 1/2))]
st_gpr_input[lower < 0, lower := 0]

# add nid - crosswalk parent seq
crosswalk_parent_seq_nid <- fread(paste0(interms_dir, 'FILEPATH'))
# take first nid - seq pair
crosswalk_parent_seq_nid <- crosswalk_parent_seq_nid[match(unique(crosswalk_parent_seq_nid$nid), crosswalk_parent_seq_nid$nid),]
# merge on crosswalk_parent_seq
st_gpr_input <- merge(st_gpr_input, crosswalk_parent_seq_nid, by = 'nid', all.x = TRUE)
if (nrow(st_gpr_input[is.na(crosswalk_parent_seq)]) > 0 ){stop("merge issue")}

openxlsx::write.xlsx(st_gpr_input, paste0(interms_dir, "FILEPATH"),  sheetName = "extraction")

all_data_description <- DESCRIPTION

all_data_cw_md  <- save_crosswalk_version(bundle_version_id = ADDRESS,
                                          data_filepath =  paste0(interms_dir, "FILEPATH"),
                                          description = all_data_description)
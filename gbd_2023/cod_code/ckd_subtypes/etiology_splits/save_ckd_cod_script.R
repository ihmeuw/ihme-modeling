# ---HEADER--------------------------------------------------------------------------------------------------
# Project: Save CKD cod splits
#------------------------------------------------------------------------------------------------------------

message("*** Starting script ---------------------------")


rm(list = ls())
# ---CONFIG--------------------------------------------------------------------------------------------------
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}


# source functions
user <- Sys.info()["user"]
ckd_repo<-paste0("FILEPATH", user, "FILEPATH")
source(paste0(ckd_repo, "general_func_lib.R"))
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# pass args, these are all empty should these be empty since it's the child script? 
args<-commandArgs(trailingOnly = TRUE)
message(args)
out_dir <- args[1] 
description <- args[2]
id <- args[3]
release_id <- as.numeric(args[4])


file_pattern <- "death_{location_id}.csv"

message(paste("cause id : ", id))
message(paste("description :", description))
message(paste("input_dir : ", out_dir))
message(paste("file pattern: ", file_pattern))
message(paste("Release: ", release_id))
#-------------------------------------------------------------------------------------------------------------

# ---SPLIT----------------------------------------------------------------------------------------------------
save_results_cod(input_dir = out_dir,
                 input_file_pattern = file_pattern,
                 cause_id = id,
                 description = description,
                 mark_best = T,
                 release_id = release_id)
#-------------------------------------------------------------------------------------------------------------
message("Finish save COD models")



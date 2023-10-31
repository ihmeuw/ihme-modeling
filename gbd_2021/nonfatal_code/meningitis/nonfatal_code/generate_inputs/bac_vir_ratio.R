#####################################################################################################################################################################################
#' @Title: 00_master - Master file for meningitis: see specific step files for descriptions.
#' @Author: USERNAME
#' @Description: Calculate bacterial vs viral meningitis ratio - needs to be updated per hospital data set
#####################################################################################################################################################################################
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
}

pacman::p_load(stats, openxlsx, data.table)

# SOURCE FUNCTIONS --------------------------------------------------------
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))

# USER SPECIFIED OPTIONS --------------------------------------------------
date <- format(Sys.Date(), "%Y_%m_%d")
ds <- 'iterative'
gbd_round_id <- 7

out_dir <- "FILEPATH"

# Read in the AMR viral proportion modeled result
dt <- get_best_model_versions(entity="modelable_entity",
                              ids = 27198,
                              gbd_round_id = gbd_round_id,
                              decomp_step = ds,
                              status = "best")
viral_prop <- fread(paste0("FILEPATH"))

# Arithmetic so that proportion of viral becomes ratio of bacterial:viral
# Ratio is 1/(1/proportion - 1)
# i.e., a proportion of 20% would be a 1:4 ratio
# or a proportion of 50% would be a 1:1 ratio
viral_prop[, ratio := 1/(1/mean - 1)]

# write to input directory
write.csv(viral_prop, paste0("FILEPATH"))

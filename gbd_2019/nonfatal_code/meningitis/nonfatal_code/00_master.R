#####################################################################################################################################################################################
#' @Title: 00_master - Master file for meningitis: see specific step files for descriptions.
#' @Author: 
#' @Description: This master file runs the steps involved in epi custom modeling for meningitis, using the steps spreadsheet as a template for launching the code 
#' 				       This master file should be used for submitting all steps or selecting one or more steps to run in the "steps" variable
#' @Notes:
#'       MUST HAVE meningitis_steps.xlsx closed when running 00_master or it won't locate file
#'       On first run, you must migrate input files into the in_dir after it is created
#'       Change local date according to when the original run date. Also it must be manually input on ODE prep and ODE run codes.
#'
#'        1. RUN STEPS 02-04a
#'        2. Launch ODE solver prep code in python
#'        3. Launch ODE solver run code in python
#'        4. Run 04b save results to upload hearing, vision, epilepsy (requires completion of 04b and ODE)
#'	         note: this is annoyingly ran from 04b again because I dont want to add more scripts, make sure code portion is commented out
#'           Regenerate bac_viral_ratio for each new set of hospital data, change path in step 08
#'        5. Run 05a-08                                                                                                                                                           
#'        6. Run 09 to check and save reults for acute, viral, long_modsev, long_mild
#####################################################################################################################################################################################
rm(list=ls())

# User specified options -------------------------------------------------------
# Define the steps to run as vector: 
# steps <- c("02a", "02c", "03a", "03b", "04a")
# steps <- c("04b", "05a", "05b", "07", "08", "09")

# date <- gsub("-", "_", Sys.Date())

# Specify gbd round
gbd_round <- 6

# Specify decomp step
ds <- 'step4'

# Define directories -----------------------------------------------------------
# Define code directory 
code_dir <- # filepath

# Define directory on clustertmp that holds intermediate draws and logs
men_dir <- # filepath
dir.create(file.path(men_dir), showWarnings = F)

# Define directory on clustertmp that holds intermediate draws and ODE inputs/outputs
tmp_dir = # filepath
dir.create(file.path(tmp_dir), showWarnings = F)

# Define directory that will contain log files
out_dir <- # filepath
dir.create(file.path(out_dir), showWarnings = F)

# Manually put in files here 
in_dir <- # filepath
dir.create(file.path(in_dir), showWarnings = F)

# Specify location -----------------------------------------------------------
# type "all" for all locations, or specify one or more test locations, or run failed locations
loc <- "all"

# Run steps --------------------------------------------------------------------
source(paste0(code_dir,"model_custom.R"))
model_custom(code_dir, out_dir, tmp_dir, date, steps, in_dir, ds, loc)

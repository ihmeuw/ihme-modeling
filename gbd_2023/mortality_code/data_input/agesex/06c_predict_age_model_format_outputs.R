###########################################################################
## Purpose: Combines age-sex files into sex files
## Steps:
##    1. Load age-sex split files
##    2. Combines files
##    3. Writes sex split files
#############################################################################

## Initializing R, libraries

rm(list=ls())

library(RMySQL)
library(data.table)
library(foreign)
library(plyr)
library(haven)
library(assertable)
library(devtools)
library(methods)
library(argparse)
library(splines)
library(mortdb, lib.loc = "FILEPATH")

# Parse arguments
# Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of age-sex')
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# Get file path
output_dir <- paste0("FILEPATH/", version_id)

# Combine age-sex files into sex files
sexes <- c("male", "female")

for(sex_arg in sexes){
  
  # List files
  st_data <- paste0("FILEPATH") 
  st_pred <- paste0("FILEPATH")
  gpr_input1 <- paste0("FILEPATH")
  gpr_input2 <- paste0("FILEPATH")
  high_dd_2a <- paste0("FILEPATH")
  high_dd_2b <- paste0("FILEPATH")
  high_dd_2c <- paste0("FILEPATH")
  high_dd_2d <- paste0("FILEPATH")
  gpr_input3 <- paste0("FILEPATH")
  gpr_inputt1<- paste0("FILEPATH")
  gpr_input <- paste0("FILEPATH")
  
  models <- c(st_data, st_pred, gpr_input1, gpr_input2, high_dd_2a, high_dd_2b, high_dd_2c, high_dd_2d,
              gpr_input3, gpr_inputt1, gpr_input)
  
  for(model in models){
    
    # Split file path and file name
    path <- unlist(strsplit(model, "\\/"))
    file <- path[length(path)]
    path <- paste(path[-length(path)], collapse="/")
    
    # Combine age-split files
    files  <- paste0(path, "/", list.files(path= path, pattern = glob2rx(file)))
    tables <- lapply(files, read.csv, header = TRUE, colClasses=c("age_group_name"="factor"))
    combined <- do.call(rbind , tables)
    
    # Write combined file
    combined_filepath <- gsub(pattern = "_\\*", replacement = "", x = model)
    write.csv(combined, combined_filepath, row.names=F)
    
  } # close model loop
} # close sex loop

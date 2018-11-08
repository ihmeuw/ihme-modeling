# Author:
# Date: 02/20/18
# Purpose: 1) Generate parent-child relationships
# 

rm(list=ls())
library(data.table); library(DBI); library(mortdb, lib = "FILEPATH"); library(mortcore, lib = "FILEPATH")

if (Sys.info()[1] == "Linux") {
  root <- "/home/j/"
  username <- Sys.getenv("USER")
  child_version_est <- as.numeric(commandArgs(trailingOnly = T)[1])
  child_version_data <- as.numeric(commandArgs(trailingOnly = T)[2])
} else {
  root <- "J:/"
}

# Read in process table for DDM
process_table <- fread(paste0("FILEPATH"))
parent_list <- process_table$run_id
names(parent_list) <- process_table$process_name
parent_list <- as.list(parent_list)

# Generate parent-child relationships for DDM data and estimates
gen_parent_child(parent_runs = parent_list, child_process = "ddm estimate", child_id = child_version_est)
gen_parent_child(parent_runs = parent_list, child_process = "ddm data", child_id = child_version_data)

# DONE
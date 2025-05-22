rm(list = ls())

# Libraries
library(data.table)
library(tidyverse)
library(dplyr)
library(haven) # for reading in dta files
library(readxl)
library(argparse)
# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "H:/"

# Arguments (These should carry over into the launched scripts) 
parser <- ArgumentParser()
parser$add_argument("--gbd_round", help = "GBD round/cycle",
                    default = "gbd2022", type = "character")
parser$add_argument("--version", help = "Version (?)",
                    default = "2022", type = "character")
args <- parser$parse_args()
args_flat <- paste(args[1], args[2]) 
list2env(args, environment()) # rm(args)

data_prep <- T
run_06 <- T
run_jobs <- T

# Paths

code_path <- "FILEPATH"

data_path <- paste0("FILEPATH") # this is generally where data will be output throughout the pipeline
govern_path <- paste0("FILEPATH")

output_f <- paste0(data_path, "05_output/")
sge.output.dir <- paste0("-o FILEPATH") # output and error logs

# Data
if (data_prep) {
  
  govern <- read_csv(govern_path)
  loc_ids <- unique(govern$location_id)
  job_ids <- 1:length(loc_ids)
  gvrn_df <- data.frame(loc_ids, job_ids)

  write_csv(gvrn_df, paste0(output_f, "govern.csv"))
  
  }

#### BEGIN JOB RUN CODE ####

if (run_jobs) {

  if (run_06 == T) {
    
    ## 06_variance_raw.R
    
    map_path <- paste0(output_f, "govern.csv")
    
    rw <- length(count.fields(map_path, sep = ","))-1
    
    sys.sub_06 <- paste0("sbatch -J ", "fao06 -c 6 --array=0-", rw, "%10 --mem=12G -t 02:00:00 -A proj_diet -p long.q ", sge.output.dir) # proj_diet 
    script_06 <- paste0(code_path, "06_variance_raw.R")
    jid_06 <- system(paste(sys.sub_06, "FILEPATH/execRscript.sh", "-s", script_06, args_flat, map_path), intern = T)
    job_id_06 <- stringr::str_extract(jid_06, "[:digit:]+") 
    sys.sub_07 <- paste0("sbatch -J fao07 -c 6 -d ", job_id_06, " --mem=12G -t 02:00:00 -A proj_diet -p long.q ", sge.output.dir)
    
  } else {
    
    job_id_01 <- "None. Variance step skipped."
    sys.sub_07 <- paste0("sbatch -J fao07 -c 6 --mem=12G -t 02:00:00 -A proj_diet -p all.q ", sge.output.dir)
    
  }
    
  ## 07_variance_compile.R
  
  script_07 <- paste0(code_path, "07_variance_compile.R")
  jid_07 <- system(paste(sys.sub_07, "FILEPATH/execRscript.sh", "-s", script_07, args_flat), intern = T)
  job_id_07 <- str_extract(jid_07, "[:digit:]+")
  
  ## 08_variance_compile.R
  
  sys.sub_08 <- paste0("sbatch -J fao08 -c 6 -d ", job_id_07, " --mem=12G -t 02:00:00 -A proj_diet -p all.q ", sge.output.dir)
  script_08 <- paste0(code_path, "08_indiv_sheets_to_model.R")
  jid_08 <- system(paste(sys.sub_08, "FILEPATH/execRscript.sh", "-s", script_08, args_flat), intern = T)
  job_id_08 <- str_extract(jid_08, "[:digit:]+")
    
  }


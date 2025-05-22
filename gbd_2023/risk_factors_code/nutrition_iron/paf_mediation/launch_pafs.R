################################################################################
## DESCRIPTION: Launch PAFs for the specified "risks." 
## INPUTS: Specify in PARSE ARGS.
###
rm(list=ls())


## LOAD DEPENDENCIES -----------------------------------------------------------
library(argparse)
library(data.table)


## System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FIlEPATH" else if (os == "Windows") "J:/"

# Base filepaths
log_dir <- paste0("FILEPATH", user)

## BODY ------------------------------------------------------------------------

ids <- fread("FILEPATH/mnd_ids.csv") # diet_risk metadata with rei_ids
code <- "FILEPATH/run_paf.R"
rei = 95
year_id = c(1990:2024)
draws <- 250
release_id <- 16
proj_name <- "proj_diet"
resume_stat <- "False" 
skip_save <- "False"


print(rei)

for (i in rei){
  rei_id <- i
  print(i)
  me <- ids[rei_id == i, rei_name]
  print(me)
 arg_list <- paste(me, rei_id, release_id, year_id, draws, proj_name, resume_stat, skip_save)
  
   
  jobname <- paste0(gsub("MND_", "", me), "_launch_paf")
  
  system(paste0("sbatch -J ", jobname,
                " -A ", proj_name, " --mem=80G -c 20 -t 384:00:00 -p long.q",
                " -o ", log_dir, "/output/%x.o%j -e ", log_dir, "/errors/%x.e%j ",
                "FILEPATH/execRscript.sh ",
                "-s ", code,
                " ", arg_list
                
            
  ))
}


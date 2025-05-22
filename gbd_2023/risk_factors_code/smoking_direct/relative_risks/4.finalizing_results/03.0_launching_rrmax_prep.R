## HEADER #################################################################
# Date: 1/12/2021, updated 02/21/2024
# Purpose: Updating the RRmax for forecasting with the 95th percentile exposure values 
#          

## SET-UP #################################################################
library(data.table)
date <- format(Sys.Date(), "%m_%d_%Y")

## SCRIPT ##################################################################
# filepaths: 
rrmax_old_fp <- FILEPATH
RO_pairs_fp <- FILEPATH
rr_draws_fp <- FILEPATH
py_exp_fp <- FILEPATH # 95th percentile of pack-year exposures
cd_exp_fp <- FILEPATH # 95th percentile of cig/day exposures
visual_fp <- paste0(FILEPATH, date, ".pdf")
save_fp <- paste0(FILEPATH, date, ".csv")

save_plot <- TRUE # Set to true if you want to print out a scatter plot comparing the new RRmax to the old one

#Specifies shell to use and script. These should not change often.
shell  <- paste0(FILEPATH, "/execRscript.sh")
script <- paste0(FILEPATH, "/03.1_rrmax_prep.R")

#Specifying project to run on, slots, and where the output and error files to land
proj   <- "proj_team"

output_info <- FILEPATH
error_info <- FILEPATH

mem_alloc <- 30
threads <- 4
time_alloc <- "00:30:00"
queue <- "all.q"

job_name <- paste0("rrmax_prep")

#Insert any variables that need to be added to the end of the job
other_variables <- paste(rrmax_old_fp, RO_pairs_fp, rr_draws_fp, py_exp_fp, cd_exp_fp, visual_fp, save_fp, save_plot) 

##Launches job
command   <- paste0("sbatch --mem ", mem_alloc,
                    "G -C archive",  
                    " -c ", threads, 
                    " -t ", time_alloc,
                    " -p ", queue, 
                    " -e ", paste0(error_info, "/%x.e%j"),
                    " -o ", paste0(output_info, "/%x.o%j"),
                    " -A ", proj, 
                    " -J ", job_name,
                    " ", shell, 
                    " -s ", script, 
                    " ", other_variables)

system(command)



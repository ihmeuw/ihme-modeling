
#Specifies shell to use and script. These should not change often.
shell  <- paste0(FILEPATH, "execRscript.sh")
script <- paste0(FILEPATH, "02.1_saving_risks.R")

#Specifying project to run on and where the output and error files to land
proj   <- "proj_team"
job_name <- "saving_rr_smoking"

output_info <- FILEPATH
error_info <- FILEPATH

mem_alloc <- 100
threads <- 24
time_alloc <- "02:00:00"
queue <- "all.q"

# saving rr or unmediated rr draws
unmed <- T

if(unmed){
  me_id <- 26976 # unmediated RR for current any smoking for PAD
  description <- "Final_draws_no_gamma_unmediated_rr_for_PAD"
  file_path <- FILEPATH
  
} else {
  me_id <- 9075 #current any smoking relative risk
  description <- "Final_draws_no_gamma"
  file_path <- FILEPATH
}

gbd_id <- 7
decomp <- "iterative"
best <- TRUE
file_version <- "all_draws.csv"

year <- 2021 # change the year for the RR draws.
release <- 9 # realease id 9 for non-fatal results for GBD 2021

#Insert any variables that need to be added to the end of the job
other_variables <- paste(file_path, file_version, me_id, description, best, year, release) 

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

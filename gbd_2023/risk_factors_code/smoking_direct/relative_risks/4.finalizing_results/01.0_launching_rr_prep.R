library(data.table)
library(dplyr)

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

rr_files <- FILEPATH
files <- list.files(rr_files, full.names = F)
causes_produced <- gsub(".csv","",files)

purpose <- "final" # "continuous" if prepping continuous RO pairs, "dichotomous" if prepping fractures (our only dichtomous RO), and 
                   # "final" if combining all prepped RO files into dataset to upload, and prepare the unmediated draws for PAD to upload.

# filepaths: 
RO_pairs <- paste0(FILEPATH)
cause_specific_file_path <- paste0(FILEPATH)

final_file_path <- paste0(FILEPATH)


#Specifies shell to use and script. These should not change often.
shell  <- paste0(FILEPATH, "execRscript.sh")
script <- paste0(FILEPATH, "04.0_finalizing_results/01.1_rr_prep.R")

#Specifying project to run on and where the output and error files to land
proj   <- "proj_team"
job_name <- "prepping_rr_for_saving"

output_info <- sprintf(FILEPATH, user)
error_info <- sprintf(FILEPATH, user)

mem_alloc <- 20
threads <- 4
time_alloc <- "00:30:00"
queue <- "all.q"

if(purpose == "continuous"){
  for(i in causes_produced){
    if(i == "fractures"){
      print(i)
      next # Because fractures is dichotomous
    } else {
      #Insert any variables that need to be added to the end of the job
      other_variables <- paste(i, purpose, rr_files, final_file_path, RO_pairs, cause_specific_file_path) 
      
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
    }
  }
} else if(purpose == "dichotomous"){
  i <- "fractures" # because fractures are our only dichotomous RO pair
  #Insert any variables that need to be added to the end of the job
  other_variables <- paste(i, purpose, rr_files, final_file_path, RO_pairs, cause_specific_file_path) 
  
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
} else if(purpose == "final"){
  i <- "none"
  mem_alloc <- 100
  time_alloc <- "00:40:00"
  #Insert any variables that need to be added to the end of the job
  other_variables <- paste(i, purpose, rr_files, final_file_path, RO_pairs, cause_specific_file_path) 
  
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
}

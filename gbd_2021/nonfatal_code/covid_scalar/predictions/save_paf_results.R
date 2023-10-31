rm(list=ls())
library(argparse); library(data.table)
eti <- read.csv("FILEPATH")
upload_rei <- c(187,190)
info <- subset(eti, rei_id %in% upload_rei)
source(paste0("FILEPATH"))
gbd_round <- 7
ds <- 'iterative'

for (n in 1:nrow(info)) {
  pathogen <- info$pathogen[n]
  meid <- info$me_id[n]
  upload_dir <- paste0("FILEPATH")
  desc <- "COVID_adjust_AMR_result"
  my_job_name <- paste0(pathogen, "_", meid, "_save_results")
  print(paste("submitting", my_job_name))
  parallel_script <- paste0("FILEPATH")
  my_args <- paste("--meid", meid,
                   "--upload_dir", upload_dir,
                   "--ds", ds,
                   "--gbd_round_id", gbd_round,
                   "--desc", desc)
  
  sbatch(job_name = my_job_name,
         project = "proj_tb",
         queue = "all.q",
         mem = "200G",
         fthread = 24,
         time = "08:00:00",
         holds = NULL,
         output = "FILEPATH",
         error = "FILEPATH",
         shell_file = "FILEPATH",
         script = parallel_script,
         args = my_args)
}

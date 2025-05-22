library(data.table)

cli_args <- commandArgs(trailingOnly = TRUE)
map_dir <- cli_args[1]
topic_name <- cli_args[2]
job_name <- cli_args[3]
max_jobs <- as.numeric(cli_args[4])

task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))

a <- system(paste("squeue --me | grep",job_name,"| wc"), intern = T)
num_jobs <- as.numeric(stringr::str_extract(a, "[0-9]+")[1])

delay <- 0
if(num_jobs < max_jobs && task_id <= max_jobs){
  delay <- task_id*3
}else{
  delay <- log(task_id)*5
}

print(delay)
Sys.sleep(delay)

ubcov_map <- fread(paste0(map_dir,"ubcov_ids.csv"))
curr_ubcov_id <- ubcov_map[task_id,ubcov_ID_vec]

system(paste(file.path(getwd(), "extract/winnower_extract/extract_shell.sh",topic_name,curr_ubcov_id)))

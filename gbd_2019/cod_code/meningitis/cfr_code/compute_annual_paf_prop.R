#' @author 
#' @date 2019/09/09
#' @description predict CFR values using and submit parallel job to compute 
#' mortality proportions for all GBD locations

rm(list=ls())

pacman::p_load(data.table, boot, ggplot2, parallel, foreach, doSNOW)

# USER INPUTS -------------------------------------------------------------
ds <- 'step4'
main_dir <- # filepath
date <- gsub("-", "_", Sys.Date())
# where nonfatal and fatal PAF draws are outputed
out_dir <- # filepath

check_dir <- paste0(out_dir, "checks/")
dir.create(check_dir, recursive = T, showWarnings = F)

# create output directores for upload for each etiology and for fatal/ nonfatal
fatal_out_dir <- # filepath
spn_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_pneumo/")
hib_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_hib/")
nm_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_meningo/")
other_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_other/")
dir.create(spn_fatal_out_dir, recursive = T, showWarnings = F)
dir.create(hib_fatal_out_dir, recursive = T, showWarnings = F)
dir.create(nm_fatal_out_dir, recursive = T, showWarnings = F)
dir.create(other_fatal_out_dir, recursive = T, showWarnings = F)

nonfatal_out_dir <- # filepath
spn_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_pneumo/")
hib_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_hib/")
nm_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_meningo/")
other_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_other/")
dir.create(spn_nonfatal_out_dir, recursive = T, showWarnings = F)
dir.create(hib_nonfatal_out_dir, recursive = T, showWarnings = F)
dir.create(nm_nonfatal_out_dir, recursive = T, showWarnings = F)
dir.create(other_nonfatal_out_dir, recursive = T, showWarnings = F)

# delete finished check files if rerunning
files <- list.files(check_dir, pattern='finished.*.txt')
for (f in files) {
  file.remove(file.path(check_dir, f))
}

param_map_path <- # filepath

# MEIDs for saving results
spn_mort_meid <- 10494
nm_mort_meid <- 10495
hib_mort_meid <- 10496

spn_inc_meid <- 24739
nm_inc_meid <- 24741
hib_inc_meid <- 24740

# create parameter map for upload jobs
upload_param_map <- data.table(etiology = c("meningitis_pneumo", "meningitis_meningo", "meningitis_hib",
                                            "meningitis_pneumo", "meningitis_meningo", "meningitis_hib"),
                               meid = c(spn_mort_meid, nm_mort_meid, hib_mort_meid,
                                        spn_inc_meid, nm_inc_meid, hib_inc_meid),
                               dir = c(spn_fatal_out_dir, nm_fatal_out_dir, hib_fatal_out_dir, 
                                       spn_nonfatal_out_dir, nm_nonfatal_out_dir, hib_nonfatal_out_dir))


# SOURCE FUNCTIONS --------------------------------------------------------
source("current/r/get_location_metadata.R")
source("current/r/get_demographics.R")
source(paste0(main_dir, "/helper_functions/qsub.R"))

demo <- get_demographics('epi', gbd_round_id = 6)
locations <- demo$location_id
years <- demo$year_id

# INTERPOLATE FATAL ANNUAL PAFS -------------------------------------------------
# create parameter map to where each row represents parameters for each job
param_map <- expand.grid(location_id = locations)
write.csv(param_map, param_map_path, row.names=F)
n_jobs <- nrow(param_map)

# Submit job
my_job_name <- "child_annual_paf_meningitis"
print(paste("submitting", my_job_name))
my_project <- "proj_tb"
my_tasks <- paste0("1:", n_jobs)
my_fthread <- 1
my_mem <- "5G"
my_time <- "02:00:00"
my_queue <- 'long.q'
parallel_script <- paste0(main_dir, "/compute_annual_paf_parallel.R")
shell_file <- paste0(main_dir, "r_shell.sh")

my_args <- paste("--date", date,
                 "--out_dir", out_dir,
                 "--check_dir", check_dir,
                 "--param_map_path", param_map_path,
                 "--ds", ds)

qsub(
  job_name = my_job_name,
  project = my_project,
  queue = my_queue,
  tasks = my_tasks,
  mem = my_mem,
  fthread = my_fthread,
  j_archive = 1,
  time = my_time,
  shell_file = shell_file,
  script = parallel_script,
  args = my_args
)

# check for jobs to finish
Sys.sleep(60)
# Wait for jobs to finish before passing execution back to main step file
i <- 0
while (i == 0) {
  checks <- list.files(check_dir, pattern='finished.*.txt')
  count <- length(checks)
  print(paste("checking ", Sys.time(), " ", count, "of ", n_jobs, " jobs finished"))

  if (count == n_jobs) i <- i + 1 else Sys.sleep(60)
}

# if you need to rerun only certain locations -> change locations variable to only unfinished locations and rerun from there

# SAVE RESULTS ------------------------------------------------------------
# Submit jobs

for (n in 1:nrow(upload_param_map)) {
  etio <- upload_param_map[n, etiology]
  meid <- upload_param_map[n, meid]
  upload_dir <- upload_param_map[n, dir]
  
  my_job_name <- paste0(etio, "_", meid, "_save_results")
  print(paste("submitting", my_job_name))
  my_project <- "proj_tb"
  my_fthread <- 10
  my_mem <- "50G"
  my_time <- "08:00:00"
  my_queue <- 'long.q'
  shell_file <- paste0(main_dir, "r_shell.sh")
  parallel_script <- paste0(main_dir, "/save_annual_paf_parallel.R")
  my_args <- paste("--meid", meid,
                   "--upload_dir", upload_dir,
                   "--check_dir", check_dir,
                   "--ds", ds,
                   "--etio", etio)
  
  qsub(
    job_name = my_job_name,
    project = my_project,
    queue = my_queue,
    mem = my_mem,
    fthread = my_fthread,
    j_archive = 1,
    time = my_time,
    shell_file = shell_file,
    script = parallel_script,
    args = my_args
  )
}




# Source central functions
source("FILEPATH/get_location_metadata.R")
source('FILEPATH/function_lib.R')
source("FILEPATH/custom_process_functions.R")

run_squeeze_code <- function(path_to_errors, path_to_settings, are_you_testing = 0,
                             threads = 4, memory = 15, runtime = "3:00:00", q = "all.q",
                             project = "USERNAME", shell, user = "USERNAME"
                             code_directory = "FILEPATH"){
  
  print("Starting Squeeze Code ----------------------------------------")
  
  path_to_map <- path_to_settings
  
  # Error messages to send
  errors <- path_to_errors
  
  # Path to script
  script_split <- paste0(code_directory, "01_squeeze_draws_general.R")
  
  ### Cluster Job Settings ----------------------------------------------------------
  threads <- threads
  mem_free <- memory
  runtime <- runtime
  q <- q
  project <- project
  shell <- shell
  
  ### Locations set up ------------------------------------------
  # Read in model settings for all shared functions in scripts
  mapping <- read.csv("FILEPATH", stringsAsFactors = FALSE)
  
  print("Column names:")
  print(names(mapping))
  
  # Get unique locations to get estimates for, should cover all GBD estimations
  location_dt <- get_location_metadata(35, 
                                       gbd_round_id = mapping$gbd_round_id[1], 
                                       decomp_step = mapping$decomp_step[1])
  
  locs <- unique(location_dt[most_detailed==1, location_id])
  
  if (are_you_testing == 1) {
    locs <- locs[1:10]
  }
  
  print(paste("Running for ", locs, "locations ------------------------"))
  
  ### Run Qsub job array - launches all jobs at once ------------------
  arguments_for_script <- c(path_to_map, are_you_testing)
  
  job.array.master(tester = F, paramlist = locs,
                   username = user,
                   project = project,
                   threads = threads,
                   mem_free = mem_free,
                   runtime = runtime,
                   errors = errors,
                   q = q,
                   jobname = "squeeze_draws",
                   childscript = script_split,
                   shell = shell,
                   args = arguments_for_script)
  
  
  ### Where are results saved ----------------------------------------------
  saved <- mapping[!is.na(mapping$target_me_id), ]
  for (i in 1:nrow(saved)) {
    txt <- paste(saved$target_me_id[i], "saved to ", 
                 paste0(saved$directory_to_save[i], saved$target_me_id[i], "/"))
    print(txt)
  }
  
  print("Finished running squeeze, check cluster for job status ------------------------------------------")
  
}

run_epi_splits <- function(path_to_errors, path_to_settings, 
                           threads = 30, memory = 60, runtime = "24:00:00", q = "all.q",
                           project = "USERNAME ", shell, code_directory = "FILEPATH") {
  
  split_script <- paste0(code_directory, "01_split_epi_general.R")
  errors <- path_to_errors
  
  mapping <- read.csv(path_to_settings, stringsAsFactors = FALSE)
  source_mes <- unique(mapping$source_me_id) 
  threads = threads
  memory = memory
  runtime = runtime
  q = q
  project = project
  
  job_names <- c() # outputs for save results for qholds
  for (i in source_mes) {
    me <- i # sending unique source ME which will filter in the qsub call
    job_name <- paste0("split_", me)
    sys_sub <- paste0('qsub', 
                      ' -P ', project, 
                      ' -N ', job_name , 
                      " -e ", errors, 
                      ' -l m_mem_free=', memory, 'G', 
                      " -l fthread=", threads,
                      " -l h_rt=", runtime, " -q ", q)
    print(paste(sys_sub, shell, split_script, path_to_map, me))
    system(paste(sys_sub, shell, split_script, path_to_map, me))
    job_names <- c(job_names, job_name)
  }
  return(job_names) 
}

save_results_function <- function(path_to_errors, path_to_settings,
                                  threads = 10, memory = 75, runtime = "24:00:00", q = "all.q",
                                  project = "USERNAME", best = FALSE,
                                  description, code_directory = "FILEPATH",
                                  list_of_job_names = "no_job_holds",
                                  shell) {
  
  print("Starting Save Results ------------------------------")
  mapping <- read.csv(path_to_settings, stringsAsFactors = FALSE)
  mapping <- mapping[!is.na(mapping$target_me_id), ]

  errors <- path_to_errors # Error messages to send

  script_02 <- paste0(code_directory, "02_save_results.R") # Path to script
  
  ds <- mapping$decomp_step[1]
  round <- mapping$gbd_round_id[1]
  memory <- memory
  runtime <- runtime
  
  print('Starting saves -----------------------------------')
  for(i in 1:nrow(mapping)){
    me <- mapping$target_me_id[i]
    directory <- paste0(mapping$directory_to_save[i], me, '/')
    measure <- mapping$target_measure_id[i]
    print(paste(me, 'saved from', directory))
    print('Measure IDs')
    print(measure)

    job_name <- paste0(' save_', me)
    sys_sub <- paste0('qsub', 
                      ' -P ', project,
                      ' -N ', job_name , 
                      ' -e ', errors, 
                      ' -hold_jid ', list_of_job_names, 
                      ' -l m_mem_free=', memory, 'G', 
                      ' -l fthread=', threads,
                      ' -l h_rt=', runtime, 
                      ' -q ', q)
    system(paste(sys_sub, shell, script_02, me, directory, description, best, ds, round, measure))
    print(paste(sys_sub, shell, script_02, me, directory, description, best, ds, round, measure))
  }

  print("Finished save code, check cluster for job status ------------------")
}
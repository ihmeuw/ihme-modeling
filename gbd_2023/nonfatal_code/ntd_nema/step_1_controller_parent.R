### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <- "FILEPATH"
data_root <- "FILEPATH"

if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPATH")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
}


library(data.table)
library(stringr)
source(paste0(code_root, "/FILEPATH/qsub_function.R"))
source("/FILEPATH/save_results_epi.R")
source(paste0(code_root, '/FILEPATH/sbatch.R'))

shell  <- "/FILEPATH/execRscript.sh"
run_file <- fread(paste0(data_root, "/FILEPATH/run_file.csv"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "/crosswalks/")
draws_dir    <- paste0(run_dir, "/draws/")
interms_dir    <- paste0(run_dir, "/interms/")


release_id <- ADDRESS
run_all_cases <- 1
run_heavy <- 1
run_mild <- 1
run_asymp <- 1

worm_list <- list(list("ascariasis", ADDRESS, ADDRESS, ADDRESS),
                  list("hookworm", ADDRESS, ADDRESS, ADDRESS),
                  list("trichuriasis", ADDRESS, ADDRESS, ADDRESS))

worm_element <- worm_list[[1]]

#############################################################################################
###                                 Main Execution                                        ###
#############################################################################################

my_worm <- worm_element[[1]]  
model_ver <- worm_element[[2]]
bundle_id <- worm_element[[3]]
crosswalk_version_id <- worm_element[[4]]
model_version_id <- worm_element[[5]]
heavy_id <- worm_element[[6]]
mild_id <- worm_element[[7]]

description <- paste0("COMMENT: ", model_ver)

stgpr_draw_directory <- paste0("/FILEPATH/", model_ver, "/FILEPATH/")

study_dems <- readRDS(paste0(data_root, "/FILEPATH.rds"))
my_locs    <- study_dems$location_id

meid_dir             <- paste0(params_dir, "/FILEPATH.csv")
meids                <- fread(meid_dir)
meids      <- meids[worm == my_worm]
all_cases  <- meids[model == "all"]$meid
heavy <- meids[model == "heavy"]$meid
mild <- meids[model == "mild"]$meid
asymp  <- meids[model == "asymptomatic"]$meid
if (all_cases == ADDRESS) {asymp <- ADDRESS} 

ifelse(!dir.exists(paste0(draws_dir, my_worm, "/")), dir.create(paste0(draws_dir, '/', my_worm, "/")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/", all_cases, "/")), dir.create(paste0(draws_dir, my_worm,  "/", all_cases, "/")), FALSE)
ifelse(!dir.exists(paste0(interms_dir, my_worm, "/")), dir.create(paste0(interms_dir, my_worm, "/")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/", heavy, "/")), dir.create(paste0(draws_dir, my_worm, "/", heavy, "/")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/", mild, "/")), dir.create(paste0(draws_dir, my_worm,  "/", mild, "/")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/", asymp, "/")), dir.create(paste0(draws_dir, my_worm,  "/", asymp, "/")), FALSE)

#############################################################################################
###                             Prep Proportions used for splits                          ###
#############################################################################################

time_alloc <- "00:10:00"
script <- paste0(code_root, "/FILEPATH/child_infest_prop.R")

command   <- paste0("sbatch --mem=",2,"G",
                    " -c ", 1,
                    " -t ",time_alloc,
                    " -p long.q",
                    " -e /FILEPATH/errors/%x.e%j",
                    " -o /FILEPATH/output/%x.o%j",
                    " -A ADDRESS", 
                    " -i /FILEPATH.img",
                    " -J ", paste0("COMMENT_", my_worm),
                    " ", shell, 
                    " -s ", script)

system(command)

#############################################################################################
###                                 All Prevalence MEIDS                                  ###
#############################################################################################
if (run_all_cases == 1){
    param_map_impute <- data.table(location_ids = my_locs,
                                   stgpr_draws_directory = stgpr_draw_directory,
                                   release_id = release_id,
                                   my_worm = my_worm)
    
    fwrite(param_map_impute, paste0(interms_dir, my_worm, "FILEPATH.csv"), row.names = F)
  
  fthreads <- 1
  mem_alloc <- 5
  time_alloc <- "00:10:00"
  script <- paste0(code_root, "/FILEPATH.R")
  arg_name <- list("--param_path_impute", paste0(interms_dir, my_worm, "FILEPATH.csv"))
  
  sbatch(job_name = paste0("COMMENT", my_worm),
         shell    = shell,
         code     = script,
         output   = "/FILEPATH/output/%x.o%j",
         error    = "/FILEPATH/errors/%x.e%j",
         args     = arg_name,
         project  = "ADDRESS",
         num_jobs = nrow(param_map_impute),
         threads  = fthreads,
         memory   = mem_alloc,
         time     = time_alloc,
         queue    = "long")
}


## Now upload all-prevalence:
save_results_epi(input_dir = paste0(draws_dir, my_worm,  "/", all_cases, "/"),
                 input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = all_cases,
                 measure_id = 5,
                 metric_id = 3,
                 description = description,
                 mark_best = TRUE,
                 release_id = release_id,
                 bundle_id = bundle_id,
                 crosswalk_version_id = crosswalk_version_id)


#############################################################################################
###                                 Heavy - Sequelae Splits                               ###
#############################################################################################
if (run_heavy == 1){
  
  done_files <- list.files(paste0(draws_dir, my_worm,  "/", heavy, "/"))
  done_locs <- as.integer(str_extract_all(string = done_files, pattern = "[0-9]+", simplify= TRUE))

  while (length(done_locs) < length(my_locs)){

  est_locs <- setdiff(my_locs, done_locs)
  cat(paste0("Length of heavy est_locs is ", length(est_locs)))
  
    param_map_heavy <- data.table(location_ids = my_locs,
                                  is_heavy = 1,
                                  my_worm = my_worm,
                                  release_id = release_id,
                                  all_cases_id = model_version_id)
    
    fwrite(param_map_heavy, paste0(interms_dir, my_worm, "FILEPATH.csv"), row.names = F)
    
    fthreads <- 1
    mem_alloc <- 5
    time_alloc <- "00:20:00"
    script <- paste0(code_root, "/FILEPATH.R")
    arg_name <- list("--param_path", paste0(interms_dir, my_worm, "FILEPATH.csv"))
    
    sbatch(job_name = paste0("COMMENT", my_worm),
           shell    = shell,
           code     = script,
           num_jobs = nrow(param_map_heavy),
           output   = "/FILEPATH/%x.o%j",
           error    = "/FILEPATH/%x.e%j",
           args     = arg_name,
           project  = "ADDRESS",
           threads  = fthreads,
           memory   = mem_alloc,
           time     = time_alloc,
           queue    = "long")
    
  }
}  
  save_results_epi(input_dir = paste0(draws_dir, my_worm, "/", heavy, "/"),
                   input_file_pattern = "{location_id}.csv",
                   modelable_entity_id = heavy,
                   measure_id = 5,
                   metric_id = 3,
                   description = description,
                   mark_best = TRUE,
                   release_id = release_id,
                   bundle_id = bundle_id,
                   crosswalk_version_id = crosswalk_version_id)


#############################################################################################
###                                Mild -   Sequelae Splits                               ###
#############################################################################################

if (run_mild == 1){
  
  done_files <- list.files(paste0(draws_dir, my_worm,  "/", mild, "/"))
  done_locs <- as.integer(str_extract_all(string = done_files, pattern = "[0-9]+", simplify= TRUE))

  while (length(done_locs) < length(my_locs)){

    est_locs <- setdiff(my_locs, done_locs)
    cat(paste0("Length of mild est_locs is ", length(est_locs)))
    
    param_map_mild <- data.table(location_ids = my_locs, 
                                 is_heavy = 0, 
                                 my_worm = my_worm, 
                                 release_id = release_id,
                                 all_cases_id = model_version_id)
    
    fwrite(param_map_mild, paste0(interms_dir, my_worm, "FILEPATH.csv"), row.names = F)
    
    fthreads <- 1
    mem_alloc <- 3
    time_alloc <- "00:20:00"
    script <- paste0(code_root, "/FILEPATH/child_sequlea_split.R")
    arg_name <- list("--param_path", paste0(interms_dir, my_worm, "FILEPATH.csv"))
    
    sbatch(job_name = paste0("COMMENT", my_worm),
           shell    = shell,
           code     = script,
           num_jobs = nrow(param_map_mild),
           output   = "/FILEPATH/output/%x.o%j",
           error    = "/FILEPATH/errors/%x.e%j",
           args     = arg_name,
           project  = "ADDRESS",
           threads  = fthreads,
           memory   = mem_alloc,
           time     = time_alloc,
           queue    = "all")

  }
}  
  
  save_results_epi(input_dir = paste0(draws_dir, my_worm, "/", mild, "/"),
                   input_file_pattern = "{location_id}.csv",
                   modelable_entity_id = mild,
                   measure_id = 5,
                   metric_id = 3,
                   description = description,
                   mark_best = TRUE,
                   release_id = release_id,
                   bundle_id = bundle_id,
                   crosswalk_version_id = crosswalk_version_id)


#############################################################################################
###                              Asymptomatic -   Sequelae Splits                         ###
#############################################################################################

if ( run_asymp == 1){
  done_files <- list.files(paste0(draws_dir, my_worm,  "/", asymp, "/"))
  done_locs <- as.integer(str_extract_all(string = done_files, pattern = "[0-9]+", simplify= TRUE))

  while (length(done_locs) < length(my_locs)){

    est_locs <- setdiff(my_locs, done_locs)
    cat(paste0("Length of asymp est_locs is ", length(est_locs)))

    param_map_asymp <- data.table(location_ids = my_locs,
                                  my_worm = my_worm,
                                  release_id = release_id,
                                  all_cases_id = model_version_id,
                                  heavy_id = heavy_id,
                                  mild_id = mild_id)
    
     
    fwrite(param_map_asymp, paste0(interms_dir, my_worm, "FILEPATH.csv"), row.names = F)
     
    fthreads <- 1
    mem_alloc <- 5
    time_alloc <- "00:20:00"
    script <- paste0(code_root, "/FILEPATH/child_asymptomatic.R")
    arg_name <- list("--param_path", paste0(interms_dir, my_worm, "FILEPATH.csv"))
    
    sbatch(job_name = paste0("COMMENT", my_worm),
           shell    = shell,
           code     = script,
           num_jobs = nrow(param_map_asymp),
           output   = "/FILEPATH/output/%x.o%j",
           error    = "/FILEPATH/errors/%x.e%j",
           args     = arg_name,
           project  = "ADDRESS",
           threads  = fthreads,
           memory   = mem_alloc,
           time     = time_alloc,
           queue    = "long")

  }
}  
  save_results_epi(input_dir = paste0(draws_dir, my_worm,  "/", asymp, "/"), 
                   input_file_pattern = "{location_id}.csv",
                   modelable_entity_id = asymp, 
                   measure_id = 5, 
                   metric_id = 3, 
                   description = description, 
                   mark_best = TRUE,
                   release_id = release_id,
                   bundle_id = bundle_id,
                   crosswalk_version_id = crosswalk_version_id)

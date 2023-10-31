# Purpose: Submit jobs and saves for all prevalence, heavy, medium, and asymptomatic meids
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
## Load functions and packages
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common   IO Paths
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
   location_id <- 214
}

# set run dir
library(data.table)
library(stringr)
source(paste0(code_root, "FILEPATH/launch_function.R"))
source("FILEPATH/save_results_address.R")
my_shell <- paste0(code_root, "FILEPATH")

run_file <- fread(paste0(data_root, "FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")
draws_dir    <- paste0(run_dir, "FILEPATH")
interms_dir    <- paste0(run_dir, "FILEPATH")

# load packages / central functions / custom functions
# select yes/no (1/0) for which sequalae you want to create draws for
decomp_step <- ADDRESS
gbd_round_id <- ADDRESS
run_all_cases <- 1
run_heavy <- 1
run_mild <- 1
run_asymp <- 1

# update as needed: ("worm name", st-gpr best model, DisMod bundle_id, DisMod crosswalk_version)
worm_list <- list(list("ascariasis", ADDRESS, ADDRESS, ADDRESS),
                  list("hookworm", ADDRESS, ADDRESS, ADDRESS),
                  list("trichuriasis", ADDRESS, ADDRESS, ADDRESS))


# index by worm: 1=ascariasis, 2=hookworm, 3=trichuriasis
worm_element <- worm_list[[ADDRESS]]

#############################################################################################
###                                 Main Execution                                        ###
#############################################################################################

my_worm              <- worm_element[[1]]  
model_ver            <- worm_element[[2]]
bundle_id <- worm_element[[3]]
crosswalk_version_id <- worm_element[[4]]

description <- paste0(ADDRESS, model_ver)

stgpr_draw_directory <- paste0("FILEPATH")

study_dems <- readRDS(paste0(data_root, "FILEPATH"))
my_locs    <- study_dems$location_id

meid_dir             <- paste0(params_dir, "FILEPATH")
meids                <- fread(meid_dir)
meids      <- meids[worm == my_worm]
all_cases  <- meids[model == "all"]$meid
heavy <- meids[model == "heavy"]$meid
mild <- meids[model == "mild"]$meid
asymp  <- meids[model == "asymptomatic"]$meid
if (all_cases == 3000) {asymp <- ADDRESS} # hookworm ADDRESS is as estimated as other STH meids, 3139 is subtracted from anemia

ifelse(!dir.exists(paste0(draws_dir, my_worm, "/")), dir.create(paste0(draws_dir, '/', my_worm, "/")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/", all_cases, "/")), dir.create(paste0(draws_dir, my_worm,  "/", all_cases, "/")), FALSE)
ifelse(!dir.exists(paste0(interms_dir, my_worm, "/")), dir.create(paste0(interms_dir, my_worm, "/")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/", heavy, "/")), dir.create(paste0(draws_dir, my_worm, "/", heavy, "/")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/", mild, "/")), dir.create(paste0(draws_dir, my_worm,  "/", mild, "/")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/", asymp, "/")), dir.create(paste0(draws_dir, my_worm,  "/", asymp, "/")), FALSE)

#############################################################################################
###                             Prep Proportions used for splits                          ###
#############################################################################################

## Submit job to prep proportions, only run if needed
# 
LAUNCH_seq(job_name = paste0("compute_sequlae_proportions_", my_worm),
         shell   = my_shell,
         code    = paste0(code_root, "FILEPATH"),
         project = ADDRESS,
         args = list("--my_worm", my_worm),
         m_mem_free = "5G",
         fthread = "1",
         archive = NULL,
         h_rt = "00:00:10:00",
         queue = "long.q")
Sys.sleep(100)

#############################################################################################
###                                 All Prevalence MEIDS                                  ###
#############################################################################################

if (run_all_cases == 1){
   done_files <- list.files(paste0(draws_dir, my_worm,  "FILEPATH"))
   done_locs <- as.integer(str_extract_all(string = done_files, pattern = "[0-9]+", simplify= TRUE))
   
   while (length(done_locs) < length(my_locs)){
      
      est_locs <- setdiff(my_locs, done_locs)
      cat(paste0("Length of all_cases est_locs is ", length(est_locs)))
      
      param_map_impute <- data.table(location_ids = est_locs,
                                     stgpr_draws_directory = stgpr_draw_directory,
                                     gbd_round_id = gbd_round_id,
                                     decomp_step = decomp_step,
                                     my_worm = my_worm)
      
      num_jobs  <- nrow(param_map_impute)
      fwrite(param_map_impute, paste0(interms_dir, my_worm, "FILEPATH"), row.names = F)
      
      LAUNCH(job_name = paste0("Impute_ST-GPR_", my_worm),
           shell    = my_shell,
           code     = paste0(code_root, "FILEPATH"),
           args     = list("--param_path_impute", paste0(interms_dir, my_worm, "FILEPATH")),
           project  = ADDRESS,
           m_mem_free = "2G",
           fthread = "1",
           archive = NULL,
           h_rt = "00:00:05:00",
           queue = "long.q",
           num_jobs = num_jobs)
      
      Sys.sleep(600)
      
      done_files <- list.files(paste0(draws_dir, my_worm,  "FILEPATH"))
      done_locs <- as.integer(str_extract_all(string = done_files, pattern = "[0-9]+", simplify= TRUE))
   }
   
   
   ## Now upload all-prevalence:
   save_results_address(input_dir = paste0(draws_dir, my_worm,  "FILEPATH"),
                    input_file_pattern = "{location_id}.csv",
                    model_id= all_cases,
                    measure_id = 5,
                    metric_id = 3,
                    description = description,
                    mark_best = TRUE,
                    decomp_step = decomp_step,
                    gbd_round_id = gbd_round_id,
                    bundle_id = bundle_id,
                    crosswalk_version_id = crosswalk_version_id)

   cat(paste0("\n Submitted all-prevalence results for ", my_worm, ". \n" ))
}

#############################################################################################
###                                 Heavy - Sequelae Splits                               ###
#############################################################################################
if (run_heavy == 1){
   
   #Submit jobs to compute prevalence of heavy infestation
   done_files <- list.files(paste0(draws_dir, my_worm,  "FILEPATH"))
   done_locs <- as.integer(str_extract_all(string = done_files, pattern = "[0-9]+", simplify= TRUE))
   
   while (length(done_locs) < length(my_locs)){
      
      est_locs <- setdiff(my_locs, done_locs)
      cat(paste0("Length of heavy est_locs is ", length(est_locs)))
      param_map_heavy <- data.table(location_ids = est_locs,
                                    is_heavy = 1,
                                    my_worm = my_worm,
                                    decomp_step = decomp_step, 
                                    gbd_round_id = gbd_round_id)
      
      num_jobs  <- nrow(param_map_heavy)
      fwrite(param_map_heavy, paste0(interms_dir, my_worm, "FILEPATH"), row.names = F)
      
      LAUNCH(job_name = paste0("Heavy_Sequelae_Splits_", my_worm),
           shell    = my_shell,
           code     = paste0(code_root, "FILEPATH"),
           args     = list("--param_path", paste0(interms_dir, my_worm, "FILEPATH")),
           project  = ADDRESS,
           m_mem_free = "5G",
           fthread = "1",
           archive = NULL,
           h_rt = "00:00:20:00",
           queue = "long.q",
           num_jobs = num_jobs)
      
      Sys.sleep(600)
      
      done_files <- list.files(paste0(draws_dir, my_worm,  "FILEPATH"))
      done_locs <- as.integer(str_extract_all(string = done_files, pattern = "[0-9]+", simplify= TRUE))
      
   }
   
   ## Upload
   save_results_address(input_dir = paste0(draws_dir, my_worm, "FILEPATH"),
                    input_file_pattern = "FILEPATH",
                    model_id= heavy,
                    measure_id = 5,
                    metric_id = 3,
                    description = description,
                    decomp_step = decomp_step,
                    mark_best = TRUE,
                    gbd_round_id = gbd_round_id,
                    bundle_id = bundle_id,
                    crosswalk_version_id = crosswalk_version_id)
}

#############################################################################################
###                                Mild -   Sequelae Splits                               ###
#############################################################################################

if (run_mild == 1){
   
   done_files <- list.files(paste0(draws_dir, my_worm,  "FILEPATH"))
   done_locs <- as.integer(str_extract_all(string = done_files, pattern = "[0-9]+", simplify= TRUE))
   
   while (length(done_locs) < length(my_locs)){
      
      est_locs <- setdiff(my_locs, done_locs)
      cat(paste0("Length of mild est_locs is ", length(est_locs)))
      param_map_mild <- data.table(location_ids = est_locs, 
                                   is_heavy = 0, 
                                   my_worm = my_worm, 
                                   decomp_step = decomp_step, 
                                   gbd_round_id = gbd_round_id)
      
      num_jobs       <- nrow(param_map_mild)
      fwrite(param_map_mild, paste0(interms_dir, my_worm, "FILEPATH"), row.names = F)
      
      LAUNCH(job_name = paste0("Mild_Sequelae_Splits_", my_worm),
           shell    = my_shell,
           code     = paste0(code_root, "FILEPATH"),
           args     = list("--param_path", paste0(interms_dir, my_worm, "FILEPATH")),
           project  = ADDRESS,
           m_mem_free = "3G",
           fthread = "1",
           archive = NULL,
           h_rt = "00:00:20:00",
           queue = "long.q",
           num_jobs = num_jobs)
      
      Sys.sleep(600)
      
      done_files <- list.files(paste0(draws_dir, my_worm,  "FILEPATH"))
      done_locs <- as.integer(str_extract_all(string = done_files, pattern = "[0-9]+", simplify= TRUE))
      
   }
   
   
   ## Upload Mild
   save_results_address(input_dir = paste0(draws_dir, my_worm, "/", mild, "/"),
                    input_file_pattern = "{location_id}.csv",
                    model_id= mild,
                    measure_id = 5,
                    metric_id = 3,
                    description = description,
                    decomp_step = decomp_step,
                    mark_best = TRUE,
                    gbd_round_id = gbd_round_id ,
                    bundle_id = bundle_id,
                    crosswalk_version_id = crosswalk_version_id)
}

#############################################################################################
###                              Asymptomatic -   Sequelae Splits                         ###
#############################################################################################

if ( run_asymp == 1){
   done_files <- list.files(paste0(draws_dir, my_worm,  "/", asymp, "/"))
   done_locs <- as.integer(str_extract_all(string = done_files, pattern = "[0-9]+", simplify= TRUE))
   
   while (length(done_locs) < length(my_locs)){
      
      est_locs <- setdiff(my_locs, done_locs)
      cat(paste0("Length of asymp est_locs is ", length(est_locs)))
      
      param_map_asymp <- data.table(location_ids = est_locs, 
                                    my_worm = my_worm,
                                    decomp_step = decomp_step, 
                                    gbd_round_id = gbd_round_id)
      
      num_jobs  <- nrow(param_map_asymp)
      fwrite(param_map_asymp, paste0(interms_dir, my_worm, "_asymp_parameters.csv"), row.names = F)
      
      LAUNCH(job_name = paste0("Asymp_Sequelae_Splits_", my_worm),
           shell    = my_shell,
           code     = paste0(code_root, "FILEPATH"),
           args     = list("--param_path_asymp", paste0(interms_dir, my_worm, "_asymp_parameters.csv")),
           project  = ADDRESS,
           m_mem_free = "5G",
           fthread = "1",
           archive = NULL,
           h_rt = "00:00:30:00",
           queue = "long.q",
           num_jobs = num_jobs)
      
      Sys.sleep(1800) # has to pull several meids so takes longer to run
      
      done_files <- list.files(paste0(draws_dir, my_worm,  "/", asymp, "/"))
      done_locs <- as.integer(str_extract_all(string = done_files, pattern = "[0-9]+", simplify= TRUE))
      
   }
   
   save_results_address(input_dir = paste0(draws_dir, my_worm,  "/", asymp, "/"), 
                    input_file_pattern = "{location_id}.csv",
                    model_id= asymp, 
                    measure_id = 5, 
                    metric_id = 3, 
                    description = description, 
                    mark_best = TRUE,
                    decomp_step = decomp_step,
                    gbd_round_id = gbd_round_id,
                    bundle_id = bundle_id,
                    crosswalk_version_id = crosswalk_version_id)
}

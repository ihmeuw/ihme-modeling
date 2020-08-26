# Purpose: Solve for and submit meids

#'[ General Set-Up]

# clear enviroment
rm(list = ls())

# set root
root <- paste0(FILEPATH, Sys.info()[7], "/")

# create new run dir
source(FILEPATH)
#gen_rundir(root = FILEPATH, acause = "ntd_nema")

# set run dir
run_file <- fread(paste0(FILEPATH))
run_dir <- run_file[nrow(run_file), run_folder_path]

crosswalks_dir    <- paste0(run_dir, FILEPATH)
draws_dir    <- paste0(run_dir, FILEPATH)
interms_dir    <- paste0(run_dir, FILEPATH)

code_dir <- paste0(root, FILEPATH)
params_dir <- FILEPATH
# load packages / central functions / custom functions
library(metafor, lib.loc = FILEPATH)
library(msm, lib.loc = FILEPATH)
library(data.table)
library(ggplot2)
library(openxlsx)
library(stringr)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
library(argparse, lib.loc = paste0(FILEPATH)) 
source(paste0(root, FILEPATH))
source(paste0(root, FILEPATH))

# custom
source(paste0(code_dir, FILEPATH))
source(FILEPATH)
my_shell <- FILEPATH

gbd_round_id <- 6

#############################################################################################
###                                     Main Script                                       ###
#############################################################################################

## Helper vectors
worm_list <- list(list("ascariasis", ADDRESS), list("hookworm", ADDRESS), list("trichuriasis", ADDRESS))

worm_element <- worm_list[[1]]

my_worm              <- worm_element[[1]]  
model_ver            <- worm_element[[2]]

cat(paste0("Estimating for ", my_worm, " stgpr model version ", model_ver, " for prev, heavy, med"))

meid_dir             <- paste0(params_dir, FILEPATH)
meids                <- fread(meid_dir)
stgpr_draw_directory <- paste0(FILEPATH, model_ver, FILEPATH)

## Get locations to parrelize by
year_ids   <- sort(get_demographics("epi", gbd_round_id = 6)$year_id)
year_ids   <- as.numeric(str_split(year_ids, " ", simplify = TRUE))
my_locs    <- sort(get_demographics("epi", gbd_round_id = 6)$location_id)
all_locs   <- sort(get_demographics("epi", gbd_round_id = 6)$location_id)
meids      <- meids[worm == my_worm]
all_cases  <- meids[model == "all"]$meid

## Submit jobs to compute prevalence draws of worm
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/")), dir.create(paste0(draws_dir, my_worm, "/")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/", all_cases, "/")), dir.create(paste0(draws_dir, my_worm,  "/", all_cases, "/")), FALSE)

 param_map_impute <- expand.grid(location_ids = my_locs,
                                 year_ids = paste0(year_ids, collapse = " "),
                                 draws_directory = paste0(draws_dir, my_worm,  "/", all_cases, "/"),
                                 stgpr_draws_directory = stgpr_draw_directory,
                                 meids_directory = meid_dir, my_worm = my_worm,
                                 params_directory = params_dir)
  
 num_jobs  <- nrow(param_map_impute)
 fwrite(param_map_impute, paste0(interms_dir, my_worm, FILEPATH), row.names = F)
 
 qsub(job_name = paste0("Impute_ST-GPR_", my_worm),
      shell    = my_shell,
      code     = paste0(code_dir, FILEPATH),
      args     = list("--param_path_impute", paste0(interms_dir, my_worm, FILEPATH)),
      project  = ADDRESS,
      m_mem_free = "2G",
      fthread = "1",
      archive = NULL,
      h_rt = "00:00:05:00",
      queue = ADDRESS,
      num_jobs = num_jobs)

my_description <- paste0("GBD decomp 4 ascariasis resubmission (age pattern and stgpr w/ step 2 data); ST-GPR model (no. ", model_ver, ") ~step 2 age pattern data and min cv, ~step2 stgpr data")

cat(paste0("Done with the qsubs for ", my_worm, ". Submitting results."))

save_results_epi(input_dir = paste0(draws_dir, my_worm,  "/", all_cases, "/"), input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = all_cases, measure_id = 5, metric_id = 3, description = my_description,
                 mark_best = TRUE, decomp_step = "step4", gbd_round_id = gbd_round_id)

cat(paste0("\n Submitted all-prevalence results for ", my_worm, ". \n" ))


#############################################################################################
###                             Prep Proportions used for splits                          ###
#############################################################################################

ifelse(!dir.exists(paste0(interms_dir, my_worm, "/")), dir.create(paste0(interms_dir, my_worm, "/")), FALSE)
cat(paste0("Prepping proportion for ", my_worm, "."))

# Submit job to prep proportions
qsub_seq(job_name = paste0("compute_sequlae_proportions_", my_worm),
       shell   = my_shell,
       code    = paste0(code_dir, FILEPATH),
       project = ADDRESS,
       args = list("--my_worm", my_worm,
                   "--interms_directory", interms_dir,
                   "--params_directory", params_dir),
       m_mem_free = "5G",
       fthread = "1",
       archive = NULL,
       h_rt = "00:00:10:00",
       queue = ADDRESS)

#############################################################################################
###                                 Heavy - Sequelae Splits                               ###
#############################################################################################

## Grab correct meid
heavy <- meids[model == "heavy"]$meid

#Submit jobs to compute prevalence of heavy infestation
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/", heavy, "/")), dir.create(paste0(draws_dir, my_worm, "/", heavy, "/")), FALSE)
param_map_heavy <- expand.grid(location_ids = my_locs,
                               is_heavy = 1,
                               my_worm = my_worm,
                               draws_directory = paste0(draws_dir, my_worm, "/", heavy, "/"),
                               prop_directory = paste0(interms_dir, my_worm, "/", my_worm, FILEPATH),
                               meids_directory = meid_dir,
                               params_directory = params_dir)

num_jobs  <- nrow(param_map_heavy)
fwrite(param_map_heavy, paste0(interms_dir, my_worm, FILEPATH), row.names = F)

qsub(job_name = paste0("Heavy_Sequelae_Splits_", my_worm),
     shell    = my_shell,
     code     = paste0(code_dir, FILEPATH),
     args     = list("--param_path", paste0(interms_dir, my_worm, FILEPATH)),
     project  = ADDRESS,
     m_mem_free = "5G",
     fthread = "1",
     archive = NULL,
     h_rt = "00:00:20:00",
     queue = ADDRESS,
     num_jobs = num_jobs)

## Wait for jobs to complete
i                <- 0
time_to_resubmit <- 0
resubmits        <- 0

 for (loc in my_locs) {
   while (!file.exists(paste0(draws_dir, my_worm, "/", heavy, "/", loc, ".csv"))) {
     cat(paste0("Waiting for file: ", draws_dir, my_worm, "/", heavy, "/", loc, ".csv -- ", Sys.time(), "\n"))
     cat(paste0(i, " of 990 completed and accounted for."))
     Sys.sleep(30)
     time_to_resubmit <- time_to_resubmit + 30
     cat(paste0("\n resubmit timer at: ", time_to_resubmit, ". Resubmitting array jobs of missing locations in ", 900 - time_to_resubmit, "\n"))

      if (time_to_resubmit > 900){
       time_to_resubmit <- 0
       cat("Resubmitting Heavy Array Job Now.")
       resubmit_heavy(my_worm = my_worm, model_ver = model_ver, all_locs = all_locs)
       cat("Resubmitted Heavy Array Job. Sleeping for 5 minutes")
       Sys.sleep(300)
       time_to_resubmit <- 0
       resubmits        <- resubmits + 1
     }

     if (resubmits > 2){
       cat("Too many heavy resubmits something is wrong.")
       break
     }

   }
   if (file.exists(paste0(draws_dir, my_worm,  "/", heavy, "/", loc, ".csv"))){
   i <- i + 1
}}

## Upload
cat(paste0("\n All heavy draws for ", my_worm, " completed. \n" ))

 my_description <- paste0("GBD resubmission, ST-GPR model (no. ", model_ver, ") ~step 2 age pattern data and min cv, ~step2 stgpr data; Used the EG draws to obtain a proportion of cases that are heavy infestations. ",
                          "Applied this proportion to the new all cases model.")
 
 save_results_epi(input_dir = paste0(draws_dir, my_worm, "/", heavy, "/"), input_file_pattern = "{location_id}.csv",
                  modelable_entity_id = heavy, measure_id = 5, metric_id = 3, description = my_description, decomp_step = "step4",
                  mark_best = TRUE, gbd_round_id = gbd_round_id)

 cat(paste0("\n Submitted heavy prevalence for ", my_worm, ". \n"))
 
#############################################################################################
###                              Medium -   Sequelae Splits                               ###
#############################################################################################
   
## Grab correct meid
mild  <- meids[model == "mild"]$meid
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/", mild, "/")), dir.create(paste0(draws_dir, my_worm,  "/", mild, "/")), FALSE)

## Submit jobs to compute prevalence of medium infestation
param_map_mild <- expand.grid(location_ids = my_locs, 
                              is_heavy = 0, 
                              my_worm = my_worm, 
                              draws_directory = paste0(draws_dir, my_worm, "/", mild, "/"), 
                              prop_directory = paste0(interms_dir, my_worm, "/", my_worm, FILEPATH), 
                              meids_directory = meid_dir,
                              params_directory = params_dir)

num_jobs       <- nrow(param_map_mild)
fwrite(param_map_mild, paste0(interms_dir, my_worm, FILEPATH), row.names = F)

qsub(job_name = paste0("Mild_Sequelae_Splits_", my_worm),
     shell    = my_shell,
     code     = paste0(code_dir, FILEPATH),
     args     = list("--param_path", paste0(interms_dir, my_worm, FILEPATH)),
     project  = ADDRESS,
     m_mem_free = "5G",
     fthread = "1",
     archive = NULL,
     h_rt = "00:00:20:00",
     queue = ADDRESS,
     num_jobs = num_jobs)

## Wait for jobs to complete
i                <- 0
time_to_resubmit <- 0
resubmits        <- 0

 for (loc in my_locs) {
   while (!file.exists(paste0(draws_dir, my_worm, "/", mild, "/", loc, ".csv"))) {
     cat(paste0("Waiting for file: ", draws_dir, mild, "/", loc, ".csv -- ", Sys.time(), "\n"))
     cat(paste0(i, " of 990 completed and accounted for."))
     Sys.sleep(30)
     time_to_resubmit <- time_to_resubmit + 30
     cat(paste0("\n resubmit timer at: ", time_to_resubmit, ". Resubmitting array jobs of missing locations in ", 900 - time_to_resubmit, "\n"))
     
     if (time_to_resubmit > 900){
       cat("Resubmitting Mild Array Job Now.")
       resubmit_mild(my_worm = my_worm, model_ver = model_ver, all_locs = all_locs)
       cat("Resubmitted Mild Array Job. Sleeping for 5 minutes")
       Sys.sleep(300)
       time_to_resubmit <- 0
       resubmits        <- resubmits + 1
     }
     
     if (resubmits > 2){
       cat("Too many mild resubmits something is wrong.")
       break
     }
   }
 if (file.exists(paste0(draws_dir, my_worm,  "/", mild, "/", loc, ".csv"))){
  i <- i + 1
 }}

## Upload
cat(paste0("\n All mild draws for ", my_worm, " completed. \n" ))
 
my_description <- paste0("ST-GPR model (no. ", model_ver, ") ~step 2 age pattern data and min cv, ~step2 stgpr data; Similar to heavy infestation; we obtained a moderate proportion from the EG draws, ",
                          "and applied this proportion to the new all cases model.")
 
save_results_epi(input_dir = paste0(draws_dir, my_worm, "/", mild, "/"), input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = mild, measure_id = 5, metric_id = 3, description = my_description, decomp_step = "step4",
                 mark_best = TRUE, gbd_round_id = gbd_round_id)
 
cat(paste0("\n Submitted mild prevalence for ", my_worm, ". \n"))
cat(paste0("\n \n \n \n Done with worm ", my_worm, "!!! \n \n \n \n"))
 
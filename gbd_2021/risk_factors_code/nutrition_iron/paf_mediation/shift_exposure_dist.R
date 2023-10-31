#
#     Goal: Using the hemoglobin shift calculated in MR-BRT, shift the epi iron exposure distribution
#
#     Launched using launcher at bottom of script
#     Must have the post-anemia causal attribution exposure model marked best!! The best model will be shifted
#
#     GBD 2020
#

source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_best_model_versions.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/save_results_epi.R")
source("FILEPATH/get_elmo_ids.R")
library("reticulate")
library(mrbrt001, lib.loc = "FILEPATH") # for R version 3.6.3


if(interactive()){
  
  shift_model <- "FILEPATH/Hemoglobin_level/pregnant_women_trim_intercept/mod.pkl"
  gbd_round_id <- 7
  decomp_step <- "iterative"
  save_dir <- "FILEPATH/hemoglobin_shifted_exp/"
  save_dir <- paste0(save_dir, "round", gbd_round_id, "_", decomp_step, "/")
  location_id <- 211
  save <- 0
  map_path <- "FILEPATH/task_map.csv"
  
}else{
  
  args <- commandArgs(trailingOnly = T)
  map_path <- args[1]
  save_dir <- args[2]
  shift_model <- args[3]
  gbd_round_id <- args[4]
  decomp_step <- args[5]
  save <- args[6]
  shift_val <- args[7]
  
  params <- fread(map_path)
  task_id <- Sys.getenv("SGE_TASK_ID")
  location_id <- params[task_num == task_id, location]
  files <- paste0(params$location, ".csv")
  
}

# the iron exposure meid, shouldn't ever change
iron_exp_meid <- 8882

# get the best mvid and make a new directory
ids <- get_best_model_versions("modelable_entity",
                               ids = iron_exp_meid,
                               gbd_round_id = gbd_round_id, 
                               status = "best", 
                               decomp_step = decomp_step)

orig_save_dir <- save_dir
save_dir <- paste0(save_dir, "/mvid_", ids$model_version_id, "_ivshift")
if(!dir.exists(save_dir)){
  dir.create(save_dir)
}


# read in the shift model
iv_shift_model <- py_load_object(shift_model)


if(save==0){

  exposure_draws <- get_draws(gbd_id_type = "modelable_entity_id", 
                              gbd_id = iron_exp_meid, 
                              source = "epi", 
                              location_id = location_id, 
                              version_id = ids$model_version_id,
                              gbd_round_id = gbd_round_id,
                              decomp_step = decomp_step)
  
  
  #reshape long
  exposure <- melt(exposure_draws, measure.vars = c(paste0("draw_",0:999)) )
  
  #predict
  pred_data <- exposure[, `:=` (intercept = 0, iv_oral = 1, oral_placebo = 1)]
  dat_pred1 <- MRData()
  
  dat_pred1$load_df(
    data = pred_data,
    col_covs = as.list(c("iv_oral", "oral_placebo"))
  )
  
  exposure$iv_shift <- iv_shift_model$predict(dat_pred1, sort_by_data_id = T)
  
  shift_val <- unique(exposure$iv_shift)
  
  exposure[, value:=value+abs(iv_shift)]
  exposure[, `:=` (iv_shift=NULL, intercept=NULL, iv_oral=NULL, oral_placebo=NULL)]
  
  #reshape wide
  shift_draws <- as.data.table(dcast(exposure, ... ~ variable, value.var = "value"))
  
  write.csv(shift_draws, paste0(save_dir, "/", location_id, ".csv"), row.names = F)
  
  
  # check to see if all locations are saved in the folder!
  comp_files <- list.files(save_dir)
  not_complete <- setdiff(files, comp_files)
  if(length(not_complete) ==0){
  
    print("All locations have finished. Launching save results epi now")
    log_dir <- paste0("FILEPATH",Sys.info()['user'])
    system(paste0("qsub -N save_iron_shifted_dist",
                  " -P proj_diet -l m_mem_free=80G -l fthread=20 -q all.q",
                  " -o ", log_dir, "/output -e ", log_dir, "/errors ",
                  "FILEPATH/execRscript.sh ",
                  "-i FILEPATH/ihme_rstudio_3631.img ",
                  "-s FILEPATH/shift_exposure_dist.R ",
                  map_path, " ", orig_save_dir, " ", shift_model, " ", gbd_round_id, " ", decomp_step, " 1 ", shift_val))
    
  }

}else if(save==1){
  
    print("Beginning to save results")
  
    # iron traditionally uses the hemoglobin bundle id (4754) when uploading 
    metadata <- get_elmo_ids(bundle_id = 4754, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
    metadata <- metadata[modelable_entity_id==10487 & is_best==1]
    d <- save_results_epi(input_dir= save_dir,
                     input_file_pattern = "{location_id}.csv",
                     modelable_entity_id = iron_exp_meid, 
                     measure_id = 19,
                     decomp_step = decomp_step, 
                     gbd_round_id = gbd_round_id,
                     bundle_id = metadata$bundle_id, 
                     crosswalk_version_id = metadata$crosswalk_version_id,
                     description = paste0("mvid ", ids$model_version_id, " shifted by IV supp shift of ", shift_val)
                     )
    print(d)
    write.csv(d, paste0(save_dir, "/", d$model_version_id, "_saved.csv"), row.names = F)
    
}




### Launch this script
if(F){
  
  source("FILEPATH/get_location_metadata.R")
  log_dir <- paste0("FILEPATH",Sys.info()['user'])
  decomp_step <- "iterative"
  gbd_round_id <- 7
  
  save_dir <- "FILEPATH/hemoglobin_shifted_exp/"
  save_dir <- paste0(save_dir, "round", gbd_round_id, "_", decomp_step, "/")
  if(!dir.exists(save_dir)){dir.create(save_dir)}
  
  shift_model <- "FILEPATH/pregnant_women_trim_wcovs_IVoral/mod.pkl"
  loc_dt <- get_location_metadata(location_set_id = 22, gbd_round_id = gbd_round_id)
  loc_dt <- loc_dt[is_estimate == 1 & most_detailed == 1]
  params <- data.table(location = loc_dt[, location_id])
  params[, task_num := 1:.N]
  map_path <- paste0(save_dir, "task_map.csv")
  write.csv(params, map_path, row.names = F)
  
  
  system(paste0("qsub -N iron_shift_dist",
                " -P proj_diet -l m_mem_free=10G -l fthread=5 -q all.q",
                " -o ", log_dir, "/output -e ", log_dir, "/errors ",
                "-t 1:", nrow(params), " ",
                "FILEPATH/execRscript.sh ",
                "-i FILEPATH/ihme_rstudio_3631.img ",
                "-s FILEPATH/shift_exposure_dist.R ",
                map_path, " ", save_dir, " ", shift_model, " ", gbd_round_id, " ", decomp_step, " 0"))
  
  
}





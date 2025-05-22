#
#     Goal: Using the hemoglobin shift calculated in MR-BRT, shift the epi iron exposure distribution
#
#     Launched using launcher at bottom of script
#     Must have the post-anemia causal attribution exposure model marked best!! The best model will be shifted


source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_best_model_versions.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/save_results_epi.R")
source("FILEPATH/get_elmo_ids.R")
library("reticulate")
library(mrbrt001, lib.loc = "FILEPATH") # for R version 3.6.3


if(interactive()){
  
  shift_model <- "FILEPATH/Hemoglobin_level/pregnant_women_trim_intercept/mod.pkl"
  release_id <- 16
  save_dir <- "FILEPATH/hemoglobin_shifted_exp/"
  save_dir <- paste0(save_dir)
  location_id <- 211
  save <- 0
  map_path <- "FILEPATH/task_map.csv"
  
}else{
  
  args <- commandArgs(trailingOnly = T)
  map_path <- args[1]
  save_dir <- args[2]
  shift_model <- args[3]
  release_id <- args[4]
  save <- args[5]
  shift_val <- args[6]
  
  params <- fread(map_path)
  task_id <- Sys.getenv("SGE_TASK_ID")
  location_id <- params[task_num == task_id, location]
  files <- paste0(params$location, ".csv")
  
}


iron_exp_meid <- 8882

# get the best mvid and make a new directory
ids <- get_best_model_versions("modelable_entity",
                               ids = iron_exp_meid,
                               status = "best", 
                               release_id = release_id)

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
                              release_id = release_id)
  
  
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

    jobname = "save_iron_shifted_dist"
    proj_name = "proj_diet"
    system(paste0("sbatch -J ", jobname,,
                   " -A ", proj_name, " --mem=80G -c 20 -t 384:00:00 -p long.q",
                  " -o ", log_dir, "/output/%x.o%j -e ", log_dir, "/errors/%x.e%j ",
                  "FILEPATH/execRscript.sh ",
                  "-i FILEPATH/ihme_rstudio_3631.img ",
                  "-s FILEPATH/shift_exposure_dist.R ",
                  map_path, " ", orig_save_dir, " ", shift_model, " ", release_id, shift_val))
    
  }




}else if(save==1){
  
    print("Beginning to save results")
  
    metadata <- get_elmo_ids(bundle_id = 4754, release_id = release_id)
    metadata <- metadata[modelable_entity_id==10487 & is_best==1]
    d <- save_results_epi(input_dir= save_dir,
                     input_file_pattern = "{location_id}.csv",
                     modelable_entity_id = iron_exp_meid, 
                     measure_id = 19,
                     release_id = release_id,
                     bundle_id = metadata$bundle_id, 
                     crosswalk_version_id = metadata$crosswalk_version_id,
                     description = paste0("mvid ", ids$model_version_id, " shifted by IV supp shift of ", shift_val)
                     )
    print(d)
    write.csv(d, paste0(save_dir, "/", d$model_version_id, "_saved.csv"), row.names = F)
    
}







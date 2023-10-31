# #
#     Goal: Bring in two version ids of recently run PAFs, get draws for those. Take the difference and save csv. BY LOCATION
#           Norm_version - shifted_version = final_paf
#
#     Launched using launcher at bottom of script
#     Must know the two PAF version_ids. 
#
#


library(data.table)
source("FILEPATH/get_best_model_versions.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/save_results_risk.R")
source("FILEPATH/get_elmo_ids.R")


# Load arguments
if(interactive()){
  
  save_dir <- "/FILEPATH/"
  gbd_round_id <- 7
  decomp_step <- "iterative"
  location_id <- 555
  save <- 0
  norm_version <- 564257
  shifted_version <- 595925
  
}else{
  
  args <- commandArgs(trailingOnly = T)
  map_path <- args[1]
  save_dir <- args[2]
  gbd_round_id <- args[3]
  decomp_step <- args[4]
  save <- args[5]
  norm_version <- args[6]
  shifted_version <- args[7]
  
  params <- fread(map_path)
  task_id <- Sys.getenv("SGE_TASK_ID")
  location_id <- params[task_num == task_id, location]
  files <- paste0(params$location, ".csv")
  
  }
#----

# Iron PAF meid
iron_paf_meid <- 8755
iron_rei_id <- 95


if(save==0){
  
  
  idvars <- c("rei_id", "modelable_entity_id", "location_id", "year_id", "age_group_id", "sex_id", "cause_id",
              "measure_id", "metric_id")
  
  norm_draws <- get_draws(gbd_id_type = "rei_id", 
                          gbd_id = iron_rei_id, 
                          source = "paf", 
                          location_id = location_id, 
                          version_id = norm_version,
                          gbd_round_id = gbd_round_id,
                          decomp_step = decomp_step)
  norm_draws <- melt(norm_draws, id.vars = idvars, value.name = "norm_val", variable.name = "draw" )
  
  
  shifted_draws <- get_draws(gbd_id_type = "rei_id", 
                             gbd_id = iron_rei_id, 
                             source = "paf", 
                             location_id = location_id, 
                             version_id = shifted_version,
                             gbd_round_id = gbd_round_id,
                             decomp_step = decomp_step)
  shifted_draws <- melt(shifted_draws, id.vars = idvars, value.name = "shifted_val", variable.name = "draw" )
  
  
  
  # take the difference -> norm - shifted
  draws <- merge(shifted_draws, norm_draws, by = c(idvars, "draw"))
  draws[, value:= norm_val - shifted_val]
  draws[, `:=` (norm_val=NULL, shifted_val=NULL)]
  
  # check if any are negative -> can't have negative PAFs
  neg <- draws[value < 0,]
  if(nrow(neg) > 0 ){
    
    print(neg)
    warning("Some draws are negative - this shouldn't happen!!")
    
  }
  
  
  # reshape wide and save
  draws <- as.data.table(dcast(draws, ... ~ draw, value.var = "value"))
  draws[, `:=` (modelable_entity_id = NULL, metric_id = NULL)]
  
  write.csv(draws, paste0(save_dir, "/", location_id, ".csv"), row.names = F)
  
  
  # check to see if all locations are saved in the folder!
  comp_files <- list.files(save_dir)
  not_complete <- setdiff(files, comp_files)
  if(length(not_complete) ==0){
    
    
    # write log so only one save results job gets launched
    logdir <- paste0(gsub(paste0(norm_version, ".*$"), "", save_dir), "logs/")
    logfile <- paste0(logdir, "save_", norm_version, "_", shifted_version, "_launched.csv")
    
    # pause for a unique time so only one job gets launched.
    pause_secs <- task_id * 30/nrow(params)
    Sys.sleep(pause_secs)
    
    if(!file.exists(logfile)){
    
      write.csv(data.table(), logfile, row.names = F)
      
      print("All locations have finished. Launching save results risk now")
      log_dir <- paste0("FILEPATH",Sys.info()['user'])
      system(paste0("qsub -N save_iron_custom_PAF",
                    " -P proj_diet -l m_mem_free=70G -l fthread=25 -q long.q",
                    " -o ", log_dir, "/output -e ", log_dir, "/errors ",
                    "FILEPATH/execRscript.sh ",
                    "-s FILEPATH/calc_final_paf_save.R ",
                    map_path, " ", save_dir, " ", gbd_round_id, " ", decomp_step, " 1 ",
                    norm_version, " ", shifted_version))
    
    }
  }
  
}else if(save==1){
  
  d <- save_results_risk(input_dir= save_dir,
                        input_file_pattern = "{location_id}.csv",
                        modelable_entity_id = iron_paf_meid, 
                        decomp_step = decomp_step, 
                        gbd_round_id = gbd_round_id,
                        sex_id = 2,
                        n_draws = 1000,
                        risk_type = "paf",
                        mark_best = T,
                        description = paste0("Custom PAF: difference between PAF versions ", norm_version, " and ", shifted_version, " for direct effect of iron" )
  )
  print(d)
  write.csv(d, paste0(save_dir, "/", d$model_version_id, "_saved.csv"), row.names = F)
  
}


### Launch this script
if(F){
  
  source("FILEPATH/get_location_metadata.R")
  log_dir <- paste0("FILEPATH/",Sys.info()['user'])
  
  decomp_step <- "iterative"
  gbd_round_id <- 7
  
  norm_version <- 625958 
  shifted_version <- 625985
  top_dir <- "FILEPATH"
  save_dir <- paste0(top_dir, "round", gbd_round_id, "_", decomp_step)
  dir.create(save_dir)
  save_dir <- paste0(save_dir, "/", norm_version, "_", shifted_version, "/")
  dir.create(save_dir)
  
  
  loc_dt <- get_location_metadata(location_set_id = 22, gbd_round_id = gbd_round_id)
  loc_dt <- loc_dt[is_estimate == 1 & most_detailed == 1]
  params <- data.table(location = loc_dt[, location_id])
  params[, task_num := 1:.N]
  map_path <- paste0(top_dir, "task_map.csv")
  write.csv(params, map_path, row.names = F)
  
  system(paste0("qsub -N calc_iron_custom_PAF",
                " -P proj_diet -l m_mem_free=30G -l fthread=10 -q all.q",
                " -o ", log_dir, "/output -e ", log_dir, "/errors ",
                "-t 1:", nrow(params), " ",
                "FILEPATH/execRscript.sh ",
                "-s FILEPATH/calc_final_paf_save.R ",
                map_path, " ", save_dir, " ", gbd_round_id, " ", decomp_step, " 0 ",
                norm_version, " ", shifted_version))
  
}





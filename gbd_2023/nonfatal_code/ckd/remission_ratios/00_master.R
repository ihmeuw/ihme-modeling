#Purpose: add_bundle - add dummy seq to bundle
#         calc_ratios - calculate remission ratios for stage 3 to 4, stage 4 to 5 and dialysis to transplant
#         append_and_save - append remission ratios to crosswalk

# BEFORE running ratio calculation - make sure stage 3, 4, and dialysis intermediate models without remission are bested
# RUN ratio calculation for stage 4 and dialysis first, then run final models with remission and best, then run ratio calculation for stage 3 -> run stage 3 model with remission and best

library(data.table)
require(openxlsx)
user <- Sys.info()["user"]
code_dir <- paste0("/FILEPATH/",user,"/FILEPATH")
invisible(sapply(list.files("FILEPATH/", full.names = T), source)) 
script_01 <- paste0(code_dir, "FILEPATH/01_ratios.R")
script_02 <- paste0(code_dir, "FILEPATH/02_append_save.R")

source(paste0(code_dir, "/general_func_lib.R"))
scratch <- "FILEPATH"

process_cv_ref_map_path <- paste0("FILEPATH/process_cv_reference_map.csv")
bvids <- as.data.table(fread(process_cv_ref_map_path))


######################################
# User input variables
######################################
output_version <- "gbd2023"
release_id <- 16

add_dummy_seq <- F
# list of bundles to add dummy seq to
bundles<- c(185) # 182, 183

ratios <- F
# variables for which stage to calc ratios for
stg_3 <- F
stg_4 <- F
stg_dial <- F

append_xwalk <- T
# list of dismod cross walk for each bundle to append remission ratios to.
# these can be found in cvid_pre_remission column of bvids
xwalk_ver_ids <- c(44733)


######################################

seq_track_path <- "FILEPATH/dummy_seq_remission_tracker.csv"

date <- gsub("-", "_", Sys.Date())
location_dt <- get_location_metadata(location_set_id = 9, release_id =  release_id)
location_dt <- location_dt[most_detailed == 1, list(location_id, location_name, location_type)]
locations <- location_dt[, location_id]


if(add_dummy_seq){
  # for each bundle_id stated , added dummy seq value, nid, and null underlying nid
  # saves new bundle and bundle version, saving it to tracker
  for(bun_id in bundles){
    if(bun_id == 182){
      temp_nid <- 251359
    } else if(bun_id == 183){
      temp_nid <- 251358
    } else if(bun_id == 185){
      temp_nid <- 294381
    }else{
      stop(paste0("bundle_id: ",bun_id, " does not require temp/ dummy seq"))
    }
    
    bundle_df <- as.data.table(get_bundle_data(bun_id))
    if(nrow(bundle_df[note_modeler == "dummy_seq"]) > 0){
      dummy <- bundle_df[note_modeler == "dummy_seq"]
      dummy_seqs <- dummy[,seq]
      stop(paste0("bundle_id: ",bun_id, " already has a temp/ dummy seq: ", dummy_seqs))
    }
    # creates dummy seq row values to be added
    add <- data.table()
    add[, `:=` (underlying_nid = NA,
                nid = temp_nid,
                note_modeler = "dummy_seq",
                is_outlier = 1,
                seq = NA,
                year_start = 2010,
                year_end = 2010,
                age_start = 0,
                age_end = 99,
                source_type = "Unidentifiable",
                sampling_type = NA,
                representative_name = "Unknown",
                urbanicity_type = "Mixed/both",
                recall_type = "Point",
                recall_type_value = NA,
                unit_type = "Person",
                uncertainty_type = NA,
                input_type = NA,
                measure = "remission",
                mean = 1,
                upper = NA,
                lower = NA,
                standard_error = 0.5,
                effective_sample_size = NA,
                sample_size = NA,
                cases = NA,
                design_effect = NA,
                unit_value_as_published = 1,
                uncertainty_type_value = NA,
                sex = "Both")]
    
    location_merge <- subset(location_dt, select = location_id)
    location_merge[, seq := NA]
    
    add <- merge(add, location_merge, by = "seq")
    
    # writes new bundle excel file, then uploads and then saves bundle ver
    filepath <- paste0("FILEPATH/", bun_id, "FILEPATH/",date, "_dummy_seq.xlsx")
    write.xlsx(add,filepath, sheetName = 'extraction', row.names = FALSE)
    result <- upload_bundle_data(bundle_id = bun_id,filepath = filepath)
    bun_ver <- save_bundle_version(bundle_id = bun_id)
    bvid <- bun_ver$bundle_version_id
    print(sprintf('Bundle version ID: %s', bvid))
    
    # saving dummy_seq_remission_tracker.csv and process_cv_reference_map.csv
    new_bun <- get_bundle_version(bundle_version_id = bvid)
    dummy_seq <- new_bun[note_modeler == "dummy_seq", seq]
    dummy_seq <- paste(unlist(dummy_seq),collapse=", ")
    
    dummy_seq_tracker <- as.data.table(read.csv(seq_track_path))
    
    dummy_seq_tracker$bundle_ver_id[dummy_seq_tracker$bundle_id == bun_id] <-  bvid
    dummy_seq_tracker$dummy_seq[dummy_seq_tracker$bundle_id == bun_id] <-  paste(dummy_seq)
    dummy_seq_tracker$date_seq_added[dummy_seq_tracker$bundle_id == bun_id] <-  date
    write.csv(dummy_seq_tracker, file = seq_track_path , row.names = FALSE)
    print(paste0("Finished adding dummy seq to bundle_id: ",bun_id))
    print(paste0("New bundle version: ", bvid))
    
    bvids<-fread(process_cv_ref_map_path)
    bvids$bundle_version_remission_dummy[bvids$bundle_id==bun_id] <- bvid
    fwrite(bvids, file = process_cv_ref_map_path, append = FALSE)
  }
}

calc_ratios_helper <- function(num_me,num_bundle,denom_me,denom_bundle, file_path, stg){
  # helper function that creates sbatch for each location within each stage
  runt <- "0:50:00"
  thrds <- 1
  mem <- "3G" 
  for (loc in locations) {
    # create sbatch
    location_type <- location_dt[location_id == loc, location_type]
    job_n <- paste0(stg, loc)
    pass_args <- list(loc, num_me, num_bundle, denom_me, denom_bundle, file_path, location_type,release_id)
    construct_sbatch(
      memory = mem, threads = thrds, runtime = runt, script = script_01,
      pass = pass_args, job_name = job_n, submit = T)
  }
}

if(ratios){
  # submits sbatch jobs for each stage listed as true in the user input variables to calculate remission ratios
  if(stg_4){ 
    out_dir <- paste0(scratch,output_version,"FILEPATH/remission_ratios")
    dir.create(out_dir, recursive = TRUE)
    calc_ratios_helper(num_me = 2022, num_bundle = 186, denom_me = 2019, denom_bundle = 183,file_path = out_dir, stg = "stg_4_5_")
  }
  
  if(stg_3){ 
    out_dir <- paste0(scratch,output_version,"FILEPATH/remission_ratios")
    dir.create(out_dir, recursive = TRUE)
    calc_ratios_helper(num_me = 2019, num_bundle = 183, denom_me = 2018, denom_bundle = 182,file_path = out_dir, stg = "stg_3_4_")
  }
  
  if(stg_dial){
    out_dir <- paste0(scratch,output_version,"FILEPATH/remission_ratios")
    dir.create(out_dir, recursive = TRUE)
    calc_ratios_helper(num_me = 2020, num_bundle = 184, denom_me = 2021, denom_bundle = 185,file_path = out_dir, stg = 'stage_dial_')
  }
}


append_helper <- function(xwalk_id){
  # takes in crosswalk id and checks bundle code book to return bundle_id, bundle_version, and filepath to the remission
  # data , as a list of bundle_in, bundle_cause, bundle_ver, remis_path
  elmo_dat <- as.data.table(get_elmo_ids(crosswalk_version_id = xwalk_id))
  bundle_in <- elmo_dat[1,bundle_id]
  cause <- elmo_dat[1,bundle_name]
  stage <- strsplit(cause," ")[[1]]
  if(stage[2] == "III" ){
    stage_num <- "stage_3_4"
  }else if(stage[2] == "IV"){
    stage_num <- "stage_4_5"
  }else if(stage[5] == "dialysis"){
    stage_num <- "stage_dial_xplant"
  }
  remis_path <- paste0(scratch,output_version,"/",stage_num,"/remission_ratios")
  xwalk_path <- paste0(scratch,output_version,"/",stage_num,"/xwalk")
  
  bvids<-as.data.table(fread(process_cv_ref_map_path))
  bundle_ver <- bvids[bundle_id == bundle_in,]$bundle_version_remission_dummy
  return(c(bundle_in, stage_num,bundle_ver,remis_path, xwalk_path))
}

if(append_xwalk){ 
  # get current crosswalk and append all 
  runt <- "12:00:00"
  thrds <- 10
  mem <- "30G"
  for (xwalk_id in xwalk_ver_ids) {
    bundle_info <- append_helper(xwalk_id)
    out_dir <- bundle_info[4]
    cause <- bundle_info[2]
    denom_bundle_ver <- bundle_info[3]
    bundle_id <- bundle_info[1]
    xwalk_path <- bundle_info[5]
    job_n <- paste0(cause,"_append")
    pass_args<-list(out_dir, output_version, cause, denom_bundle_ver, xwalk_id, release_id, bundle_id, xwalk_path)
    construct_sbatch(
      memory = mem, threads = thrds, runtime = runt, script = script_02,
      pass = pass_args, job_name = job_n, submit = T)
  }
}



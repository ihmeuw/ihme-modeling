
## Run all portions of Ensemble process
## Involving, for Group 1 (GEN): Average Spectrum and Envelope HIV results
## For Group 2A (Non-GEN incomplete): Subtract ST-GPR results from all-cause
## For Group 2B/2C: Use CIBA Spectrum results 

## Outputs: post-reckoning, without-shock, with-HIV and HIV-deleted envelopes and lifetables

###############################################################################################################
## Set up settings
  rm(list=ls())
  library(foreign); library(RMySQL); library(dplyr); library(data.table); library(rhdf5)
  
  if (Sys.info()[1]=="Windows") {
    root <- "filepath" 
    user <- Sys.getenv("USERNAME")
  } else {
    root <- "filepath"
    user <- Sys.getenv("USER")
  }
  
## Grab functions
  func_dir <- paste0("filepath")

###############################################################################################################
## Set start and end options
## Note: These are categorized toggles rather than numeric because of the number of possibilities that can be run in parallel
## First, do you want to run the ensemble process?
  run_ensemble <- T
  
## Run LTs based off of ensemble
  run_lts <- T
  
## Run envelope aggregation and prep for upload
  env_agg <- T
  lt_agg <- T

## Run envelope and/or upload, and potentially mark as best
  upload_results <- T
  mark_best <- F
  upload_lt_results <- T

## Create graphs of the envelope
create_graphs <- T

## Set other run options
  spec_name <- "filepath"# Update this with the new Spectrum run results each time
  test <- F # Test submission of everything 
  file_del <- F
  run_comment <- ""

###############################################################################################################
## Get version number, create process lineages, make directories
  new_upload_version <- gen_new_version(model_name="", model_type="", 
                               comment=run_comment)
  print(paste0("Creating new version ", new_upload_version))
  

  # Get parent processes of the version, and update version mapping appropriately
  # Grab life table and population numbers
  lt_version <- get_proc_version(model_name="", model_type="", run_id="")
  pop_version <- get_proc_version(model_name="", model_type="", run_id="")
  
  gen_parent_child(child_process="", child_id=new_upload_version,
                   parent_process="", parent_id=lt_version)
  
  ## Set code_dir
  code_dir <- paste0("filepath")
  setwd(code_dir)
  
  # Tag Git commit with the current version number
  git_tag(code_dir=code_dir, name=paste0("v",new_upload_version), message=run_comment)
  
## Set directories
  master_dir <- paste0("filepath")
  
## Grab locations
  codes <- get_locations(level = "lowest")
  run_countries <- unique(codes$ihme_loc_id)
  write.csv(codes,paste0(filepath))

## Find and save an age map for use in 01_ensemble for formatting
  age_map <- get_age_map()
  write.csv(age_map,paste0("filepath"))

## Set years that you are running for 
  years <- c(1970:2016)
  
  
## Save spectrum version in a flat file, until HIV is included in DB tracking for Mortality DB
  hiv_output <- data.table(used_spec_run = spec_name)
  write.csv(hiv_output,paste0("filepath"),row.names=F)
  
## Create draw maps to scramble draws so that they are not correlated over time
  ## This is because Spectrum output is semi-ranked due to Ranked Draws into EPP
  ## Then, it propogates into here which would screw up downstream processes
  ## We do the loop thing to make sure that each location_id has their seed set to the appropriate 
  create_draws <- function(location_id) {
    new_seed <- location_id * 100 + 121 # Just to avoid any unintended correlation with other seeds that rely on location_id
    set.seed(new_seed)
    data <- data.table(expand.grid(location_id=location_id,old_draw=c(0:999)))
    data[,draw_sort:=rnorm(1000)]
    data <- data[order(location_id,draw_sort),]
    data[,new_draw:=seq_along(draw_sort)-1,by = location_id] # Creates a new variable with the ordering based on the values of draw_sort 
    data[,draw_sort:=NULL]
  }
  draw_map <- rbindlist(lapply(unique(codes$location_id),create_draws))
  
  write.csv(draw_map,paste0("filepath"),row.names=F)

  
###############################################################################################################
## Create HIV type file
## HIV version of get locations
source(paste0("filepath"))
hiv_groups <- get_locations()

## Create groups list
hiv_groups <- as.data.table(merge(codes, hiv_groups, by = c("ihme_loc_id", "location_id")))
master_types <- hiv_groups[,list(location_id, ihme_loc_id, group)]

write.csv(master_types,paste0("filepath"),row.names=F)
  

###############################################################################################################
## Submit Jobs

## Run ensemble process
   if (run_ensemble==T) {  
    for(country in unique(codes$ihme_loc_id)) {
      group <- unique(master_types$group[master_types$ihme_loc_id==country])    
      qsub(paste0("ensemble_01_",country),paste0(code_dir,"/01_run_ensemble.R"), slots = 3,  pass=list(country,group,spec_name,new_upload_version), proj="proj_mortenvelope", submit = !test, log = T)
    }
  }
    
    print("Waiting 5 minutes before checking results for 01_ensemble")
    Sys.sleep(60*5)


# # ## Start envelope aggregation to region/age/sex aggregates now
  if (env_agg == T) {
    env_agg_jobs <- c()
    for(type in c("hiv_free","with_hiv")) {
      for (year in years){
        job_title <- paste0("agg_env_02a_",type,"_",year)
        env_agg_jobs <- c(env_agg_jobs, job_title)
        qsub(job_title,paste0(code_dir,"/02a_agg_env.R"), slots = 4, pass= list(type, year, new_upload_version), proj="proj_mortenvelope", intel=T, submit = !test, log = T)
      }
    }
  }

#
## At the same time, create new LTs based off of the new MX results from HIV
  if (run_lts==T) {
    for(country in run_countries) {
      group <- unique(master_types$group[master_types$ihme_loc_id==country])
      loc_id <- unique(codes$location_id[codes$ihme_loc_id == country])
      qsub(paste0("lt_gen_02_",country),paste0(code_dir,"/02_lt_gen.R"), slots = 4,  pass=list(country,loc_id,group,spec_name,new_upload_version), proj="proj_mortenvelope", submit = !test, log = T)
  }

  }
    
## Create age, sex, and region/global envelope aggregates at the draw level, and create draw- and summary-level files
  if (lt_agg == T) {
    for(type in c("hiv_free","with_hiv")) {
      ## Submit code to aggregate from mx_ax at country to mx_ax at region level
      for (year in years){
        qsub(paste0("agg_mx_ax_",type, "_", year),paste0(code_dir,"/02a_agg_mx_ax.R"), slots = 5,pass=list(type,year,new_upload_version), proj="proj_mortenvelope", submit = !test, log = T)
      }

    }

    ## Submit jobs to create summary 5q0 and 45q15 results for aggregate locations
    lowest <- data.table(get_locations(level="lowest"))
    aggs <- data.table(get_locations(level="all"))
    aggs <- unique(aggs[!location_id %in% unique(lowest[,location_id]),location_id])

    sdi_map <- data.table(get_locations(gbd_type="sdi", gbd_year = 2015))
    sdi_locs <- unique(sdi_map[level==0,location_id])

    agg_countries <- c(aggs,sdi_locs)

    for(type in c("hiv_free","with_hiv")) {
      for(country in agg_countries) {
        qsub(paste0("calc_agg_qx_",country,"_",type),paste0(code_dir,"/02d_calc_agg_qx.R"), slots = 4,  pass=list(country,type,new_upload_version), proj="proj_mortenvelope", submit = !test, log = T)
      }
    }
    

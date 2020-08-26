#--------------------------------------------------------------
# Project: GBD Non-fatal CKD Estimation - Sex-splits
# Purpose: Prep microdata raw bundle data to estimate 
# coefficients for sex-splits outside of DisMod
#--------------------------------------------------------------

# source functions
source(paste0("FILEPATH", "/function_lib.R"))
source(paste0("FILEPATH","/get_bundle_version_ids.R"))

  # DATA PREP SETTINGS ---------------------------
    # Set decomp step
    decomp_step<-"step2"
    
    # Set bundles to run for
    # ckd_bids<-c(183,186,760,670,551,3110,3113,552,553,554)
    ckd_bids<-c(917)
    
    # Pull in most recent bundle version ids to prep for sex splitting
    bvids<-get_bundle_version_ids(ckd_bids)
    bvids[,last_updated:=as.POSIXct(last_updated,format='%Y-%m-%d %H:%M:%S')]
    bvids<-bvids[decomp_step_id==as.numeric(gsub("\\D","",decomp_step)),
                 .SD[which.max(last_updated)],keyby=bundle_id]
    if (nrow(bvids)==0) stop("No bundle version ids for this decomp step")
    
    # Choose whether or not to add an offset to points with a value of 0
    apply_offset<-F
    offset_pcts<-c(0)
    
    # Set model type : logit_diff or log_diff
    mod_type<-"log_diff"
  # ----------------------------------------------
  
  # DATA ANALYSIS SETTINGS -----------------------
    # Choose whether or not to apply trimming in MRBRT model
    apply_trimming<-T
    trim_pcts<-c(0.1)
  # ----------------------------------------------
  
  # SYSTEM SETTINGS ------------------------------
    # Specify usage params
    threads <- 5
    mem <- 3
    runtime <- "00:10:00"
    shell <- 'FILEPATH'
    queue <- "all.q"
    submit <- T
    
    #Scripts
    data_prep_script <- paste0("FILEPATH")
    run_mrbrt <- paste0("FILEPATH")
    
    # Job type: Either one of "data_prep" or "mrbrt". Job Type of "data_prep" preps sex split data for mrtbrt 
    # while a value of "mrbrt" runs sex split models for each bundle.
    job_type <- "mrbrt"
    
  # ----------------------------------------------
    
#   -----------------------------------------------------------------------


# launch ------------------------------------------------------------------
  # Launch cleaning jobs for each bundle
  
  if (job_type == "data_prep") {
    for (bid in ckd_bids){
      error_path <- paste0("FILEPATH")
      if (!file.exists(error_path)){dir.create(error_path,recursive=T)}
      for(offset_pct in offset_pcts){
        job_name<- paste0('prep_sex_split_inputs_bid_',bid,"_offset_",offset_pct)
        bvid<-unique(bvids[bundle_id==bid,bundle_version_id])
        acause<-unique(bvids[bundle_id==bid,acause])
        pass_args<-list(threads, bvid, decomp_step, apply_offset, offset_pct, acause, 
                        mod_type,bid)
        construct_qsub(mem_free = mem, threads = threads, runtime = runtime, script = data_prep_script, jname = job_name, submit = submit,
                       q = queue, pass = pass_args, errors = error_path)
      }
    }
  } else if (job_type == "mrbrt") {
    # WAIT UNTIL PREP JOBS ARE DONE TO LAUNCH
    # Launch MRBRT models for each stage-crosswalk
    for (bid in ckd_bids){
      error_path <- paste0("FILEPATH")
      if (!file.exists(error_path)){dir.create(error_path,recursive=T)}
      for(offset_pct in offset_pcts){
        for (trim_pct in trim_pcts){
          job_name<- paste0('MRBRT_sex_split_mod_bid_',bid,"_",trim_pct,"_",offset_pct)
          bvid<-unique(bvids[bundle_id==bid,bundle_version_id])
          acause<-unique(bvids[bundle_id==bid,acause])
          # Set directory where MRBRT models should be saved
          mrbrt_dir<-paste0("FILEPATH")
          if(!dir.exists(mrbrt_dir)){dir.create(mrbrt_dir)}
          apply_trimming<-ifelse(trim_pct==0,F,T)
          pass_args<-list(bid,acause,mrbrt_dir,apply_trimming,trim_pct,apply_offset,offset_pct,mod_type,bvid)
          construct_qsub(mem_free = mem, threads = threads, runtime = runtime, script = run_mrbrt, jname = job_name, submit = submit,
                         q = queue, pass = pass_args, errors = error_path)
        }
      }
    }
  } else {
    stop(paste0("job_type value ", job_type, " is not a valid argument."))
  }
  
  

#   -----------------------------------------------------------------------
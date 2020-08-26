#--------------------------------------------------------------
# Project: GBD Non-fatal CKD Estimation - Crosswalks
# Purpose: Prep microdata sources to estimate coefficients
# for the estimating equation and acr threshold crosswalks 
# outside of DisMod
# Note: Some age/sex groups returned in ubcov tabulations have a SE \
#       that is NA - this is a known issue with the svy package
# TODO: fix mapping in ubcov and re-extract nids 293226, 133397, 280192 
#--------------------------------------------------------------

# setup -------------------------------------------------------

ckd_repo<-"FILEPATH"
source(paste0(ckd_repo,"function_lib.R"))

  # DATA PREP SETTINGS ---------------------------
    # Pull in file mapping stages to crosswalks
    cv_ref_map_path<-paste0(ckd_repo,"FILEPATH/stage_cv_reference_map.csv") # must be a .csv
    cv_ref_map<-fread(cv_ref_map_path)

    # Specify filepaths to most recent ubcov all-age and 
    # age-specific tabulations 
    ubcov_age_agg_file<-paste0("FILEPATH/FILENAME.csv") # must be a .csv
    ubcov_age_spec_file<-paste0("FILEPATH/FILENAME.csv") # must be a .csv
    
    # Set output directory for prepped data and plot directory for diagnostics
    out_dir<-"FILEPATH"
    if (!dir.exists(out_dir)) dir.create(out_dir)
    plot_dir<-"FILEPATH"
    if (!dir.exists(plot_dir)) dir.create(plot_dir)
    
    # Choose whether or not to add an offset to points with a value of 0 
    apply_offset<-F
    offset_pcts<-c(0)

    # Set names of the mean and standard error columns
    mean_col="mean"
    se_col="standard_error"
    
    # Set model type : logit_diff or log_diff
    mod_type<-"logit_diff"
    
    # Set which variable to match microdata on
    match_vars<-paste("nid","ihme_loc_id","year_start","year_end","sex_id","age_start","age_end","stage",collapse=" , ")
  # ----------------------------------------------
  
  # DATA ANALYSIS SETTINGS -----------------------
    # Choose whether or not to apply trimming in MRBRT model
    apply_trimming<-T
    trim_pcts<-c(0.1)
    
    # Choose how many knots to use in MRBRT spline
    n_knots_age_midpoint<-3
    n_knots_age_sex<-3
    
    # Choose wether or not to set linear priors on tails
    tail_priors <- T
    
    # Set spline degree 
    deg_age_midpoint<-3
    deg_age_sex<-3
    
  # ----------------------------------------------
  
  # SYSTEM SETTINGS ------------------------------
    # Specify usage params
    threads <- 1
    mem <- 3
    runtime <- "00:10:00"
    shell <- "FILEPATH"
    queue <- "all.q"
    submit <- T
    
    #Scripts
    data_prep_script <- paste0(ckd_repo,"FILEPATH/prep_crosswalk_data.R")
    run_mrbrt_script_all_age <- paste0(ckd_repo,"FILEPATH/run_mrbrt_model_no_covs.R")
    run_mrbrt_script_age_specific <- paste0(ckd_repo,"FILEPATH/run_mrbrt_model.R")
  # ----------------------------------------------
    
#   -----------------------------------------------------------------------


# launch ------------------------------------------------------------------
  # Launch cleaning jobs for each stage-crosswalk combo in the mapping file 
  for (map_row in 1:nrow(cv_ref_map)){
    for(offset_pct in offset_pcts){
      job_name<- paste0('prep_crosswalk_inputs_row',map_row,"_offset_",offset_pct)
      pass_args<-list(cv_ref_map_path, ubcov_age_agg_file, ubcov_age_spec_file, out_dir, plot_dir, apply_offset, 
                      offset_pct, mean_col, se_col, map_row, mod_type, match_vars)
      construct_qsub(mem_free = mem, threads = threads, runtime = runtime, script = data_prep_script, jname = job_name, submit = submit,
                     q = queue, pass = pass_args)
    }
  }
    
  # WAIT UNTIL PREP JOBS ARE DONE TO LAUNCH
  # Launch MRBRT models for each stage-crosswalk
  for (map_row in 1:nrow(cv_ref_map)){
    for(offset_pct in offset_pcts){
      for (trim_pct in trim_pcts){
        
        apply_trimming<-ifelse(trim_pct==0,F,T)
        job_name<- paste0('MRBRT_crosswalk_mod_row_',map_row,"_",trim_pct,"_",offset_pct)
        bid<-cv_ref_map[map_row,bundle_id]
        acause<-cv_ref_map[map_row,acause]
        # Set directory where MRBRT models should be saved
        mrbrt_dir<-"FILEPATH"
        # Create directiory if it doesn't exist
        if(!dir.exists(mrbrt_dir)) dir.create(mrbrt_dir)
        # set args to pass
        if (cv_ref_map[map_row,xwalk_type]=="all_age"){
          pass_args<-list(cv_ref_map_path, out_dir, mrbrt_dir,apply_trimming, trim_pct, offset_pct, map_row,apply_offset,mod_type)
          pass_script<-run_mrbrt_script_all_age
        }
        if (cv_ref_map[map_row,xwalk_type]=="age_spec"){
          pass_args<-list(cv_ref_map_path, out_dir, mrbrt_dir,apply_trimming, trim_pct, n_knots_age_midpoint, n_knots_age_sex, 
                          tail_priors, deg_age_midpoint, deg_age_sex, offset_pct, map_row,apply_offset,mod_type)
          pass_script<-run_mrbrt_script_age_specific
        }
        # submit
        construct_qsub(mem_free = mem, threads = threads, runtime = runtime, script = pass_script, jname = job_name, submit = submit,
                       q = queue, pass = pass_args)
      } 
    }
  }
  
#   -----------------------------------------------------------------------
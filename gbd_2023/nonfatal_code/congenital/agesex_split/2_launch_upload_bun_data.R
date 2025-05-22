### upload bundle data

cong_map_dt <- cong_map_dt[bundle %ni% c(439, 2978, 2975, 2972)]
cong_bundles <- cong_map_dt[, bundle]
heart_buns <- cong_map_dt[acause %like% 'cong_heart']$bundle
decomp_step <- 'iterative'
gbd_round_id <- 7

### Clear bundle data if necessary ###
for (bun_id in heart_buns){
  print(bun_id)
  current_bundle <- get_bundle_data(bun_id, decomp_step, gbd_round_id)
  print(nrow(current_bundle))
  write.xlsx(current_bundle,
             file = paste0('FILEPATH', decomp_step, '_wipe_data/', bun_id, '_', Sys.Date(), '.xlsx'),
             sheetName = "extraction", row.names = FALSE)
  df <- data.table()
  df$seq <- current_bundle$seq
  write.xlsx(df,
             file = paste0('FILEPATH', decomp_step, '_wipe_data/', bun_id, '_empty.xlsx'),
             sheetName = "extraction", row.names = FALSE)
  upload_bundle_data(bundle_id = bun_id, decomp_step = decomp_step, filepath = paste0('FILEPATH', decomp_step, '_wipe_data/', bun_id, '_empty.xlsx'),
                     gbd_round_id = gbd_round_id)
  print('Uploaded empty bundle')
}
#####################################

launch_upload <- function(bundles, gbd_round_id, decomp_step){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  source("FILEPATH")
  '%ni%' <- Negate('%in%')
  
  measure_name <- "prevalence"
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=90G" ### CHANGE MEMORY AS NEEDED
  fthread <- "-l fthread=2" ### CHANGE THREADS AS NEEDED
  runtime_flag <- "-l h_rt=02:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q" ### CHANGE QUEUE AS NEEDED
  shell_script <- "-cwd FILEPATH -s "
  
  script <- paste0(h, "FILEPATH", "2_upload_bun_data.R") ### CHANGE THIS TO YOUR SCRIPT
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  
  
  for (bun_id in bundles){ ### CHANGE ARGUMENTS AS NEEDED
    job_name <- paste0("-N", " bundle_", bun_id, "_") ### CHANGE JOB NAME AS NEEDED
    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                 jdrive_flag, queue_flag,
                 job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, bun_id, gbd_round_id, decomp_step) ### CHANGE QSUB TO MATCH JOB NAME AS NEEDED
    
    system(job)
    print(job_name)
  }
}

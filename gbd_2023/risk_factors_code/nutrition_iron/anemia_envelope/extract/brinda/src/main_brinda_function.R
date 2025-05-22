
# source libraries --------------------------------------------------------

library(data.table)

# main function -----------------------------------------------------------

main_brinda_function <- function(winnower_dir, lu_winnower_dir = NULL, cb_file_path, submit_jobs = TRUE){
  
  df <- fread(cb_file_path)

  # step 1 - get file names for each survey ---------------------------------

  df <- get_extracted_file_names(
    input_df = df,
    winnower_dir = winnower_dir,
    lu_winnower_dir = lu_winnower_dir
  )
  
  # step 2 - assign what files will be adjusted -----------------------------
  
  df <- assign_brinda_files(input_df = df)

  if(submit_jobs){
  
    # step 3 - brinda adjust with gps files -----------------------------------
  
    gps_cb <- df[brinda_adjustment_type == 'gps']
    if(nrow(gps_cb) > 0){
      launch_brinda_job(
        codebook = gps_cb,
        adj_type = 'gps',
        cluster_memory = 25,
        cluster_threads = 5,
        cluster_time = 60,
        parallelize = TRUE,
        j_drive_flag = TRUE
      )
    }
  
    # step 4 - brinda adjust using ihme subnats -------------------------------
  
    ihme_subnat_cb <- df[brinda_adjustment_type == 'ihme_admin']
    if(nrow(ihme_subnat_cb)){
      launch_brinda_job(
        codebook = ihme_subnat_cb,
        adj_type = 'ihme_admin',
        cluster_memory = 25,
        cluster_threads = 5,
        cluster_time = 60,
        parallelize = TRUE
      )
    }
    
    # step 5 - get coordinates from google sheets and elevation adjust --------
    
    # google_df <- df[brinda_adjustment_type == 'google']
    # if(nrow(google_df) > 0){
    #   run_google_flag <- check_google_coords(google_df)
    #   print(run_google_flag)
    #   if(run_google_flag){
    #     google_df <- get_google_admin_coordinates(
    #       input_df = google_df
    #     )
    #   }
    # 
    #   launch_brinda_job(
    #     codebook = google_df,
    #     adj_type = 'google',
    #     cluster_memory = 30,
    #     cluster_threads = 5,
    #     cluster_time = 60 * 2,
    #     parallelize = TRUE,
    #     j_drive_flag = TRUE
    #   )
    # }
  }
  
  return(df)
  
}

# create a function to launch brinda jobs ---------------------------------

launch_brinda_job <- function(codebook,
                              adj_type,
                              cluster_memory,
                              cluster_threads,
                              cluster_time,
                              j_drive_flag = F,
                              parallelize = F,
                              job_dependency = NULL) {
  
  adj_dir <- file.path(getwd(), "extract/brinda/param_maps/")

  adj_cb_file_path <- file.path(adj_dir, paste0(adj_type, "_brinda_cb.csv"))
  write.csv(
    x = codebook,
    file = adj_cb_file_path,
    row.names = F
  )

  brinda_adjust_script_dir <- file.path(getwd(), "extract/brinda/adjust_scripts/")
  script_file_name <- file.path(brinda_adjust_script_dir, paste0(adj_type, ".R"))

  brinda_job <- paste0(adj_type, "_brinda_adj")
  
  array_string <- NULL
  if(parallelize){
    num_concurrent_jobs <- min( # cap memory around 1TB
      ceiling(1024 / cluster_memory),
      nrow(codebook)
    ) 
    array_string <- paste0("1-", nrow(codebook), "%", num_concurrent_jobs)
  }

  nch::submit_job(
    script = script_file_name,
    script_args = adj_cb_file_path,
    job_name = brinda_job,
    memory = cluster_memory,
    ncpus = cluster_threads,
    time = cluster_time,
    partition = 'long.q',
    archive = j_drive_flag,
    dependency = job_dependency,
    array = array_string
  )
}


# source libraries --------------------------------------------------------

library(data.table)

# source maps -------------------------------------------------------------

bundle_map <- fread(file.path(getwd(), "model_prep/param_maps/bundle_map.csv"))
output_dir <- 'FILEPATH'

if(interactive()){
  task_id <- 5
}else{
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  Sys.sleep(log(task_id))
}

# upload bundle data ------------------------------------------------------

upload_anemia_envelope_bundles <- function(input_df, bundle_map, r){
  df <- copy(input_df)
  if(bundle_map$update[r]){
    message(crayon::black("Updating bundle ID: ", bundle_map$id[r]))
    
    if(bundle_map$clear_bundle[r] && isFALSE(bundle_map$debug[r])){
      message(crayon::magenta("Dropping current bundle..."))
      clear_bundle_data(bundle_id = bundle_map$id[r])
    }
    
    i_vec <- which(
      df$var == bundle_map$var[r] &
        df$cv_pregnant == bundle_map$cv_pregnant[r]
    )
    bun_df <- df[i_vec, ]
    
    if(nrow(bun_df) > 0){
      bun_df$seq <- seq_len(nrow(bun_df))
      
      bun_df$measure_orig <- bun_df$measure
      bun_df$measure <- bundle_map$measure[r]
      
      bundle_dir <- file.path(
        output_dir,
        bundle_map$id[r]
      )
      if(!(dir.exists(bundle_dir))) dir.create(bundle_dir)
      
      file_name <- file.path(
        bundle_dir, 
        paste0(bundle_map$id[r], "bundle_data.csv")
      )
      
      fwrite(
        x = bun_df,
        file = file_name
      )
      
      if(isFALSE(bundle_map$debug[r])){
        file_name <- file.path(
          bundle_dir, 
          paste0(bundle_map$id[r], "bundle_data.xlsx")
        )
        openxlsx::write.xlsx(bun_df, file_name, sheetName = "extraction")
        
        ihme::upload_bundle_data(
          bundle_id = bundle_map$id[r],
          filepath = file_name
        )
        
        ihme::save_bundle_version(
          bundle_id = bundle_map$id[r], 
          description = '...'
        )
      }
    }
  }
}

# clear bundle data -------------------------------------------------------

clear_bundle_data <- function(bundle_id){
  bundle_data <- ihme::get_bundle_data(bundle_id)
  if(nrow(bundle_data) > 0){
    seqs <- bundle_data[, "seq"]
    # path to the extraction sheet to be created:
    path <- fs::path_expand(
      paste0(file.path(getwd(), "model_prep/param_maps/", bundle_id, "_seq.xlsx"))
    )
    openxlsx::write.xlsx(seqs, path, sheetName = "extraction")
    ihme::upload_bundle_data(bundle_id = bundle_id, filepath = path)
    # remove the extraction sheet used to clear the bundle data
    file.remove(path)
  }
}

# call funciton -----------------------------------------------------------

df <- fst::read_fst(
  path = file.path(
    output_dir,
    'full_data_set',
    'all_bundle_data.fst'
  ),
  as.data.table = TRUE
)

upload_anemia_envelope_bundles(
  input_df = df,
  bundle_map = bundle_map,
  r = task_id
)
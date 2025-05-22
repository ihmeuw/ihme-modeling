
library(data.table)

# get cluster args --------------------------------------------------------

r <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))

bundle_map <- fread(file.path(getwd(), 'model_prep/param_maps/bundle_map.csv'))

bundle_dir <- 'FILEPATH'

Sys.sleep(r * 3)

# clear bundle data -------------------------------------------------------

clear_bundle_data <- function(bundle_id){
  bundle_data <- ihme::get_bundle_data(bundle_id)
  if(nrow(bundle_data) > 0){
    seqs <- bundle_data[, "seq"]
    # path to the extraction sheet to be created:
    path <- fs::path_expand(file.path(getwd(), 'model_prep/param_maps/seq.xlsx'))
    openxlsx::write.xlsx(seqs, path, sheetName = "extraction")
    ihme::upload_bundle_data(bundle_id = bundle_id, filepath = path)
    # remove the extraction sheet used to clear the bundle data
    file.remove(path)
  }
}

# upload bundle data ------------------------------------------------------

if(bundle_map$update[r]){
  message(crayon::black("Updating bundle ID: ", bundle_map$id[r]))
  
  if(bundle_map$clear_bundle[r] && isFALSE(bundle_map$debug[r])){
    message(crayon::magenta("Dropping current bundle..."))
    clear_bundle_data(bundle_id = bundle_map$id[r])
  }
  
  input_file_name <- file.path(
    bundle_dir,
    bundle_map$id[r],
    paste0(bundle_map$id[r], "bundle_data.csv")
  )
  
  bun_df <- fread(input_file_name)
  bun_df <- bun_df[val >= 0]
  
  if(nrow(bun_df) > 0){
    
    if(isFALSE(bundle_map$debug[r])){
      file_name <- file.path(
        bundle_dir, 
        bundle_map$id[r],
        paste0(bundle_map$id[r], "bundle_data.xlsx")
      )
      openxlsx::write.xlsx(bun_df, file_name, sheetName = "extraction")
      
      ihme::upload_bundle_data(
        bundle_id = bundle_map$id[r],
        filepath = file_name
      )
      
      ihme::save_bundle_version(
        bundle_id = bundle_map$id[r]
      )
    }
  }
}


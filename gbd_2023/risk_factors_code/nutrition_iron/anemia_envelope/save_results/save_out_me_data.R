
# load in command args ----------------------------------------------------

if(interactive()) {
  adj_type <- 'brinda_new'
  sd_file_param_path <- file.path(getwd(), 'save_results/sd_files.rds')
  me_id_file_path <- file.path(getwd(), 'save_results/me_id_map.csv')
  output_dir <- 'FILEPATH'
} else {
  command_args <- commandArgs(trailingOnly = TRUE)
  adj_type <- command_args[1]
  sd_file_param_path <- command_args[2]
  me_id_file_path <- command_args[3]
  output_dir <- command_args[4]
}

me_id_map <- read.csv(me_id_file_path)

# get sd file names -------------------------------------------------------

sd_files <- readRDS(sd_file_param_path)

# define columns to iterate over ------------------------------------------

req_cols <- c('location_id', 'sex_id', 'year_id', 'age_group_id', 'draw')
  
final_me_cols <- me_id_map$me_col_names
output_sub_dirs <- me_id_map$subfolder

# clean output dirs -------------------------------------------------------

for(x in output_sub_dirs) {
  me_out_dir <- file.path(output_dir, x)
  if(dir.exists(me_out_dir)) {
    unlink(x = me_out_dir, recursive = TRUE)
  }
  dir.create(path = me_out_dir, mode = '0775')
}

# resave ME data ----------------------------------------------------------

for(f in sd_files) {
  final_df <- fst::read.fst(path = f, as.data.table = TRUE) 
  
  current_loc_id <- unique(final_df$location_id)
  
  for (i in seq_len(length(final_me_cols))) {
    keep_cols <- c(req_cols, final_me_cols[i])
    me_out_dir <- file.path(output_dir, output_sub_dirs[i])
    
    final_me_df <- final_df |>
      dplyr::select(tidyselect::all_of(keep_cols)) |>
      data.table::setDT()
    
    if(!(final_me_cols[i] %in% c('mean_hb', 'stdev'))) {
      i_vec <- which(final_me_df[[final_me_cols[i]]] < 0)
      final_me_df[[final_me_cols[i]]][i_vec] <- 0
      
      i_vec <- which(final_me_df[[final_me_cols[i]]] > 1)
      final_me_df[[final_me_cols[i]]][i_vec] <- 1
    }
    
    out_file_name <- file.path(
      me_out_dir, paste0('for_upload_', current_loc_id, '.csv')
    )
    data.table::fwrite(
      x = final_me_df |>
        data.table::dcast(
          location_id + sex_id + year_id + age_group_id ~ draw, 
          value.var = final_me_cols[i] 
        ),
      file = out_file_name
    )
  }
}

# Title: Utility functions for age/sex splitting
# Purpose: Source to run cascading splines models

# Generate subdirectories (run_{run_id}/data/splines)
setup_split_dir <- function(run_id) {
  
  # Create subdirectories under run_{run_id}/data/splines
  subdirs <- c("inputs", "outputs")
  for (subdir in subdirs) {
    subdir_path <- paste0(run_dir, "data/splines/", subdir, "/")
    if (!dir.exists(subdir_path)) {
      dir.create(subdir_path)
    }
  }
  cat("Created run data subdirectories under run_{run_id}/data/: ", paste0(subdirs, collapse = ","), "\n")
}

# Source configurations
source_split_config <- function(config_split_path, split_model_index_id) {
  
  # Read in from path indicated in the laucnher
  config <- openxlsx::read.xlsx(config_split_path) %>% as.data.table()

  # Filter to ID indicated in the launcher
  filtered_config <- filter(config, model_index_id == split_model_index_id)
  
  # Assuming filtered_config contains only one row, convert it to a list
  if (nrow(filtered_config) == 1) {
    config_list <- as.list(filtered_config)
    names(config_list) <- names(filtered_config)
  } else {
    warning("Filtered configuration does not uniquely identify a single row.")
  }
  
  return(config_list)

}

# Import splines inputs and assess missing values
import_splines_inputs <- function(splines_input_path, splines_prediction_frame_path) {
  
  # Load input data
  dt <- fread(as.character(splines_input_path))
  
  # Check for missing values
  na_counts <- sapply(dt, function(x) sum(is.na(x)))
  total_na <- sum(na_counts)
  
  if (total_na == 0) {
    cat("Cascading splines input data has no missing values. Proceeding...\n")
  } else {
    missing_info <- na_counts[na_counts > 0]
    cat(sprintf("Cascading splines input data has %d missing values in the following columns: %s\n",
                total_na, toString(names(missing_info))))
  }
  
  # Load prediction frame
  pred_frame <- fread(as.character(splines_prediction_frame_path))
  
  # Check for missing values
  na_counts <- sapply(pred_frame, function(x) sum(is.na(x)))
  total_na <- sum(na_counts)
  
  if (total_na == 0) {
    cat("Prediction frame for cascading splines has no missing values. Proceeding...\n")
  } else {
    missing_info <- na_counts[na_counts > 0]
    cat(sprintf("Prediction frame for cascading splines has %d missing values in the following columns: %s\n",
                total_na, toString(names(missing_info))))
  }
  
  return(list(dt = dt, pred_frame = pred_frame))
  
}

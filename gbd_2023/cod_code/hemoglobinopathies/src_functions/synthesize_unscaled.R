#' Purpose: Combines the paths to interpolated files that do not have a 
#' corresponding data rich file (based on location ID) with the paths to all 
#' data rich files. This results in a nested list structure where each element
#' corresponds to a combination of cause and sex, containing all the relevant 
#' file paths. These file paths are then used to output CSV files for saving.
#' 
#' Note: The "result" list contains each of the given cause IDs; the length of 
#' this list equals the number of given cause IDs. For each cause ID, there is 
#' a sublist with a length equal to the number of given sex IDs.

synthesize_unscaled <- function(cause_id, sex_id, out_dir) {
  # define function to extract location ID value from a given path
  extract_location_id <- function(path) {
    stringr::str_extract(basename(path), "\\d+")
  }

  names(cause_id) <- cause_id
  names(sex_id) <- sex_id
  
  # create a nested list containing all relevant file paths - iterate over cause and sex
  result <- lapply(cause_id, function(cause) {
    lapply(sex_id, function(sex) {
      # create a vector of file paths for each source directory
      dir_interp <- file.path(out_dir, "interp_files", cause, sex)
      dir_dr <- file.path(out_dir, "dr_files", cause, sex)   
      all_interp_files <- fs::dir_ls(dir_interp, glob = "*.qs")
      all_dr_files <- fs::dir_ls(dir_dr, glob = "*.qs")
      
      # assign each file path value a name that matches its location ID
      names(all_interp_files) <- extract_location_id(all_interp_files)
      names(all_dr_files) <- extract_location_id(all_dr_files)
      
      # exclude interpolated files for location IDs with corresponding DR files  
      c(
        all_interp_files[!(names(all_interp_files) %in% names(all_dr_files))], 
        all_dr_files
      )
    })
  }) 
  
  # copy files to a save directory and convert QS to CSV
  lapply(names(result), function(cause) {
    lapply(names(result[[cause]]), function(sex) {
      lapply(names(result[[cause]][[sex]]), function(location_id) {
        paths <- result[[cause]][[sex]][[location_id]]
        lapply(paths, function(path) {
          dat <- qs::qread(path)
          # construct the file path with values of cause, sex, and location_id
          csv_path <- file.path(
            out_dir, 
            sprintf("synth_unscaled_csv/%s/%s/unscaled_hemog_%s.csv", cause, sex, location_id)
          )
          fs::dir_create(dirname(csv_path), recursive = TRUE)  # checks whether directory already exists by default
          data.table::fwrite(dat, csv_path, row.names = FALSE)
        })
      })
    })
  })
}
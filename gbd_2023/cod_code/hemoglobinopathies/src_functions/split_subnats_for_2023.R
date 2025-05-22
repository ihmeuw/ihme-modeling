# Define function to check and update files based on given location IDs

split_subnats_for_2023 <- function(data_dir) {
  # define full path for old location based on file name pattern
  files <- list.files(path = data_dir, full.names = TRUE)
  pattern <- "_hemog_44858\\.qs$"
  old_path <- files[grepl(pattern, files)]
  
  # read in values for old location and map to new locations
  draws_all <- qs::qread(old_path) |>
    hemo::impute(
      mapping = data.table::data.table(
        location_id = c(
          "Southern Nations, Nationalities, and Peoples" = 44858L
        ),
        new_location_id = c(
          "Sidama" = 60908L,
          "South West" = 94364L,
          "Southern Nations, Nationalities, and Peoples" = 95069L
        )
      ),
      name = "location_id",
      replace = TRUE)
  new_ids <- c(60908L, 94364L, 95069L)
  
  # subset draws and save in a separate file for each subnat location ID
  new_paths <- c()
  for (new_id in new_ids) {
    draws_new <- draws_all[draws_all$location_id == new_id,]
    new_path <- sub("44858\\.qs$", glue::glue("{new_id}.qs"), old_path)
    qs::qsave(draws_new, new_path)
    
    if (fs::file_exists(new_path)) {
      message(glue::glue("New file based on {old_path} output as {new_path}"))
    } else {
      message(glue::glue("File {old_path} does not exist - no new file created"))
    }
    new_paths <- c(new_paths, new_path)
  }
  return(new_paths)
}
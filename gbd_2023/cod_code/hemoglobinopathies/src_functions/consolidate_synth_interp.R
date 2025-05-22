# Consolidate synth files for DR locs and interp files for non-DR locs ----

consolidate_synth_interp <- function(cause_id, sex_id, source_dir) {
  for (cause in cause_id) {
    for (sex in sex_id) {
      # define source and target directories
      dir_interp <- file.path(source_dir, "interp_files", cause, sex)
      dir_dr <- file.path(source_dir, "dr_files", cause, sex)
      dir_synth <- file.path(source_dir, "synth_files", cause, sex)
      fs::dir_create(dir_synth, mode = "775")
      message("Synthesizing files from ", dir_interp, " and ", dir_dr)
      
      # list files in directories
      all_interp_files <- fs::dir_ls(dir_interp, glob = "*.qs")
      all_dr_files <- fs::dir_ls(dir_dr, glob = "*.qs")
      
      # assert that the location IDs in dir_dr match the expected dr_locs
      output_dr_locs <- stringr::str_extract(basename(all_dr_files), "\\d+")
      dr_locs <- qs::qread(file.path(source_dir, "synth_dr_locs.qs"))$location_id
      assertthat::assert_that(
        setequal(output_dr_locs, dr_locs), 
        msg = "The location IDs in the data rich files do not match the IDs from CoD"
        )
      
      # define function to construct an informative new file name
      construct_new_filename <- function(file_path, prefix) {
        location_id <- stringr::str_extract(basename(file_path), "\\d+")
        new_path <- file.path(dir_synth, glue::glue("{prefix}{location_id}.qs"))
      }
      
      for (file_path in all_interp_files) {
        dat <- qs::qread(file_path)
        new_path <- construct_new_filename(file_path, "synth_hemog_")
        qs::qsave(dat, new_path)
      }
      
      for (file_path in all_dr_files) {
        dat <- qs::qread(file_path)
        new_path <- construct_new_filename(file_path, "synth_hemog_")
        qs::qsave(dat, new_path)
      }
    }
  }
}
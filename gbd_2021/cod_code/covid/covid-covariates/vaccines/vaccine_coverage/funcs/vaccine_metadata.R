
vaccine_metadata.get_county_version <- function(production_version) {
  # Increment the VV in a directory path terminating in DATE.VV
  # Used to produce a county version simultaneously with a production version
  tmp <- unlist(strsplit(production_version, '/'))
  current_version <- tmp[length(tmp)]
  
  tmp <- unlist(strsplit(current_version, '[.]'))
  v <- as.numeric(tmp[2]) + 1
  z <- ifelse(v < 10, '0', '')
  county_version <- glue('{tmp[1]}.{z}{v}')
  county_dir <- file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, county_version)
  return(county_dir)
}
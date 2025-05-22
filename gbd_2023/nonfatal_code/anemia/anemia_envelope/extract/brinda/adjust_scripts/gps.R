
# source libraries --------------------------------------------------------

library(data.table)
library(raster)
library(haven)
library(stringr)

# source elevation adjustment functions -----------------------------------

source(file.path(getwd(), "extract/brinda/adjust_scripts/apply_elevation_adjustment.R"))
source(file.path(getwd(), "extract/brinda/adjust_scripts/assign_anemia_severity.R"))

# load in map file and admin elevation location ---------------------------

task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
command_args <- commandArgs(trailingOnly = TRUE)
gps_cb <- fread(command_args[1])

Sys.sleep(task_id)

elevation_map <- raster("FILEPATH/elevation_mean_synoptic.tif")

# helper functions --------------------------------------------------------

get_gps_file <- function(file_name){
  file_vec <- unlist(str_split(string = file_name, pattern = "/"))
  file_vec <- file_vec[-length(file_vec)]
  survey_dir <- paste(file_vec, collapse = "/")
  
  survey_files <- list.files(
    path = survey_dir,
    full.names = TRUE,
    pattern = "_gps_.*dta",
    ignore.case = TRUE
  )
  
  df <- setDT(read_dta(survey_files[1]))
  df$cluster_altitude <- NA_real_
  df$cluster_altitude_unit <- "m"
  
  return(df)
}

assign_elevation <- function(input_df){
  df <- copy(input_df)
  
  for(r in seq_len(nrow(df))){
    coord_df <- data.frame(x = df$longnum[r], y = df$latnum[r])
    elevation <- as.numeric(raster::extract(elevation_map, coord_df))
    df$cluster_altitude[r] <- elevation
  }
  
  keep_cols <- c("dhsclust", "cluster_altitude", "cluster_altitude_unit")
  df <- df[, keep_cols, with = FALSE]
  
  return(df)
}

# functions to assign elevation and brinda adjustment to each cluster --------

main_gps_function <- function(codebook, row_num){
  gps_df <- get_gps_file(codebook$file_path[row_num])
  gps_df <- assign_elevation(input_df = gps_df)
  
  df <- setDT(read_dta(codebook$extracted_file_name[row_num]))
  df <- remove_elevation_columns(df)
  df <- merge.data.table(
    x = df,
    y = gps_df,
    by.x = "psu",
    by.y = "dhsclust",
    all.x = TRUE
  )
  
  df <- apply_elevation_adjustment(input_df = df)
  
  write_dta(
    data = df,
    path = codebook$extracted_file_name[row_num]
  )
}

remove_elevation_columns <- function(input_df) {
  df <- copy(input_df) 
  
  col_patterns_to_drop <- c(
    'cluster_altitude', 'brinda', 'who'
  )
  
  for(col_substr in col_patterns_to_drop) {
    cluster_altitude_cols <- grep(
      pattern = col_substr,
      x = colnames(df),
      value = TRUE
    )
    for(c in cluster_altitude_cols) {
      df[[c]] <- NULL
    }
  }
  
  return(df)
}

# call main function ------------------------------------------------------

main_gps_function(
  codebook = gps_cb,
  row_num = task_id
)

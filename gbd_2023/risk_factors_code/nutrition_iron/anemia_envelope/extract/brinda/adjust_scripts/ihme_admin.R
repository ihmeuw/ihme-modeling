
# source libraries --------------------------------------------------------

library(data.table)
library(haven)
library(stringr)

# source elevation adjustment functions -----------------------------------

source(file.path(getwd(), "extract/brinda/adjust_scripts/apply_elevation_adjustment.R"))
source(file.path(getwd(), "extract/brinda/adjust_scripts/assign_anemia_severity.R"))

# load in map file and admin elevation location ---------------------------

task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
command_args <- commandArgs(trailingOnly = TRUE)
ihme_admin_cb <- fread(command_args[1])

# assign elevation --------------------------------------------------------

get_subnat_elevation <- function(year_id){
  ihme_subnat_dir <- "FILEPATH"
  elevation_csv_file_path <- file.path(
    ihme_subnat_dir,
    paste0(
      "ihme_subnat_elevation_means_",
      year_id,
      ".csv"
    )
  )
  df <- fread(elevation_csv_file_path)
  
  keep_cols <- c("ihme_lc_id", "weighted_mean_elevation")
  df <- df[, keep_cols, with = FALSE]
  setnames(
    df,
    "weighted_mean_elevation",
    "cluster_altitude"
  )
  df$cluster_altitude_unit <- "m"
  
  df$ihme_lc_id <- str_remove(df$ihme_lc_id, "\r")
  df$ihme_lc_id <- str_remove(df$ihme_lc_id, "\n")
  
  return(df)
}

get_admin_column <- function(vec){
  admin_levels <- 3:1
  for(i in admin_levels){
    admin_col <- paste("admin", i, "id", sep = "_")
    if(admin_col %in% vec){
      return(admin_col)
    }
  }
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

# main google function ----------------------------------------------------

main_ihme_admin_function <- function(codebook, row_num){
  
  df <- setDT(read_dta(codebook$extracted_file_name[row_num])) |>
    remove_elevation_columns()
  
  elevation_df <- get_subnat_elevation(codebook$year_end[row_num])
  admin_col_name <- get_admin_column(colnames(df))
  
  df[[admin_col_name]] <- str_remove(df[[admin_col_name]], "\r")
  df[[admin_col_name]] <- str_remove(df[[admin_col_name]], "\n")
  
  df <- merge.data.table(
    x = df,
    y = elevation_df,
    by.x = admin_col_name,
    by.y = "ihme_lc_id",
    all.x = TRUE
  )

  df <- apply_elevation_adjustment(input_df = df)

  write_dta(
    data = df,
    path = codebook$extracted_file_name[row_num]
  )
}

# call main function ------------------------------------------------------

main_ihme_admin_function(
  codebook = ihme_admin_cb,
  row_num = task_id
)

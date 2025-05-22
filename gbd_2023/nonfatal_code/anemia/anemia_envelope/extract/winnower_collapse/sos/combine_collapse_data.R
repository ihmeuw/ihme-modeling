# load in command line args -----------------------------------------------

if(interactive()) {
  
} else {
  cli_args <- commandArgs(trailingOnly = TRUE)
  map_file_name <- cli_args[1]
  topic_name <- cli_args[2]
  config_file_name <- cli_args[3]
  input_dir <- cli_args[4]
  
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
}

param_file_map <- read.csv(map_file_name)
config <- read.csv(config_file_name)

topic_row <- match(topic_name, config$topic)

input_dir <- file.path(
  input_dir,
  topic_name
)

output_dir <- config$output.root[topic_row]

# get all files that were collapsed ---------------------------------------

collapse_file_list <- list.files(
  path = input_dir,
  full.names = TRUE
)

# combine all files -------------------------------------------------------

final_dat <- lapply(collapse_file_list, \(f) {
  fst::read.fst(f, as.data.table = TRUE)
}) |> data.table::rbindlist(use.names = TRUE, fill = TRUE)

# write out data ----------------------------------------------------------

output_file_name <- file.path(
  output_dir,
  paste0(
    'collapsed', '_',
    topic_name, '_',
    stringr::str_replace_all(Sys.Date(), pattern = '-', replacement = '_'),
    '.csv'
  )
)

data.table::fwrite(
  x = final_dat,
  file = output_file_name
)

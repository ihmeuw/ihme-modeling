###### Wrapper function for age sex graphing.
# This is to allow Jobmon to continue on modeling without having to wait for
# age-sex graphs, which can take quite a while to generate.
####

rm(list=ls())

library(argparse)
library(data.table)
library(mortdb)
library(mortcore)


if(interactive()){
  version_id <-
  gbd_year <-

}else{
  parser <- ArgumentParser()
  parser$add_argument('--version_id', type='integer', required=TRUE,
                      help='The version_id for this run of age-sex splitting')
  parser$add_argument('--gbd_year', type='integer', required=TRUE,
                      help="Which round of GBD we're running")
  parser$add_argument('--code_dir', type='character', required=TRUE,
                      help='Directory where age-sex code is cloned')
  parser$add_argument('--start_year', type='character', required=TRUE,
                      help='Start year')

  args <- parser$parse_args()
  list2env(args, .GlobalEnv)
}

# get round id
gbd_round_id <- get_gbd_round(gbd_year = gbd_year)
loc_map <- get_locations(
  gbd_type = "ap_old",
  gbd_year = gbd_year
)

## Submit the graphing jobs and exit

# Shell arguments
path_to_r_shell <- "FILEPATH"
path_to_r_image <- "FILEPATH"

path_to_python_shell <- paste0("FILEPATH")

# prep graphing data
qsub(
  jobname = paste0('agesex_', version_id, '_prep_graphing'),
  code = paste0("FILEPATH"),
  pass_argparse = list(version_id = version_id,
                       gbd_year = gbd_year),
  cores = 5,
  mem = 10,
  wallclock = "02:00:00",
  submit = T,
  shell = path_to_r_shell,
  pass_shell = list(i = path_to_r_image)
)

# graph all locations
job_map <- CJ(
  ihme_loc_id = loc_map$ihme_loc_id,
  version_id = version_id,
  gbd_year = gbd_year,
  start_year = c(start_year, 2010)
)

write.csv(job_map, paste0("FILEPATH"))

array_qsub(
  paste0('agesex_', version_id, '_graphing'),
  code = paste0("FILEPATH"),
  hold = paste0('agesex_', version_id, '_prep_graphing'),
  cores = 5,
  mem = 5,
  wallclock = "00:30:00",
  submit = T,
  num_tasks = nrow(job_map),
  shell = path_to_r_shell,
  pass_shell = list(i = path_to_r_image)
)

# append all graphs
version_id <- as.character(version_id)
gbd_year <- as.character(gbd_year)

Sys.sleep(60*30)

qsub(
  jobname = paste0('agesex_', version_id, '_appending'),
  code = paste0("FILEPATH"),
  pass = list(version_id,
              gbd_year,
              gbd_round_id),
  log = T,
  submit = T,
  shell = path_to_python_shell,
  mem = 10,
  cores = 5,
  wallclock = "00:30:00"
)

qsub(
  jobname = paste0('agesex_', version_id, '_appending'),
  code = paste0("FILEPATH"),
  pass = list(version_id,
              gbd_year,
              gbd_round_id),
  log = T,
  submit = T,
  shell = path_to_python_shell,
  mem = 10,
  cores = 5,
  wallclock = "00:30:00"
)

quit(status=0)

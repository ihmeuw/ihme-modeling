# create parameter draws for ebola
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-"FILEPATH"
data_root <- "FILEPATH"
# Toggle (Prod Arg Parsing VS. Interactive Dev) Common FILEPATH IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- "FILEPATH"
  draws_dir <- "FILEPATH"
  interms_dir <- "FILEPATH"
  logs_dir <- "FILEPATH"
}

# Source relevant libraries
library(data.table)
library(stringr)
library(argparse)
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
my_shell <- "FILEPATH"
source("FILEPATH")
source("FILEPATH/ntd_ebola/child_scripts/utils.R"))
source("FILEPATH")

#gen_rundir(data_root = data_root, acause = 'ntd_ebola', message = 'for DATE handoff, data as reported DATE (reported DATE)')
run_file <- fread("FILEPATH")
run_path <- run_file[nrow(run_file), run_folder_path]

params_dir <- "FILEPATH"
draws_dir <- "FILEPATH"
interms_dir <- "FILEPATH"
logs_dir <- "FILEPATH"

# seed
set.seed(42) 
gbd_round_id <- 7

data_dir   <- "FILEPATH"
epi_demographics <- get_demographics('ADDRESS', gbd_round_id = gbd_round_id) 

#[ ======================= MAIN EXECUTION ======================= ###

#' [Create meids and death folders]

ifelse(!dir.exists("FILEPATH")), dir.create("FILEPATH")), FALSE)
ifelse(!dir.exists("FILEPATH")), dir.create("FILEPATH")), FALSE)
ifelse(!dir.exists("FILEPATH")), dir.create("FILEPATH")), FALSE)

####################################################################
#' [Create Ebola parameters and their draws if absent and save them]
####################################################################

# -----------------------------------------------------------------------
#' [Validate acute draws]

ifelse(!dir.exists("FILEPATH")), dir.create("FILEPATH")), FALSE)

acute_duration_present <- list.files("FILEPATH"), pattern = "FILEPATH")
  
  if(length(acute_duration_present) == 0){ 
    make_acute_draws(scalar_file = "FILEPATH"), draws_dir = draws_dir)
  }

# -----------------------------------------------------------------------
#' [Validate chronic draws] #bootstrap acorss parameter values, not predicted values

ifelse(!dir.exists("FILEPATH")), dir.create("FILEPATH")), FALSE)
chronic_duration_present <- list.files("FILEPATH"), pattern = "FILEPATH")

if(!("FILEPATH" %in% chronic_duration_present)){  #if it is not then make it
  make_chronic_draws(scalar_file = "FILEPATH"), draws_dir) 
}

# -----------------------------------------------------------------------
#' [Validate underreporting draws]

ifelse(!dir.exists("FILEPATH")), dir.create("FILEPATH")), FALSE)

underreporting_cases_present <- list.files("FILEPATH"), pattern = "FILEPATH")

if(length(underreporting_cases_present) == 0){
  make_underreport_draws(scalar_file = "FILEPATH"), draws_dir = draws_dir) 
}

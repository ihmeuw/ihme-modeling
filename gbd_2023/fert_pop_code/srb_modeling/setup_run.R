################################################################################
##                                                                            ##
## Demographics SRB Modeling Setup                                            ##
##                                                                            ##
## Steps:                                                                     ##
##   A. Read in and check configuration files                                 ##
##   B. Create new version                                                    ##
##   C. Create additional inputs                                              ##
##   D. Output detailed config for later steps                                ##
##   E. Qsub the submission script `submit_run.R`                             ##
##                                                                            ##
################################################################################

rm(list = ls())
library(data.table)
library(demInternal)
library(mortcore)
library(mortdb)

## A. Read in and check configuration files ------------------------------------

# Define the repo directory here when debugging and running this step interactively
username <- "USERNAME"
repo_dir_default <- "FILEPATH"

parser <- argparse::ArgumentParser()

parser$add_argument(
  "--config_dir",
  type = "character",
  required= !interactive(),
  default = fs::path(repo_dir_default, "configs"),
  help = "path to directory with `srb.yml` config file"
)

parser$add_argument(
  "--configuration_name_all",
  type = "character",
  required = !interactive(),
  default = "default",
  help = "configuration from `srb.yml` to use for this process run"
)

args <- parser$parse_args()
list2env(args, .GlobalEnv)

# Assertions for command line arguments
assertthat::assert_that(fs::dir_exists(config_dir))

# Load all demographics config file

config_all <- config::get(
  file = fs::path(config_dir, "srb.yml"),
  config = configuration_name_all,
  use_parent = FALSE
)
list2env(config_all, .GlobalEnv)
if(run_comment == "Informative run comment")
  stop("Please use a more descriptive run_comment")

## B. Create new version -------------------------------------------------------

if(test){
  message("Test run")
  version_id <- "Test run id"
}else{
  if(new_version){
    version_id <- mortdb::gen_new_version(
      model_name = "birth sex ratio",
      model_type = "estimate",
      comment = run_comment,
      gbd_year = gbd_year
    )
  }else{
    version_id <- "Run id"
  }
}
print(version_id)

# Create directories
code_dir <- "FILEPATH"

main_dir <- fs::path(base_dir, version_id)
input_dir <- "FILEPATH"
output_dir <- "FILEPATH"
graphs_dir <- "FILEPATH"
gpr_dir <- fs::path(input_dir, "FILEPATH")
draws_dir <- fs::path(output_dir, "FILEPATH")
shared_dir <- "FILEPATH"

for(dir in c(main_dir, gpr_dir, draws_dir, graphs_dir)){
  if(!fs::dir_exists(dir)){
    fs::dir_create(dir, recurse = TRUE, mode = "775")
  }
}

# Clone directory
workflow_args <- format(Sys.time(), "%Y%m%d%H%M")
clone_dir <- paste0("FILEPATH/SRB_Pipeline_", workflow_args)

if (fs::dir_exists(clone_dir)) {
  workflow_args <- fread(fs::path(clone_dir, "resume_file.csv")) 
  workflow_args <- workflow_args$workflow_args
  
} else {
  fs::dir_create(clone_dir)
  
  demInternal::git_clone(clone_dir, stash_repos = c("BRANCH TO COPY" = branch))
  # Open permissions so shell scripts can access py scripts
  cloned_files <- list.files(clone_dir, recursive = TRUE)
  Sys.chmod(cloned_files, mode = "775", use_umask = TRUE)
  
}

## C. Create additional inputs --------------------------

gbd_round_id <- mortdb::get_gbd_round(gbd_year)
data_version_id <- mortdb::get_proc_version(
  "birth sex ratio",
  "data",
  gbd_year = gbd_year
)
prepped_dir <- fs::path("FILEPATH", data_version_id)

# default beta value
beta_val <- ifelse(run_beta, 10, NA_real_)

# compare_version_id:
compare_version_id <- ifelse(add_compare, 224, NA_real_)

# Create inputs used by multiple scripts
list(loc_map = "mortality", ap_old_map = "ap_old") |> 
  purrr::map(
    ~mortdb::get_locations(
      gbd_type = ., 
      level = "estimate",
      gbd_year = gbd_year
    )
  ) |> list2env(envir = environment())

parent_ids <- loc_map[
  level == 3 | (level == 4 & grepl("CHN", ihme_loc_id)),
  ihme_loc_id
]

readr::write_csv(
  loc_map,
  fs::path(input_dir, "loc_map.csv"),
  na = ""
)
readr::write_csv(
  ap_old_map,
  fs::path(input_dir, "ap_old_map.csv"),
  na = ""
)

## D. Output detailed config for later steps -----------------------------------

# Save original config files
if(file.exists(fs::path(main_dir, "srb.yml"))){
  file.remove(fs::path(main_dir, "srb.yml"))
}

fs::file_copy(
  path = fs::path(config_dir, "srb.yml"),
  new_path = fs::path(main_dir, "srb.yml")
)

# Merge together config objects

config_detailed <- data.table::copy(config_all)

# Add on other important created variables

add_config_vars <- c(
  "main_dir",
  "code_dir",
  "config_dir",
  "configuration_name_all",
  "clone_dir",
  "input_dir",
  "output_dir",
  "graphs_dir",
  "gpr_dir",
  "draws_dir",
  "shared_dir",
  "prepped_dir",
  "version_id",
  "beta_val",
  "compare_version_id",
  "username",
  "parent_ids",
  "data_version_id",
  "gbd_round_id"
)

for (var in add_config_vars) {
  config_detailed[[var]] <- get(var)
}

# Save detailed config file

config_detailed <- list(default = config_detailed)

yaml::write_yaml(
  config_detailed,
  fs::path(main_dir, "srb_detailed.yml")
)

## E. Qsub the submission script `submit_run.R` ----------------------------

run_id_sources <- version_id
mortcore::qsub(
  jobname = paste0("submit_sources_", run_id_sources),
  shell = shell_r,
  pass_shell = image_r,
  code = fs::path(code_dir, "submit_run.R"),
  proj = submission_project_name,
  queue = queue,
  pass_argparse = list(main_dir = main_dir),
  wallclock = "02:00:00",
  mem = 1,
  cores = 1,
  archive = FALSE,
  submit = TRUE
)

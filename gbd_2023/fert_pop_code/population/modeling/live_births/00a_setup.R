################################################################################
## Description: Create new live births version and all output directories
##              needed. Submits `00b_submit.R`.
################################################################################

library(data.table)
library(argparse)
library(mortdb)
library(mortcore)

rm(list = ls())
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH")


# Run settings ------------------------------------------------------------

# load settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--test", type = "character",
                    help = 'Whether this run is just a test (if so will create a fake run id)')
parser$add_argument("--best", type = "character",
                    help = 'Whether to immediately best the new run after successful completion')
parser$add_argument("--comment", type = "character",
                    help = "Run id comment")
parser$add_argument("--gbd_year", type = "integer",
                    help = "The gbd year to pull inputs with and create a new run id for")
parser$add_argument("--year_start", type = "integer",
                    help = "Start year for estimation")
parser$add_argument("--year_end", type = "integer",
                    help = "Final year for estimation")

args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$test <- "F"
  args$best <- "F"
  args$comment <- "GBD 2020 Baseline loop 1"
  args$gbd_year <- 2023
  args$year_start <- 1950
  args$year_end <- 2024
}
args$test <- as.logical(args$test)
args$best <- as.logical(args$best)
list2env(args, .GlobalEnv); rm(args)
workflow_args <- format(Sys.time(), "%Y%m%d%H%M")


# Create new estimate version ---------------------------------------------

hostname <- ifelse(!test, "modeling-mortality-db", "mortality-db-t01")
if (test) {
  live_births_vid <- "99999"
} else {
  live_births_vid <- gen_new_version(model_name = "birth", model_type = "estimate",
                                     comment = comment, gbd_year = gbd_year,
                                     hostname = hostname)
}

# If resuming, update workflow_args. Otherwise create clones ---------------

clone_dir <- paste0("FILEPATH", 
                    workflow_args)

if (fs::dir_exists(clone_dir)) {
  
  workflow_args <- fread(fs::path(clone_dir, "resume_file.csv"))
  workflow_args <- workflow_args$workflow_args
  
} else {
  
  fs::dir_create(clone_dir)
  
  demInternal::git_clone(clone_dir, stash_repos = c("fp/live_births" = "master"))
  
  fs::file_copy(paste0("FILEPATH", Sys.getenv("USER"), 
                       "/live_births/00a_setup.R"), 
                paste0(clone_dir, "/livebirths_pipeline_run_all.R"))
  
  # Open permissions so shell scripts can access py scripts 
  cloned_files <- list.files(clone_dir, recursive = TRUE)
  Sys.chmod(cloned_files, mode = "0777", use_umask = TRUE)
  
}

code_dir <- paste0(clone_dir, "/live_births/")

# Make settings list ------------------------------------------------------

settings <- list(
  live_births_vid = live_births_vid,
  gbd_year = gbd_year,
  year_start = year_start,
  year_end = year_end,
  comment = comment,

  test = test,
  best = best,
  hostname = hostname,

  queue = "all",
  submission_project_name = "proj_mortenvelope",
  default_resubmission_attempts = 3,
  r_shell_singularity_version = "4221",

  # best version of all inputs needed
  asfr_vid = mortdb::get_proc_version(model_name = "asfr", model_type = "estimate", run_id = "recent", gbd_year = gbd_year),
  srb_vid = mortdb::get_proc_version(model_name = "birth sex ratio", model_type = "estimate", run_id = "recent", gbd_year = gbd_year),
  pop_vid = mortdb::get_proc_version(model_name = "population single year", model_type = "estimate", run_id = "best", gbd_year = gbd_year),

  years = year_start:year_end,
  draws = 0:999,
  reporting_hierarchies = c(40, 3, 5, 11, 20, 26, 28, 31, 32, 46, 13, 130, 131, 132, 136, 138, 143, 147),
  modeling_hierarchy = "mortality",
  extra_reporting_age_groups = c(169, 162, 215, 218),

  # parent output directories
  output_dir = paste0("FILEPATH", ifelse(test, "test/", ""), live_births_vid, "/"),
  sandbox_dir = paste0("FILEPATH")
)

# load into the environment
list2env(settings, envir = environment())


# Parent-child relationships ----------------------------------------------

# generate parent to child relationships
if (!test) {
  mortdb::gen_parent_child(parent_runs = list("population single year estimate" = pop_vid,
                                              "asfr estimate" = asfr_vid,
                                              "birth sex ratio estimate" = srb_vid),
                           child_process = "birth estimate", child_id = live_births_vid,
                           hostname = hostname)
}


# Create directories ------------------------------------------------------

# delete old test version if running a new one
if (test) {
  unlink(output_dir, recursive = T)
  unlink(sandbox_dir, recursive = T)
}

dir.create(paste0(output_dir, "task_map"), recursive = T)
dir.create(paste0(output_dir, "inputs"), recursive = T)
dir.create(paste0(output_dir, "outputs/unraked_by_loc/"), recursive = T)
dir.create(paste0(output_dir, "outputs/raked_aggregated_by_draw/"), recursive = T)
dir.create(paste0(output_dir, "outputs/raked_aggregated_by_loc/"), recursive = T)
dir.create(paste0(output_dir, "upload"), recursive = T)
dir.create(paste0(output_dir, "diagnostics"), recursive = T)
dir.create(sandbox_dir, recursive = T)

# save settings
settings_dir <- paste(output_dir, "/run_settings.csv", sep = "/")
save(settings, file = settings_dir)


# Location hierarchies ----------------------------------------------------

loc_hierarchies <- c(modeling_hierarchy, reporting_hierarchies)
all_hierarchies <- lapply(loc_hierarchies, function(loc_type) {
  locs <- data.table(mortdb::get_locations(gbd_type = ifelse(grepl("[0-9]", loc_type), as.numeric(loc_type), loc_type), gbd_year = gbd_year, level = "all"))
  locs[, location_set_name := loc_type]
  return(locs)
})
all_hierarchies <- data.table::rbindlist(all_hierarchies, use.names = T)

# rename modeling_hierarchy to "live_births"
all_hierarchies[location_set_name == modeling_hierarchy, location_set_name := "live_births"]

readr::write_csv(all_hierarchies, file = paste0(output_dir, '/inputs/all_reporting_hierarchies.csv'))


# Submit submission job ---------------------------------------------------

path_to_shell <- 'FILEPATH'
path_to_image <- paste0("FILEPATH")

mortcore::qsub(jobname = paste0("submit_live_births_", live_births_vid),
               cores = 1, mem = 1, wallclock = "05:00:00", archive_node = F,
               queue = queue, proj = submission_project_name,
               shell = path_to_shell, pass_shell = list(i = path_to_image),
               code = paste0(code_dir, "00b_submit.R"),
               pass_argparse = list(live_births_vid = live_births_vid,
                                    test = test,
                                    check_misformatted_files = TRUE, 
                                    code_dir = code_dir),
               submit = T)

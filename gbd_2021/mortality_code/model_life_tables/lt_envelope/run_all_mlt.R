################################################################################
##                                                                            ##
## Model Life Tables (MLT) Run All                                            ##
##                                                                            ##
################################################################################

library(argparse)
library(data.table)
library(jobmonr)
library(mortdb)

## Read in arguments and inputs ------------------------------------------------

# Load mort_pipeline arguments
if (interactive()) {

} else {
  
  parser <- argparse::ArgumentParser()
  
  parser$add_argument(
    "--version_id", type = "integer", required = TRUE,
    help = "MLT version id"
  )
  parser$add_argument(
    "--mlt_death_number_estimate", type = "integer", required = TRUE,
    help = "Version id of mlt death number estimate"
  )
  parser$add_argument(
    "--gbd_year", type = "integer", required = TRUE,
    help = "GBD year"
  )
  parser$add_argument(
    "--end_year", type = "integer", required = TRUE,
    help = "last year to make estimates for"
  )
  parser$add_argument(
    "--mark_best", type = "character", required = TRUE,
    help = "T/F to mark best"
  )
  parser$add_argument(
    "--code_dir", type = "character", required = TRUE,
    help = "directory where code is cloned"
  )
  parser$add_argument(
    "--workflow_args", type = "character", required = TRUE,
    action = "store", help = "workflow name"
  )
  
  args <- parser$parse_args()
  list2env(args, .GlobalEnv)
  
}

# Load mortality production config file
config <- config::get(
  file = fs::path(code_dir, "jobmon/mortality_production.yml"),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

# Update code_dir
code_dir <- fs::path(code_dir, "life-tables/lt_envelope")

# To run MLT on a subset of locations add ihme_loc_ids to this vector
subset_locations <- c()

# Specify other (usually left unchanged) mlt arguments
file_del <- TRUE
start <- 1L
end <- 3L

# Pull lineage
mlt_lineage <- get_proc_lineage(
  model_name = "mlt life table",
  model_type = "estimate",
  run_id = version_id
)
age_sex_estimate_version <- mlt_lineage[
  parent_process_name == "age sex estimate", parent_run_id
]
estimate_45q15_version <- mlt_lineage[
  parent_process_name == "45q15 estimate", parent_run_id
]
estimate_5q0_version <- mlt_lineage[
  parent_process_name == "5q0 estimate", parent_run_id
]
lt_empirical_data_version <- mlt_lineage[
  parent_process_name == "life table empirical data", parent_run_id
]
population_estimate_version <- mlt_lineage[
  parent_process_name == "population estimate", parent_run_id
]
u5_envelope_version <- mlt_lineage[
  parent_process_name == "u5 envelope estimate", parent_run_id
]
map_estimate_version <- mlt_lineage[
  parent_process_name == "life table map estimate", parent_run_id
]

agesex_lineage <- get_proc_lineage(
  model_name = "age sex",
  model_type = "estimate",
  run_id = age_sex_estimate_version
)
agesex_data_version <- agesex_lineage[
  parent_process_name == "age sex data", parent_run_id
] 

agesex_data_lineage <- get_proc_lineage(
  model_name = "age sex",
  model_type = "data",
  run_id = agesex_data_version
)
vr_version <- agesex_data_lineage[
  parent_process_name == "death number empirical data", parent_run_id
]

# Get external inputs
external_inputs <- get_external_input_map(
  process_name = "mlt life table estimate",
  run_id = version_id
)
spectrum_name <- external_inputs[external_input_name == "hiv", external_input_version]

# Re-name mlt version
mlt_envelope_version <- mlt_death_number_estimate

# Create directories
master_dir <- fs::path("FILEPATH", version_id)
input_dir <- fs::path(master_dir, "inputs")

fs::dir_create(master_dir)

lapply(
  c("diagnostics", "inputs", "lt_hiv_free", "lt_with_hiv", "env_with_hiv", "summary", "standard_lts"),
  function(x) fs::dir_create(fs::path(master_dir, x), mode = "u=rwx,go=rwx")
)

lapply(
  c("upload", "intermediary"),
  function(x) fs::dir_create(fs::path(master_dir, "summary", x), mode = "u=rwx,go=rwx")
)

for (dir in c("lt_hiv_free", "lt_with_hiv", "env_with_hiv")) {
  
  lapply(
    c("pre_scaled", "scaled"),
    function(x) fs::dir_create(fs::path(master_dir, dir, x), mode = "u=rwx,go=rwx")
  )
  
}

# Create location maps
locations <- get_locations(
  level = "estimate",
  gbd_year = gbd_year,
  hiv_metadata = TRUE
)
locations <- locations[
  , .(ihme_loc_id, location_id, region_id, super_region_id, parent_id, 
      local_id, group, is_estimate)
]

parent_locs <- unique(locations$parent_id)

parents <- get_locations(level = "all", gbd_year = gbd_year)
parents <- unique(parents[
  location_id %in% parent_locs, .(parent_id = location_id, parent_ihme = ihme_loc_id)
])

locations <- merge(locations, parents, by = "parent_id", all.x = TRUE)

# Specify how many ELTs to use when constructing the standard
# Use 15 for high-income locations (super_region_id = 64)
# Use 100 for all other locations
lt_match_map <- data.table(ihme_loc_id = locations$ihme_loc_id)

high_income_locs <- locations[super_region_id == 64, ihme_loc_id]

lt_match_map[, match := ifelse(ihme_loc_id %in% high_income_locs,
                               15,
                               100)]

assertthat::assert_that(
  all(unique(lt_match_map$ihme_loc_id) == unique(locations$ihme_loc_id)),
  msg = "Some locations are different"
)

assertable::assert_colnames(lt_match_map, c("ihme_loc_id", "match"))

if (length(subset_locations) > 0) {
  
  message(glue::glue("Subsetting to locations {subset_locations}"))
  locations[ihme_loc_id %in% subset_locations, is_estimate := 0]
  
}

run_countries <- locations[is_estimate == 1, ihme_loc_id]

readr::write_csv(locations, fs::path(input_dir, "lt_env_locations.csv"))
readr::write_csv(lt_match_map, fs::path(input_dir, "lt_match_map.csv"))

# Create workflow and templates ------------------------------------------------

# Create tool
wf_tool <- tool(name = "mlt")

set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list("project" = submission_project_name)
)

# Create task templates
mlt_prep_template <- task_template(
  tool = wf_tool,
  template_name = "mlt_prep",
  command_template = paste(
    "PATH=/opt/singularity/bin/:$PATH",
    "OMP_NUM_THREADS=0 {shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--spectrum_name {spectrum_name}",
    "--file_del {file_del}",
    "--start {start}",
    "--end {end}",
    "--gbd_year {gbd_year}",
    "--end_year {end_year}",
    "--lt_empirical_data_version {lt_empirical_data_version}",
    "--mlt_envelope_version {mlt_envelope_version}",
    "--map_estimate_version {map_estimate_version}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "script_path", "image_path"),
  task_args = list("start", "end", "file_del"),
  node_args = list("code_dir", "spectrum_name", "gbd_year", "end_year", "version_id",
                   "lt_empirical_data_version", "mlt_envelope_version", "map_estimate_version")
)

gen_lt_template <- task_template(
  tool = wf_tool,
  template_name = "mlt_lt_generation",
  command_template = paste(
    "PATH=/opt/singularity/bin/:$PATH",
    "OMP_NUM_THREADS=0 {shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--loc {loc}",
    "--spectrum_name {spectrum_name}",
    "--gbd_year {gbd_year}",
    "--end_year {end_year}",
    "--lt_empirical_data_version {lt_empirical_data_version}",
    "--estimate_45q15_version {estimate_45q15_version}",
    "--estimate_5q0_version {estimate_5q0_version}",
    "--age_sex_estimate_version {age_sex_estimate_version}",
    "--u5_envelope_version {u5_envelope_version}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "script_path", "image_path"),
  task_args = list(),
  node_args = list("code_dir", "spectrum_name", "gbd_year", "end_year","version_id", "loc",
                   "estimate_45q15_version", "estimate_5q0_version", "age_sex_estimate_version",
                   "u5_envelope_version", "lt_empirical_data_version")
)

scaling_template <- task_template(
  tool = wf_tool,
  template_name = "mlt_scale_agg",
  command_template = paste(
    "PATH=/opt/singularity/bin/:$PATH",
    "OMP_NUM_THREADS=0 {shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--year {year}",
    "--age_sex_estimate_version {age_sex_estimate_version}",
    "--gbd_year {gbd_year}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "script_path", "image_path"),
  task_args = list(),
  node_args = list("version_id", "year", "age_sex_estimate_version",
                   "gbd_year", "code_dir")
)

compile_upload_template <- task_template(
  tool = wf_tool,
  template_name = "mlt_compile_upload",
  command_template = paste(
    "PATH=/opt/singularity/bin/:$PATH",
    "OMP_NUM_THREADS=0 {shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--mlt_envelope_version {mlt_envelope_version}",
    "--map_estimate_version {map_estimate_version}",
    "--gbd_year {gbd_year}",
    "--end_year {end_year}",
    "--mark_best {mark_best}"
  ),
  op_args = list("shell_path", "script_path", "image_path"),
  task_args = list(),
  node_args = list("version_id", "mlt_envelope_version", "map_estimate_version",
                   "gbd_year", "end_year", "mark_best")
)

graph_template <- task_template(
  tool = wf_tool,
  template_name = "mlt_graphing",
  command_template = paste(
    "PATH=/opt/singularity/bin/:$PATH",
    "OMP_NUM_THREADS=0 {shell_path} -i {image_path} -s {script_path}",
    "--mlt_version {mlt_version}",
    "--mlt_env_version {mlt_env_version}",
    "--vr_version {vr_version}",
    "--gbd_year {gbd_year}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "script_path", "image_path"),
  task_args = list(),
  node_args = list("mlt_version", "mlt_env_version", "vr_version",
                   "gbd_year", "code_dir")
)

# Create tasks
wf <- workflow(
  tool = wf_tool,
  workflow_args = paste0("mlt_", workflow_args)
)

wf$workflow_attributes = list()

mlt_prep_task <- task(
  task_template = mlt_prep_template,
  name = "mlt_prep",
  upstream_tasks = list(),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = lt_image_path,
  script_path = fs::path(code_dir, "00_prep_mlt.R"),
  spectrum_name = spectrum_name,
  start = start,
  end = end,
  file_del = file_del,
  gbd_year = gbd_year,
  end_year = end_year,
  version_id = as.integer(version_id),
  lt_empirical_data_version = as.integer(lt_empirical_data_version),
  mlt_envelope_version = as.integer(mlt_envelope_version),
  map_estimate_version = as.integer(map_estimate_version),
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "10G",
    "cores" = 2L,
    "queue" = queue,
    "runtime" = "3600S"
  )
)

wf <- add_tasks(
  workflow = wf,
  tasks = list(mlt_prep_task)
)

gen_lt_tasks <- lapply(run_countries, function(x) {
  
  gen_lt_task <- task(
    task_template = gen_lt_template,
    name = paste0("mlt_lt_generation_", x),
    upstream_tasks = list(mlt_prep_task),
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = lt_image_path,
    script_path = fs::path(code_dir, "01_gen_lts.R"),
    version_id = as.integer(version_id),
    loc = x,
    spectrum_name = spectrum_name,
    estimate_45q15_version = as.integer(estimate_45q15_version),
    estimate_5q0_version = as.integer(estimate_5q0_version),
    age_sex_estimate_version = as.integer(age_sex_estimate_version),
    u5_envelope_version = as.integer(u5_envelope_version),
    lt_empirical_data_version = as.integer(lt_empirical_data_version),
    gbd_year = as.integer(gbd_year),
    end_year = as.integer(end_year),
    code_dir = code_dir,
    compute_resources = list(
      "memory" = "40G",
      "cores" = 5L,
      "queue" = queue,
      "runtime" = "30000S"
    )
  )
  
  return(gen_lt_task)
  
})

wf <- add_tasks(
  workflow = wf,
  tasks = gen_lt_tasks
)

scaling_tasks <- lapply(1950:end_year, function(x) {
  
  scaling_task <- task(
    task_template = scaling_template,
    name = paste0("mlt_lt_generation_", x),
    upstream_tasks = gen_lt_tasks,
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = lt_image_path,
    script_path = fs::path(code_dir, "02_scale_results.R"),
    version_id = as.integer(version_id),
    year = as.integer(x),
    age_sex_estimate_version = as.integer(age_sex_estimate_version),
    gbd_year = as.integer(gbd_year),
    code_dir = code_dir,
    compute_resources = list(
      "memory" = "90G",
      "cores" = 10L,
      "queue" = queue,
      "runtime" = "30000S"
    )
  )
  
  return(scaling_task)
  
})

wf <- add_tasks(
  workflow = wf,
  tasks = scaling_tasks
)

compile_upload_task <- task(
  task_template = compile_upload_template,
  name = "mlt_compile_upload",
  upstream_tasks = scaling_tasks,
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = lt_image_path,
  script_path = fs::path(code_dir, "03_compile_upload.R"),
  version_id = as.integer(version_id),
  mlt_envelope_version = as.integer(mlt_envelope_version),
  map_estimate_version = as.integer(map_estimate_version),
  gbd_year = as.integer(gbd_year),
  end_year = as.integer(end_year),
  mark_best = mark_best,
  compute_resources = list(
    "memory" = "40G",
    "cores" = 15L,
    "queue" = queue,
    "runtime" = "30800S"
  )
)

wf <- add_tasks(
  workflow = wf,
  tasks = list(compile_upload_task)
)

graph_task <- task(
  task_template = graph_template,
  name = "mlt_graphing",
  upstream_tasks = list(compile_upload_task),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = run_all_image_path,
  script_path = fs::path(code_dir, "04a_graphing_wrapper.R"),
  mlt_version = as.integer(version_id),
  mlt_env_version = as.integer(mlt_envelope_version),
  vr_version = as.integer(vr_version),
  gbd_year = as.integer(gbd_year),
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "1G",
    "cores" = 1L,
    "queue" = queue,
    "runtime" = "21600S"
  )
) 

wf <- add_tasks(
  workflow = wf,
  tasks = list(graph_task)
)

# Run workflow -----------------------------------------------------------------

wfr <- wf$run(resume = TRUE)

if (wfr != "D") {
  
  # Send failure notification
  send_slack_message(
    message = paste0("mlt life table estimate ", version_id, " failed."),
    channel = paste0("@", Sys.getenv("USER")),
    icon = ":thinking_face:",
    botname = "MLTBot"
  )  
  
  # Stop process
  stop(paste("MLT failed! See workflow_id:", wf$workflow_id))
  
} else {
  
  message("MLT completed!")
  
}

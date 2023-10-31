# Meta --------------------------------------------------------------------
#'
#' Summary: Submit workflow which harmonizes implied excess mortality,
#' estimated EM, OPRM, and covid
#'
#' Inputs:
#' 1. COVID-EM version (ours)
#' 2. EM age-sex splitting version
#' 3. Central Comp COVID EM version (referred to as oprm)

library(data.table)
library(jobmonr)
library(mortdb)

# Parse arguments ---------------------------------------------------------

parser <- argparse::ArgumentParser()

parser$add_argument(
  "wf_args",
  nargs = "?",
  type = "character",
  default = format(Sys.time(), "%Y_%m_%d-%H_%M_%S"),
  help = "Unique argument for Jobmon workflow"
)

args <- parser$parse_args()

wf_args <- args$wf_args


# Set parameters ----------------------------------------------------------

dir_proj <- here::here()

# Version of COVID-EM Splitting
run_id_splitting <- "2023_07_11-12_22_06" # "2023_04_18-16_01_39"

# Version of Central comp EM balancing
run_id_oprm <- 88L #82L

# Version of No-shock envelope to adjust
# NOTE: this should be the w/o pandemic data run used in EM splitting (version below)
# TODO: set up .yml for EM splitting that this process can read to get this info
run_id_ns_env <- 560L
run_id_mlt_dn_novr <- 557L

# note this should be a no shock death number version with 2020-2021 data
run_id_w_em_env <- 555L

# Version of COVID-EM
run_id_em <- "s3-2023-04-28-11-43"

# TODO: the parentage is rough, for now just define these manually
run_id_ddm <- 516L
run_id_elt <- 534L

# Upload CC Draws?
upload_cc_draws <- FALSE

dir_base <- "FILEPATH"
dir_output <- fs::path(dir_base, wf_args)

image_path <- "FILEPATH"
shell_path <- "FILEPATH"


# Make output directories -------------------------------------------------

dir.create(
  dir_output,
  mode = "0775"
)

fs::dir_create(
  fs::path(dir_output, "diagnostics"),
  mode = "0775"
)

fs::dir_create(
  fs::path(
    dir_output, "diagnostics",
    c("min_adj", "max_adj/loc_specific", "max_adj/loc_specific-v2")
  ),
  mode = "0775"
)

process_stages <- c(
  "loc_specific_env",
  "loc_specific_lt",
  "scaled_env",
  "covid_oprm_draws"
)

fs::dir_create(
  fs::path(dir_output, "draws", process_stages),
  mode = "0775"
)

fs::dir_create(
  fs::path(dir_output, "summary", process_stages),
  mode = "0775"
)


# Copy Inputs from EM Splitting -------------------------------------------

# To ensure consistency between this process and EM-splitting, we want
# to copy inputs from the parent_em_splitting version

dir_splitting <- fs::path(
  "FILEPATH",
  run_id_splitting
)

# files include location maps, population, VR data and availability
copy_files <- list.files(dir_splitting, full.names = T, pattern = ".csv")

if (!all(fs::file_exists(fs::path(dir_output, copy_files)))) {

  fs::file_copy(
    path = copy_files,
    new_path = dir_output,
    overwrite = TRUE
  )

}

# we need a new age map because this process works with life table ages
# and detailed U5 ages
age_map <- demInternal::get_age_map(type = "gbd", all_metadata = TRUE)
lt_age_map <- demInternal::get_age_map(type = "lifetable", all_metadata = TRUE)

readr::write_csv(
  age_map,
  fs::path(
    dir_output, "age_map.csv"
  )
)

readr::write_csv(lt_age_map, fs::path(dir_output, "lt_age_map.csv"))

# load vr years from em splitting
dt_covid_vr <- fread(
  fs::path(
    dir_output, "vr_pandemic_years_locations", ext = "csv"
  )
)

# load location_map
loc_map <- fread(
  fs::path(
    dir_output, "loc_map", ext = "csv"
  )
)

# get lowest level locations for specific steps
lowest_level_locs_map <- mortdb::get_locations(level = "lowest")

# Get Pipeline Parents ----------------------------------------------------

# Get children of the no-data run
dt_nsdn_children <- demInternal::get_parent_child(
  "no shock death number estimate",
  run_id = run_id_ns_env,
  lineage_type = "child"
)

run_id_finalizer <- dt_nsdn_children[
  child_process_name == "with shock death number estimate",
  as.integer(child_run_id)
]

# get life table estimate
dt_nsdn_parent <- demInternal::get_parent_child(
  "no shock death number estimate",
  run_id = run_id_ns_env,
  lineage_type = "parent"
)

run_id_nslt <- dt_nsdn_parent[
  parent_process_name == "no shock life table estimate",
  as.integer(parent_run_id)
]

# Get children of the with data run
dt_wemdn_children <- demInternal::get_parent_child(
  "no shock death number estimate",
  run_id = run_id_w_em_env,
  lineage_type = "child"
)

run_id_finalizer_with_data <- dt_wemdn_children[
  child_process_name == "with shock death number estimate",
  as.integer(child_run_id)
]

# get VR version from the parent EM splitting run
dt_total_vr <- fread(
  fs::path(
    dir_output,
    "vr_total_deaths.csv"
  )
)

run_id_vrp <- unique(dt_total_vr$run_id)

# get children of VR run (DDM, ELT)
dt_vrp_children <- demInternal::get_parent_child(
  "death number empirical data",
  run_id = run_id_vrp,
  lineage_type = "child"
)

# HOTFIX: replace finalizer with MLT versions
run_id_finalizer <- run_id_ns_env
run_id_finalizer_with_data <- run_id_w_em_env
run_id_nslt <- run_id_ns_env

# Handle outliering -------------------------------------------------------

# Pull the uploaded elt results for outliering information
dt_elt_outliering <- mortdb::get_mort_outputs(model_name = "life table empirical",
                                              model_type = "data",
                                              run_id = run_id_elt,
                                              life_table_parameter_ids = 3,
                                              age_group_ids = 5) # subset to save memory and potentially time

# collect outlier status for each source in ELT
dt_elt_outliering <- unique(dt_elt_outliering[outlier == 0, .(location_id, year_id, sex_id,
                                                              nid, underlying_nid, outlier)])

# add outliering status to vr
dt_covid_vr[dt_elt_outliering, outlier := i.outlier, on = .(location_id, year_id)]

# hotfix: set all mex to unoutliered (0)
#dt_covid_vr[ihme_loc_id %like% "MEX", outlier == 0]

# Create workflow ---------------------------------------------------------

wf_tool <- jobmonr::tool(name = "harmonize_em")

wf_tool <- jobmonr::set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = "slurm",
  resources = list(
    project = "proj_mortenvelope",
    queue = "all.q"
  )
)

wf <- jobmonr::workflow(
  tool = wf_tool,
  workflow_args = wf_args,
  name = "harmonize_em"
)


# 0) Prep data ------------------------------------------------------------

# Also prep VR data for graphing
template_prep_vr <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "prep_vr",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--run_id_vrp {run_id_vrp}",
    "--run_id_ddm {run_id_ddm}",
    "--run_id_elt {run_id_elt}"
  ),
  node_args = list(),
  task_args = list("dir_output", "run_id_vrp", "run_id_ddm", "run_id_elt"),
  op_args = list("shell_path", "image_path", "script_path")
)

task_prep_vr <- jobmonr::task(
  task_template = template_prep_vr,
  name = paste0("prep_vr-", wf_args),
  max_attempts = 2,
  compute_resources = list(
    "memory" = "2G",
    "cores" = 2L,
    "runtime" = "600S"
  ),
  shell_path = shell_path,
  image_path = image_path,
  script_path = fs::path(dir_proj, "prep_vr.R"),
  dir_output = dir_output,
  run_id_vrp = run_id_vrp,
  run_id_ddm = run_id_ddm,
  run_id_elt = run_id_elt
)

wf <- jobmonr::add_tasks(wf, list(task_prep_vr))

# 1) Adjust Envelope ------------------------------------------------------

pandemic_years <- unique(dt_covid_vr$year_id)

# For all location, adjust envelope
task_adjust_lt <- lapply(pandemic_years, function(yr) {

  adjust_locs <- loc_map[is_estimate == 1, location_id]

  template_implied_em <- jobmonr::task_template(
    tool = wf_tool,
    template_name = glue:::glue("adjust_lt_{yr}"),
    command_template = paste(
      "{shell_path} -i {image_path} -s {script_path}",
      "--dir_output {dir_output}",
      "--loc_id {loc_id}",
      "--current_year {current_year}",
      "--run_id_em {run_id_em}",
      "--run_id_splitting {run_id_splitting}",
      "--run_id_oprm {run_id_oprm}",
      "--run_id_finalizer {run_id_finalizer}",
      "--run_id_finalizer_with_data {run_id_finalizer_with_data}"
    ),
    node_args = list("loc_id"),
    task_args = list("dir_output", "current_year", "run_id_em", "run_id_oprm",
                     "run_id_splitting", "run_id_finalizer",
                     "run_id_finalizer_with_data"),
    op_args = list("shell_path", "image_path", "script_path")
  )

  task_implied_em <- jobmonr::array_tasks(
    task_template = template_implied_em,
    name = paste0("harmonize_env-", wf_args),
    max_attempts = 2,
    compute_resources = list(
      "memory" = "48G",
      "cores" = 4L,
      "runtime" = "600S"
    ),
    shell_path = shell_path,
    image_path = image_path,
    script_path = fs::path(dir_proj, "harmonize_lt.R"),
    dir_output = dir_output,
    run_id_em = run_id_em,
    run_id_splitting = run_id_splitting,
    run_id_oprm = run_id_oprm,
    run_id_finalizer = run_id_finalizer,
    run_id_finalizer_with_data = run_id_finalizer_with_data,

    loc_id = as.list(adjust_locs),
    current_year = yr
  )
}) |> unlist()

wf <- jobmonr::add_tasks(wf, task_adjust_lt)


# 2) Adjust Missing Loc-years ---------------------------------------------

# for each location where we have data in 2020, but not 2021 we want to
# adjust the no-shock envelope
vr_2020_locs <- dt_covid_vr[year_id == 2020 & outlier == 0, location_id]
vr_2021_locs <- dt_covid_vr[year_id == 2021 & outlier == 0, location_id]

# HOTFIX, remove MEX subnationals from the 2021 list so that we imput them
mex_subnats <- c(4669, 4672, 4655) # select mexico subnats which have female VR outliered
vr_2021_locs <- setdiff(vr_2021_locs, mex_subnats)

vr_missing_locs <- setdiff(vr_2020_locs, vr_2021_locs)

# # HOTFIX, do not impute 2021 for PHL
# phl_locs <- loc_map[ihme_loc_id %like% "PHL", location_id]
#
# vr_missing_locs <- setdiff(vr_missing_locs, phl_locs)

template_impute_lt <- jobmonr::task_template(
  tool = wf_tool,
  template_name = glue:::glue("impute_lt_2021"),
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--loc_id {loc_id}",
    "--current_year {current_year}",
    "--run_id_em {run_id_em}",
    "--run_id_splitting {run_id_splitting}",
    "--run_id_oprm {run_id_oprm}",
    "--run_id_finalizer {run_id_finalizer}"
  ),
  node_args = list("loc_id"),
  task_args = list("dir_output", "current_year", "run_id_em", "run_id_oprm",
                   "run_id_splitting", "run_id_finalizer"),
  op_args = list("shell_path", "image_path", "script_path")
)

task_impute_lt <- jobmonr::array_tasks(
  task_template = template_impute_lt,
  name = paste0("impute_lt-", wf_args),
  upstream_tasks = task_adjust_lt,
  max_attempts = 2,
  compute_resources = list(
    "memory" = "12G",
    "cores" = 2L,
    "runtime" = "300S"
  ),
  shell_path = shell_path,
  image_path = image_path,
  script_path = fs::path(dir_proj, "impute_2021.R"),
  dir_output = dir_output,
  run_id_em = run_id_em,
  run_id_splitting = run_id_splitting,
  run_id_oprm = run_id_oprm,
  run_id_finalizer = run_id_finalizer,

  loc_id = as.list(vr_missing_locs),
  current_year = 2021L
)

wf <- jobmonr::add_tasks(wf, task_impute_lt)


# Scale envelope ----------------------------------------------------------

template_scale_agg <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "scale_agg",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--current_year {current_year}"
  ),
  node_args = list("current_year"),
  task_args = list("dir_output"),
  op_args = list("shell_path", "image_path", "script_path")
)

task_scale_agg <- jobmonr::array_tasks(
  task_template = template_scale_agg,
  name = paste0("scale_agg-", wf_args),
  max_attempts = 2,
  compute_resources = list(
    "memory" = "64G",
    "cores" = 16L,
    "runtime" = "6000S"
  ),
  shell_path = shell_path,
  upstream_tasks = task_impute_lt,
  image_path = image_path,
  script_path = fs::path(dir_proj, "scale_results.R"),
  dir_output = dir_output,
  current_year = 2020:2021
)

wf <- jobmonr::add_tasks(wf, task_scale_agg)


# 2.5) Graph adjusted envelope --------------------------------------------

# Prep Indirect COVID causes to provide mean level estimates for graphing
template_prep_indirect <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "prep_indirect_covid",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--run_id_oprm {run_id_oprm}"
  ),
  node_args = list(),
  task_args = list("dir_output", "run_id_oprm"),
  op_args = list("shell_path", "image_path", "script_path")
)

task_prep_indirect <- jobmonr::task(
  task_template = template_prep_indirect,
  name = paste0("prep_indirect-", wf_args),
  max_attempts = 2,
  compute_resources = list(
    "memory" = "20G",
    "cores" = 2L,
    "runtime" = "6000S"
  ),
  shell_path = shell_path,
  upstream_tasks = task_scale_agg,
  image_path = image_path,
  script_path = fs::path(dir_proj, "prep_indirect.R"),
  dir_output = dir_output,
  run_id_oprm = run_id_oprm,
)

wf <- jobmonr::add_tasks(wf, list(task_prep_indirect))


# Graphing
#template_graph_env <- jobmonr::task_template(
#   tool = wf_tool,
#   template_name = glue:::glue("graph_adj_env"),
#   command_template = paste(
#     "{shell_path} -i {image_path} -s {script_path}",
#     "--dir_output {dir_output}",
#     "--run_id_ns_env {run_id_ns_env}"
#   ),
#   node_args = list(),
#   task_args = list("dir_output", "run_id_ns_env"),
#   op_args = list("shell_path", "image_path", "script_path")
# )
#
# task_graph_env <- jobmonr::task(
#   task_template = template_graph_env,
#   name = paste0("graph_env-", wf_args),
#   upstream_tasks = append(task_impute_lt, task_prep_indirect) |> unlist(),
#   max_attempts = 2,
#   compute_resources = list(
#     "memory" = "200G",
#     "cores" = 20L,
#     "runtime" = "6000S"
#   ),
#   shell_path = shell_path,
#   image_path = image_path,
#   script_path = fs::path(dir_proj, "plot_diagnostics.R"),
#   dir_output = dir_output,
#   run_id_ns_env = run_id_ns_env,
# )
#
# wf <- jobmonr::add_tasks(wf, list(task_graph_env))


# 3) Convert to mx and extend ages ----------------------------------------

# Do this for all locations with any pandemic VR in 2020
# and for both 2020 and 2021
task_convert_lt <- lapply(pandemic_years, function(yr) {

  all_locs <- loc_map[level >= 3 & ihme_loc_id != "CHN", location_id] # Changed to not do aggregate locations (or CHN (6) because that is missing too for some reason)

  template_implied_em <- jobmonr::task_template(
    tool = wf_tool,
    template_name = glue:::glue("convert_lt_{yr}"),
    command_template = paste(
      "{shell_path} -i {image_path} -s {script_path}",
      "--dir_output {dir_output}",
      "--loc_id {loc_id}",
      "--current_year {current_year}",
      "--run_id_em {run_id_em}",
      "--run_id_splitting {run_id_splitting}",
      "--run_id_oprm {run_id_oprm}",
      "--run_id_finalizer {run_id_finalizer}"
    ),
    node_args = list("loc_id"),
    task_args = list("dir_output", "current_year", "run_id_em", "run_id_oprm",
                     "run_id_splitting", "run_id_finalizer"),
    op_args = list("shell_path", "image_path", "script_path")
  )

  task_implied_em <- jobmonr::array_tasks(
    task_template = template_implied_em,
    name = paste0("convert_to_lt-", wf_args),
    max_attempts = 2,
    compute_resources = list(
      "memory" = "10G",
      "cores" = 2L,
      "runtime" = "600S"
    ),
    upstream_tasks = task_scale_agg,
    shell_path = shell_path,
    image_path = image_path,
    script_path = fs::path(dir_proj, "extend_to_lt_ages.R"),
    dir_output = dir_output,
    run_id_em = run_id_em,
    run_id_splitting = run_id_splitting,
    run_id_oprm = run_id_oprm,
    run_id_finalizer = run_id_finalizer,

    loc_id = as.list(all_locs),
    current_year = yr
  )
}) |> unlist()

wf <- jobmonr::add_tasks(wf, task_convert_lt)

# 3.5) Graph adjusted envelope --------------------------------------------

template_graph_env <- jobmonr::task_template(
  tool = wf_tool,
  template_name = glue:::glue("graph_adj_env"),
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--run_id_ns_env {run_id_ns_env}"
  ),
  node_args = list(),
  task_args = list("dir_output", "run_id_ns_env"),
  op_args = list("shell_path", "image_path", "script_path")
)

task_graph_env <- jobmonr::task(
  task_template = template_graph_env,
  name = paste0("graph_env-", wf_args),
  upstream_tasks = append(task_prep_indirect, task_scale_agg) |> unlist(),
  max_attempts = 2,
  compute_resources = list(
    "memory" = "24G",
    "cores" = 20L,
    "runtime" = "6000S"
  ),
  shell_path = shell_path,
  image_path = image_path,
  script_path = fs::path(dir_proj, "plot_diagnostics.R"),
  dir_output = dir_output,
  run_id_ns_env = run_id_mlt_dn_novr,
)

wf <- jobmonr::add_tasks(wf, list(task_graph_env))

# graph rates
template_graph_rates <- jobmonr::task_template(
  tool = wf_tool,
  template_name = glue:::glue("graph_adj_lt"),
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--run_id_nslt {run_id_nslt}",
    "--run_id_mlt_dn {run_id_mlt_dn}"
  ),
  node_args = list(),
  task_args = list("dir_output", "run_id_nslt", "run_id_mlt_dn"),
  op_args = list("shell_path", "image_path", "script_path")
)

task_graph_rates <- jobmonr::task(
  task_template = template_graph_rates,
  name = paste0("graph_rates-", wf_args),
  upstream_tasks = task_convert_lt,
  max_attempts = 2,
  compute_resources = list(
    "memory" = "200G",
    "cores" = 20L,
    "runtime" = "6000S"
  ),
  shell_path = shell_path,
  image_path = image_path,
  script_path = fs::path(dir_proj, "plot_rate_diagnostics.R"),
  dir_output = dir_output,
  run_id_nslt = run_id_nslt,
  run_id_mlt_dn = run_id_mlt_dn_novr,
)

wf <- jobmonr::add_tasks(wf, list(task_graph_rates))


# 4. further diagnostics --------------------------------------------------

# graph oprm, covid, before and after
template_graph_oprm_adjust <- jobmonr::task_template(
  tool = wf_tool,
  template_name = glue:::glue("graph_adj_oprm_covid"),
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--run_id_balancing {run_id_balancing}"
  ),
  node_args = list(),
  task_args = list("dir_output", "run_id_balancing"),
  op_args = list("shell_path", "image_path", "script_path")
)

task_graph_oprm_adjust <- jobmonr::task(
  task_template = template_graph_oprm_adjust,
  name = paste0("graph_oprm_adj-", wf_args),
  upstream_tasks = task_convert_lt,
  max_attempts = 2,
  compute_resources = list(
    "memory" = "100G",
    "cores" = 10L,
    "runtime" = "6000S"
  ),
  shell_path = shell_path,
  image_path = image_path,
  script_path = fs::path(dir_proj, "graph_covid_oprm_adjustment.R"),
  dir_output = dir_output,
  run_id_balancing = run_id_oprm,
)

wf <- jobmonr::add_tasks(wf, list(task_graph_oprm_adjust))

# graph raking, before and after
template_graph_raking <- jobmonr::task_template(
  tool = wf_tool,
  template_name = glue:::glue("graph_raking"),
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--run_id_balancing {run_id_balancing}"
  ),
  node_args = list(),
  task_args = list("dir_output", "run_id_balancing"),
  op_args = list("shell_path", "image_path", "script_path")
)

task_graph_raking <- jobmonr::task(
  task_template = template_graph_raking,
  name = paste0("graph_raking-", wf_args),
  upstream_tasks = task_convert_lt,
  max_attempts = 2,
  compute_resources = list(
    "memory" = "100G",
    "cores" = 10L,
    "runtime" = "6000S"
  ),
  shell_path = shell_path,
  image_path = image_path,
  script_path = fs::path(dir_proj, "graph_raking.R"),
  dir_output = dir_output,
  run_id_balancing = run_id_oprm,
)

wf <- jobmonr::add_tasks(wf, list(task_graph_raking))

# save additional diagnostic .csvs
template_addn_diagnostics <- jobmonr::task_template(
  tool = wf_tool,
  template_name = glue:::glue("save_diag"),
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--run_id_balancing {run_id_balancing}"
  ),
  node_args = list(),
  task_args = list("dir_output", "run_id_balancing"),
  op_args = list("shell_path", "image_path", "script_path")
)

task_addn_diagnostics <- jobmonr::task(
  task_template = template_addn_diagnostics,
  name = paste0("addn_diagnostics-", wf_args),
  upstream_tasks = task_convert_lt,
  max_attempts = 2,
  compute_resources = list(
    "memory" = "10G",
    "cores" = 2L,
    "runtime" = "6000S"
  ),
  shell_path = shell_path,
  image_path = image_path,
  script_path = fs::path(dir_proj, "save_high_em_locations.R"),
  dir_output = dir_output,
  run_id_balancing = run_id_oprm,
)

wf <- jobmonr::add_tasks(wf, list(task_addn_diagnostics))

# CC draws upload ---------------------------------------------------------

if (upload_cc_draws) {
  task_upload_cc <- lapply(c(1048L, 1058L), function(cause_id) {

    template_upload_cc <- jobmonr::task_template(
      tool = wf_tool,
      template_name = glue:::glue("upload_{cause_id}"),
      command_template = paste(
        "{shell_path} -i {image_path} -s {script_path}",
        "--dir_output {dir_output}",
        "--current_sex_id {current_sex_id}",
        "--cause_id {cause_id}"
      ),
      node_args = list("current_sex_id"),
      task_args = list("dir_output", "cause_id"),
      op_args = list("shell_path", "image_path", "script_path")
    )

    task_upload_cc <- jobmonr::array_tasks(
      task_template = template_upload_cc,
      name = paste0("upload_cc-", wf_args),
      max_attempts = 2,
      upstream_tasks = task_scale_agg,
      compute_resources = list(
        "memory" = "120G",
        "cores" = 10L,
        "runtime" = "90000S"
      ),
      shell_path = shell_path,
      image_path = image_path,
      script_path = fs::path(dir_proj, "upload_cc_draws.R"),
      dir_output = dir_output,
      current_sex_id = 1:2,

      cause_id = cause_id

    )
  }) |> unlist()

  wf <- jobmonr::add_tasks(wf, task_upload_cc)

}
# Run workflow ------------------------------------------------------------

wfr <- wf$run(resume = TRUE)

if (wfr == "D") {

  message("Workflow Completed!")

} else {

  stop("An issue occurred.")

}


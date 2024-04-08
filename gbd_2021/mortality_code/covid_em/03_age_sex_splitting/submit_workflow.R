
# Meta --------------------------------------------------------------------

# Launch scripts to age-sex split excess mortality results

# Load packages -----------------------------------------------------------

library(data.table)
library(jobmonr)


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


# no-shock death number run IDs to get implied shocks from
run_id_nsdn <- list(
  w_2020_vr = 651L,
  n_2020_vr = 650L
)

# Version of VR prep to get locations with 2020-2021 VR
run_id_vrp <- 356L

# Version of COVID-EM
run_id_em <- "VERSION"

# Version of Population
run_id_pop <- 357L

# Years to scale results for
pandemic_years <- 2020:2021

# If a location has fewer than this many excess deaths, use the
# global distribution in step 4
em_threshold <- c(`2020` = 75e3L,
                  `2021` = 75e3L)

# Some locations are going to be exceptions to the threshold
force_global_2020 <- c("MEX", "BRA", "IRN", "ITA", "RUS", "PER")
force_global_2021 <- c("MEX", "BRA", "PHL", "POL", "RUS")
force_global_ihme <- data.table(ihme_loc_id = c(force_global_2020, force_global_2021),
                                year_id = c(rep(2020, length(force_global_2020)),
                                            rep(2021, length(force_global_2021))))

# Locations where we do not have data. (we will aggregate to these locations)
# at the end
drop_ihme_locs <- c("GBR", "CHN", "UKR")

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

process_stages <- c(
  "01_implied_em",
  "04_adjusted_age_sex",
  "06_scaled_em"
)

fs::dir_create(
  fs::path(dir_output, "draws", process_stages),
  mode = "0775"
)

fs::dir_create(
  fs::path(dir_output, "summary", process_stages),
  mode = "0775"
)


# Load age and location maps ----------------------------------------------

map_locs <- demInternal::get_locations(gbd_year = 2020)
age_map <- mortdb::get_age_map(type = "gbd")

readr::write_csv(map_locs, fs::path(dir_output, "loc_map", ext = "csv"))
readr::write_csv(age_map, fs::path(dir_output, "age_map", ext = "csv"))

# save excluded locations
readr::write_csv(force_global_ihme, fs::path(dir_output, "force_global_locs", ext = "csv"))


# Get Pandemic Years VR locations -----------------------------------------

vrp <- demInternal::get_dem_outputs(
  "death number empirical data",
  run_id = run_id_vrp,
  year_id = pandemic_years,
  sex_ids = 3,
  age_group_ids = 22,
  name_cols = TRUE
)

vrp <- vrp[detailed_source != "IND_SRS_2020"]

vrp <- vrp[outlier == 0] # keep only unoutliered VR

dt_covid_vr <- unique(vrp[, .(location_id, ihme_loc_id, year_id)])

drop_loc_ids <- map_locs[ihme_loc_id %in% drop_ihme_locs, location_id]

dt_covid_vr <- dt_covid_vr[!location_id %in% drop_loc_ids]

# save location-years where VR is present
readr::write_csv(
  dt_covid_vr,
  fs::path(dir_output, "vr_pandemic_years_locations.csv")
)

# Also save 2020 location-year total VR deaths
readr::write_csv(
  vrp,
  fs::path(dir_output, "vr_total_deaths.csv")
)

# Get finalizer versions --------------------------------------------------

version_finalizer <- lapply(run_id_nsdn, function(run) {

  dt_nsdn_children <- demInternal::get_parent_child(
    "no shock death number estimate",
    run_id = run,
    lineage_type = "child"
  )

  dt_nsdn_children[
    child_process_name == "with shock death number estimate",
    as.integer(child_run_id)
  ]

})


# Get Baseline pop --------------------------------------------------------

pop <- demInternal::get_dem_outputs(
  "population estimate",
  run_id = run_id_pop,
  year_id = pandemic_years,
)

readr::write_csv(
  pop,
  fs::path(dir_output, "pop.csv")
)


# Create workflow ---------------------------------------------------------

wf_tool <- jobmonr::tool(name = "age_sex_em")

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
  name = "age_sex_em"
)


# 1) Calculate Implied Excess ---------------------------------------------

# For all location years with VR data, calculate implied excess rates
task_implied_em <- lapply(pandemic_years, function(yr) {

  vr_locs <- dt_covid_vr[year_id == yr, location_id]

  template_implied_em <- jobmonr::task_template(
    tool = wf_tool,
    template_name = glue:::glue("calc_props_{yr}"),
    command_template = paste(
      "{shell_path} -i {image_path} -s {script_path}",
      "--dir_output {dir_output}",
      "--version_finalizer_wshock {v_finalizer_w_2020_vr}",
      "--version_finalizer_nshock {v_finalizer_n_2020_vr}",
      "--loc_id {loc_id}",
      "--current_year {current_year}"
    ),
    node_args = list("loc_id"),
    task_args = list("dir_output", "v_finalizer_w_2020_vr",
                     "v_finalizer_n_2020_vr", "current_year"),
    op_args = list("shell_path", "image_path", "script_path")
  )

  task_implied_em <- jobmonr::array_tasks(
    task_template = template_implied_em,
    name = paste0("calc_implied_em_rates-", wf_args),
    max_attempts = 2,
    compute_resources = list(
      "memory" = "2G",
      "cores" = 2L,
      "runtime" = "300S"
    ),
    shell_path = shell_path,
    image_path = image_path,
    script_path = fs::path(dir_proj, "01_calc_age_sex_props.R"),
    dir_output = dir_output,
    v_finalizer_w_2020_vr = version_finalizer$w_2020_vr,
    v_finalizer_n_2020_vr = version_finalizer$n_2020_vr,

    loc_id = as.list(vr_locs),
    current_year = yr
  )
}) |> unlist()

wf <- jobmonr::add_tasks(wf, task_implied_em)


# 2) Calculate global distribution of excess ------------------------------

# We calculate 2 types of global EM age-sex distributions
# 1. Positive (using locations with positive implied excess from 2020)
# 2. Negative (using locations with negative implied excess from all
# pandemic years)
template_global_em_rate <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "global_em_rate",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}"
  ),
  node_args = list(),
  task_args = list("dir_output"),
  op_args = list("shell_path", "image_path", "script_path")
)


task_global_em_rate <- jobmonr::task(
  task_template = template_global_em_rate,
  name = paste0("global_em_rate-", wf_args),
  max_attempts = 2,
  upstream_tasks = task_implied_em,
  compute_resources = list(
    "memory" = "10G",
    "cores" = 4L,
    "runtime" = "600S"
  ),
  shell_path = shell_path,
  image_path = image_path,
  script_path = fs::path(dir_proj, "02_calc_global_em_rate.R"),
  dir_output = dir_output,
)

wf <- jobmonr::add_tasks(wf, list(task_global_em_rate))


# 3) Split National EM ----------------------------------------------------

#' We believe the Estimated EM total deaths are correct, therefore we
#' split estimated EM deaths using the implied EM rates.
#'
#' Depending on the location-year, we use either rates from the same
#' location-year, global distribution, or the previous year for the
#' same location
#'
#' NOTE: for this purpose, GBR level 4 locations (England, Scotland, etc.)
#' are treated as the national locations as well as Macao (CHN_361)
#' and the UKR subnationals

# get all "national" locations
nat_locs <- map_locs[level == 3 & !ihme_loc_id %in% c("GBR", "CHN", "UKR") |
                       (level == 4 & ihme_loc_id %like% "GBR") |
                       (level == 4 & ihme_loc_id %like% "CHN") |
                       (level == 4 & ihme_loc_id %like% "UKR")]

task_adj_nat <- lapply(nat_locs$location_id, function(loc_id) {
  lapply(pandemic_years, function(yr) {

    current_ihme <- map_locs[location_id == loc_id, ihme_loc_id]
    force_global <- ifelse(nrow(force_global_ihme[ihme_loc_id == current_ihme &
                                                    year_id == yr]) == 0,
                           FALSE,
                           TRUE)

    template_adj_nat <- jobmonr::task_template(
      tool = wf_tool,
      template_name = glue::glue("adjust_national_em-{loc_id}-{yr}"),
      command_template = paste(
        "{shell_path} -i {image_path} -s {script_path}",
        "--dir_output {dir_output}",
        "--loc_id {loc_id}",
        "--current_year {current_year}",
        "--em_threshold {em_threshold}",
        "--force_global {force_global}",
        "--run_id_em {run_id_em}"
      ),
      node_args = list(),
      task_args = list("loc_id", "dir_output", "current_year", "em_threshold",
                       "force_global", "run_id_em"),
      op_args = list("shell_path", "image_path", "script_path")
    )

    task_adj_nat <- jobmonr::task(
      task_template = template_adj_nat,
      name = paste0("adjust_national_em-", loc_id, "-", yr),
      max_attempts = 2,
      upstream_tasks = list(task_global_em_rate),
      compute_resources = list(
        "memory" = "8G",
        "cores" = 2L,
        "runtime" = "300S"
      ),
      shell_path = shell_path,
      image_path = image_path,
      script_path = fs::path(dir_proj, "04_apply_new_distribution.R"),
      dir_output = dir_output,
      current_year = yr,
      em_threshold = em_threshold[as.character(yr)],
      force_global = force_global,
      run_id_em = run_id_em,

      loc_id = loc_id
    )


  })
}) |> unlist()

wf <- jobmonr::add_tasks(wf, task_adj_nat)

# 4) Split subnational em -------------------------------------------------

# In general, All subnationals should match the rates of their parent locations
subnat_locs <- map_locs[!location_id %in% nat_locs$location_id & level > 3]

task_adj_sub <- lapply(pandemic_years, function(yr) {

  template_adj_nat <- jobmonr::task_template(
    tool = wf_tool,
    template_name = glue::glue("adjust_subnational_em-{yr}"),
    command_template = paste(
      "{shell_path} -i {image_path} -s {script_path}",
      "--dir_output {dir_output}",
      "--loc_id {loc_id}",
      "--current_year {current_year}",
      "--em_threshold {em_threshold}",
      "--force_global {force_global}",
      "--run_id_em {run_id_em}"
    ),
    node_args = list("loc_id"),
    task_args = list("dir_output", "current_year", "em_threshold",
                     "force_global", "run_id_em"),
    op_args = list("shell_path", "image_path", "script_path")
  )

  task_adj_sub <- jobmonr::array_tasks(
    task_template = template_adj_nat,
    name = paste0("adjust_subnational_em-", wf_args),
    max_attempts = 2,
    upstream_tasks = task_adj_nat,
    compute_resources = list(
      "memory" = "8G",
      "cores" = 2L,
      "runtime" = "300S"
    ),
    shell_path = shell_path,
    image_path = image_path,
    script_path = fs::path(dir_proj, "04_apply_new_distribution.R"),
    dir_output = dir_output,
    current_year = yr,
    em_threshold = em_threshold[as.character(yr)],
    force_global = FALSE,
    run_id_em = run_id_em,

    loc_id = as.list(subnat_locs$location_id)
  )

}) |> unlist()

wf <- jobmonr::add_tasks(wf, task_adj_sub)


# Finalize Results --------------------------------------------------------

# Compile all results and then scale/aggregate. GBR, and UKR national are
# aggregated to and all other locations are scaled.
template_scale <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "scale_results",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}"
  ),
  node_args = list(),
  task_args = list("dir_output"),
  op_args = list("shell_path", "image_path", "script_path")
)


task_scale <- jobmonr::task(
  task_template = template_scale,
  name = paste0("scale", wf_args),
  max_attempts = 2,
  upstream_tasks = task_adj_sub,
  compute_resources = list(
    "memory" = "70G",
    "cores" = 8L,
    "runtime" = "3000S"
  ),
  shell_path = shell_path,
  image_path = image_path,
  script_path = fs::path(dir_proj, "06_scaling.R"),
  dir_output = dir_output,
)

wf <- jobmonr::add_tasks(wf, list(task_scale))


# Graph distributions -----------------------------------------------------

# Graph global distributions
# runs after they are calculated
template_graph_global <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "graph_global",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}"
  ),
  node_args = list(),
  task_args = list("dir_output"),
  op_args = list("shell_path", "image_path", "script_path")
)


task_graph_global <- jobmonr::task(
  task_template = template_graph_global,
  name = paste0("graph_global", wf_args),
  max_attempts = 2,
  upstream_tasks = list(task_global_em_rate),
  compute_resources = list(
    "memory" = "2G",
    "cores" = 2L,
    "runtime" = "600S"
  ),
  shell_path = shell_path,
  image_path = image_path,
  script_path = fs::path(dir_proj, "03_graph_distributions.R"),
  dir_output = dir_output,
)

wf <- jobmonr::add_tasks(wf, list(task_graph_global))

# Graph implied and final results
# runs after final results
template_graph_final <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "graph_final",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}"
  ),
  node_args = list(),
  task_args = list("dir_output"),
  op_args = list("shell_path", "image_path", "script_path")
)


task_graph_final <- jobmonr::task(
  task_template = template_graph_final,
  name = paste0("graph_final", wf_args),
  max_attempts = 2,
  upstream_tasks = list(task_scale),
  compute_resources = list(
    "memory" = "5G",
    "cores" = 2L,
    "runtime" = "6000S"
  ),
  shell_path = shell_path,
  image_path = image_path,
  script_path = fs::path(dir_proj, "graph_final_compare.R"),
  dir_output = dir_output,
)

wf <- jobmonr::add_tasks(wf, list(task_graph_final))


# Run workflow ------------------------------------------------------------

wfr <- wf$run(resume = TRUE)

if (wfr == "D") {

  message("Workflow Completed!")

} else {

  stop("An issue occurred.")

}


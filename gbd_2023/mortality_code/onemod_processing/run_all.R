# Run all for onemod processing

library(jobmonr)

code_dir <- here::here()

cfg <- config::get(file = fs::path(code_dir, "config.yml"))

# Make Directories --------------------------------------------------------

dir_output <- fs::path(cfg$dir_processing_onemod, cfg$run_id)

if (!dir.exists(dir_output)) {

  fs::dir_create(dir_output)

  fs::dir_create(
    fs::path(dir_output, c("summaries", "draws", "diagnostics", "msca_pred")),
    mode = "775"
  )

  fs::file_copy(
    fs::path(code_dir, "config.yml"),
    fs::path(dir_output)
  )
}


# Save Age and Location Maps ----------------------------------------------

loc_map <- demInternal::get_locations(level = "all", gbd_year = cfg$gbd_year)

age_map_gbd <- demInternal::get_age_map(all_metadata = TRUE, type = "gbd")

age_map_sy <- demInternal::get_age_map(all_metadata = TRUE, type = "single_year")

age_map_canonical <- rbind(
  age_map_gbd[age_end <= 1 | age_end == Inf],
  age_map_sy[age_end > 1 & age_end <= 95]
)[order(age_start)]

readr::write_csv(loc_map,fs::path(dir_output, "loc_map.csv"))

readr::write_csv(age_map_gbd, fs::path(dir_output, "age_map_gbd.csv"))

readr::write_csv(age_map_sy, fs::path(dir_output, "age_map_sy.csv"))

readr::write_csv(age_map_canonical, fs::path(dir_output, "age_map_canonical.csv"))


# Create workflow ---------------------------------------------------------

wf_tool <- jobmonr::tool(name = "processing_onemod")

wf_tool <- jobmonr::set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = "slurm",
  resources = list(
    project = "proj_mortenvelope",
    queue = cfg$queue
  )
)

wf <- jobmonr::workflow(
  tool = wf_tool,
  workflow_args = cfg$wf_args,
  name = "processing_onemod"
)


# Prep Inputs -------------------------------------------------------------

template_prep_inputs <- jobmonr::task_template(
  tool = wf_tool,
  template_name = glue:::glue("prep_inputs"),
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--code_dir {code_dir}"
  ),
  node_args = list(),
  task_args = list("dir_output", "code_dir"),
  op_args = list("shell_path", "image_path", "script_path")
)

task_prep_inputs <- jobmonr::task(
  task_template = template_prep_inputs,
  name = paste0("prep_inputs-", cfg$wf_args),
  max_attempts = 2,
  compute_resources = list(
    "memory" = "10G",
    "cores" = 2L,
    "runtime" = "600S"
  ),
  shell_path = cfg$shell_path,
  image_path = cfg$image_path,
  script_path = fs::path(code_dir, "00_prep_inputs.R"),

  dir_output = dir_output,
  code_dir = code_dir
)

wf <- jobmonr::add_tasks(wf, list(task_prep_inputs))


# Scale/Rake Onemod -------------------------------------------------------

template_scale_env <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "scale_envelope",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--year_id {year_id}",
    "--dir_output {dir_output}",
    "--code_dir {code_dir}"
  ),
  node_args = list("year_id"),
  task_args = list("dir_output", "code_dir"),
  op_args = list("shell_path", "image_path", "script_path")
)

task_scale_env <- jobmonr::array_tasks(
  task_template = template_scale_env,
  name = paste0("scale_envelope-", cfg$wf_args),
  upstream_tasks = list(task_prep_inputs),
  max_attempts = 2,
  compute_resources = list(
    "memory" = "96G",
    "cores" = 8L,
    "runtime" = "50m"
  ),
  shell_path = cfg$shell_path,
  image_path = cfg$image_path,
  script_path = fs::path(code_dir, "02_scale_results.R"),

  year_id = cfg$years,
  dir_output = dir_output,
  code_dir = code_dir
)

wf <- jobmonr::add_tasks(wf, task_scale_env)


# Plot mx, deaths graphs --------------------------------------------------

template_plot_mx_env <- jobmonr::task_template(
  tool = wf_tool,
  template_name = glue:::glue("plot_mx_env"),
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--code_dir {code_dir}"
  ),
  node_args = list("script_path"),
  task_args = list("dir_output", "code_dir"),
  op_args = list("shell_path", "image_path")
)

task_plot_mx_env <- jobmonr::task(
  task_template = template_plot_mx_env,
  name = paste0("plot_mx_env-", cfg$wf_args),
  upstream_tasks = task_scale_env,
  max_attempts = 2,
  compute_resources = list(
    "memory" = "100G",
    "cores" = 20L,
    "runtime" = "6H"
  ),
  shell_path = cfg$shell_path,
  image_path = cfg$image_path,
  script_path = fs::path(code_dir, "03a_deaths_mx_plots.R"),

  dir_output = dir_output,
  code_dir = code_dir
)

wf <- jobmonr::add_tasks(wf, list(task_plot_mx_env))

task_map_mx_env <- jobmonr::task(
  task_template = template_plot_mx_env,
  name = paste0("map_mx_env-", cfg$wf_args),
  upstream_tasks = task_scale_env,
  max_attempts = 2,
  compute_resources = list(
    "memory" = "60G",
    "cores" = 8L,
    "runtime" = "5H",
    "constraints" = "archive"
  ),
  shell_path = cfg$shell_path,
  image_path = cfg$image_path,
  script_path = fs::path(code_dir, "03c_mx_maps.R"),

  dir_output = dir_output,
  code_dir = code_dir
)

wf <- jobmonr::add_tasks(wf, list(task_map_mx_env))

template_plot_5q0 <- jobmonr::task_template(
  tool = wf_tool,
  template_name = glue:::glue("plot_5q0"),
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--code_dir {code_dir}",
    "--loc {loc}"
  ),
  node_args = list("script_path", "loc"),
  task_args = list("dir_output", "code_dir"),
  op_args = list("shell_path", "image_path")
)

tasks_plot_5q0 <- lapply(
  loc_map[level >= 3]$location_id,
  function(loc) {

    task_plot_5q0 <- jobmonr::task(
      task_template = template_plot_5q0,
      name = paste0("plot_5q0-", cfg$wf_args, "_", loc),
      upstream_tasks = task_scale_env,
      max_attempts = 2,
      compute_resources = list(
        "memory" = "4G",
        "cores" = 2L,
        "runtime" = "6H"
      ),
      shell_path = cfg$shell_path,
      image_path = cfg$image_path,
      script_path = fs::path(code_dir, "03b_plot_like_5q0.R"),
      loc = loc,
      dir_output = dir_output,
      code_dir = code_dir
    )

    return (task_plot_5q0)

  }
)

wf <- jobmonr::add_tasks(wf, tasks_plot_5q0)

task_child_nn_log_mx <- jobmonr::task(
  task_template = template_plot_mx_env,
  name = paste0("scatter_nn_log_mx-", cfg$wf_args),
  upstream_tasks = task_scale_env,
  max_attempts = 2,
  compute_resources = list(
    "memory" = "10G",
    "cores" = 8L,
    "runtime" = "6H"
  ),
  shell_path = cfg$shell_path,
  image_path = cfg$image_path,
  script_path = fs::path(code_dir, "03d_scatter_nn_ages.R"),

  dir_output = dir_output,
  code_dir = code_dir
)

template_misc_graphs <- jobmonr::task_template(
  tool = wf_tool,
  template_name = glue:::glue("misc_graphs"),
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--code_dir {code_dir}"
  ),
  node_args = list("script_path"),
  task_args = list("dir_output", "code_dir"),
  op_args = list("shell_path", "image_path")
)

task_plot_ex <- jobmonr::task(
  task_template = template_misc_graphs,
  name = paste0("plot_ex-", cfg$wf_args),
  upstream_tasks = task_scale_env,
  max_attempts = 2,
  compute_resources = list(
    "memory" = "20G",
    "cores" = 4L,
    "runtime" = "6H"
  ),
  shell_path = cfg$shell_path,
  image_path = cfg$image_path,
  script_path = fs::path(code_dir, "03g_plot_ex_graphs.R"),

  dir_output = dir_output,
  code_dir = code_dir
)

wf <- jobmonr::add_tasks(wf, list(task_plot_ex))

wf <- jobmonr::add_tasks(wf, list(task_child_nn_log_mx))

task_adult_scatter_env <- jobmonr::task(
  task_template = template_misc_graphs,
  name = paste0("adlt_scat_env-", cfg$wf_args),
  upstream_tasks = task_scale_env,
  max_attempts = 2,
  compute_resources = list(
    "memory" = "20G",
    "cores" = 4L,
    "runtime" = "6H"
  ),
  shell_path = cfg$shell_path,
  image_path = cfg$image_path,
  script_path = fs::path(code_dir, "03e_scatter_35q15_30q50.R"),

  dir_output = dir_output,
  code_dir = code_dir
)

wf <- jobmonr::add_tasks(wf, list(task_adult_scatter_env))

task_child_scatter_env <- jobmonr::task(
  task_template = template_misc_graphs,
  name = paste0("child_scat_env-", cfg$wf_args),
  upstream_tasks = task_scale_env,
  max_attempts = 2,
  compute_resources = list(
    "memory" = "10G",
    "cores" = 8L,
    "runtime" = "6H"
  ),
  shell_path = cfg$shell_path,
  image_path = cfg$image_path,
  script_path = fs::path(code_dir, "03f_scatter_u5.R"),

  dir_output = dir_output,
  code_dir = code_dir
)

wf <- jobmonr::add_tasks(wf, list(task_child_scatter_env))

template_plot_45q15 <- jobmonr::task_template(
  tool = wf_tool,
  template_name = glue:::glue("plot_45q15"),
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--dir_output {dir_output}",
    "--code_dir {code_dir}",
    "--loc {loc}"
  ),
  node_args = list("script_path", "loc"),
  task_args = list("dir_output", "code_dir"),
  op_args = list("shell_path", "image_path")
)

tasks_plot_45q15 <- lapply(
  loc_map[level >= 3]$location_id,
  function(loc) {

    task_plot_45q15 <- jobmonr::task(
      task_template = template_plot_45q15,
      name = paste0("plot_45q15-", cfg$wf_args, "_", loc),
      upstream_tasks = task_scale_env,
      max_attempts = 2,
      compute_resources = list(
        "memory" = "4G",
        "cores" = 2L,
        "runtime" = "6H"
      ),
      shell_path = cfg$shell_path,
      image_path = cfg$image_path,
      script_path = fs::path(code_dir, "03h_plot_45q15.R"),
      loc = loc,
      dir_output = dir_output,
      code_dir = code_dir
    )

    return (task_plot_45q15)

  }
)

wf <- jobmonr::add_tasks(wf, tasks_plot_45q15)


# Run Workflow ------------------------------------------------------------

wfr <- wf$run(resume = TRUE)

if (wfr == "D") {

  message("Workflow Completed!")

} else {

  stop("An issue occurred.")

}

# Combine 5q0 plots ------------------------------------------------------------

graph_loc_ids <-
  loc_map[order(sort_order)] |>
  _[level >= 3, location_id]

pdftools::pdf_combine(
  paste0(
    dir_output, "/diagnostics/u5/plot_5q0_style/mx_", graph_loc_ids, ".pdf"
  ),
  glue::glue("{dir_output}/diagnostics/u5/plot_5q0_style_mx.pdf")
)

pdftools::pdf_combine(
  paste0(
    dir_output, "/diagnostics/u5/plot_5q0_style/qx_", graph_loc_ids, ".pdf"
  ),
  glue::glue("{dir_output}/diagnostics/u5/plot_5q0_style_qx.pdf")
)


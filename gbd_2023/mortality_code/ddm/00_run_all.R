################################################################################
##
## Death Distribution Methods (DDM) Run All
##
## Description: Runs all steps of the DDM pipeline including:
## 1. DDM pre-5q0
## 2. DDM mid-5q0
## 3. DDM post-5q0
##
################################################################################

## Settings --------------------------------------------------------------------

library(argparse)
library(data.table)
library(jobmonr)
library(mortdb)

## Read in arguments and inputs ------------------------------------------------

# Load arguments from mortpipeline run-all
if(!interactive()) {

  parser <- argparse::ArgumentParser()
  parser$add_argument("--version_id", type = "integer", required = TRUE,
                      help = "DDM version id")
  parser$add_argument("--username", type = "character", required = TRUE,
                      help = "user conducting model run")
  parser$add_argument("--gbd_year", type = "integer", required = TRUE,
                      help = "GBD year")
  parser$add_argument("--end_year", type = "integer", required = TRUE,
                      help = "last year to make estimates for")
  parser$add_argument("--ddm_step", type = "integer", required = TRUE,
                      help = "numeric DDM step in relation to 5q0")
  parser$add_argument("--mark_best", type = "character", required = TRUE,
                      default = FALSE,
                      help = "TRUE/FALSE mark run as best")
  parser$add_argument("--code_dir", type = "character", required = TRUE,
                      help = "directory where code is cloned")
  parser$add_argument("--workflow_args", type = "character", required = TRUE,
                      action = "store", help = "workflow name")

  args <- parser$parse_args()
  list2env(args, .GlobalEnv)

} else {

  version_id <-
  username <- Sys.getenv("USER")
  gbd_year <-
  end_year <-
  ddm_step <-
  mark_best <-
  workflow_args <- ""

  code_dir <- glue::glue(
    "FILEPATH"
  )

}

# Make sure DDM step is in 0-2
checkmate::assert_choice(ddm_step, c(0:2))

# Update code_dir
code_dir <- fs::path("FILEPATH")

# Load mortality production config file
configs_dir <- fs::path("FILEPATH")
config <- config::get(
  file = fs::path("FILEPATH"),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

# Pull lineage
ddm_lineage <- get_proc_lineage(
  "ddm",
  "estimate",
  run_id = version_id
)

pop_est_version_id <- as.integer(ddm_lineage[
  parent_process_name == "population estimate", parent_run_id
])
emp_pop_version_id <- as.integer(ddm_lineage[
  parent_process_name == "population empirical data", parent_run_id
])
emp_deaths_version_id <- as.integer(ddm_lineage[
  parent_process_name == "death number empirical data", parent_run_id
])
asfr_version_id <- as.integer(ddm_lineage[
  parent_process_name == "asfr estimate", parent_run_id
])
srb_version_id <- as.integer(ddm_lineage[
  parent_process_name == "birth sex ratio estimate", parent_run_id
])
pop_sy_version_id <- as.integer(ddm_lineage[
  parent_process_name == "population single year estimate", parent_run_id
])

if(ddm_step == 0L) ddm_post_5q0 <- 0L
if(ddm_step == 2L) {

  ddm_post_5q0 <- 1L
  data_5q0_version_id <- ddm_lineage[
    parent_process_name == "5q0 data", parent_run_id
  ]
  est_5q0_version_id <- ddm_lineage[
    parent_process_name == "5q0 estimate", parent_run_id
  ]

}

# Set file paths
main_dir <- fs::path("FILEPATH")
outdir <- fs::path("FILEPATH")
indir <- fs::path("FILEPATH")
function_dir <- fs::path("FILEPATH")

# Create file paths
dir.create(fs::path(main_dir))
lapply(c("data", "inputs", "archive", "diagnostics", "graphs", "upload"),
       function(x) dir.create(fs::path("FILEPATH")))
dir.create(fs::path("FILEPATH"))
lapply(c("dropped_deaths", "dropped_pop"),
       function(x) dir.create(fs::path("FILEPATH")))

# Save location map
loc_map <- get_locations(gbd_type = "ap_old", gbd_year = gbd_year)
readr::write_csv(
  loc_map,
  glue::glue("FILEPATH")
)

locs <- unique(loc_map[, location_id])

locs <- tidyr::crossing(location_id = locs, with_shock = 0:1)
setDT(locs)

## Create Jobmon workflow, task templates, and tasks ---------------------------

# Create workflow
if(ddm_step == 0L) {

  wf_name <- "ddm_pre_5q0"

} else if(ddm_step == 1L) {

  wf_name <- "ddm_mid_5q0"

} else if(ddm_step == 2L) {

  wf_name <- "ddm_post_5q0"

}

wf_tool <- tool(name = wf_name)

set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list("project" = submission_project_name,
                   "constraints" = "archive")
)

# Create generic task templates
ddm_ver_template <- task_template(
  tool = wf_tool,
  template_name = "ddm_generic",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path"),
  task_args = list(),
  node_args = list("script_path", "version_id")
)

ddm_shock_ver_template <- task_template(
  tool = wf_tool,
  template_name = "ddm_generic_wshock",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path"),
  task_args = list(),
  node_args = list("script_path", "version_id", "with_shock")
)

ddm_dir_wshock_template <- task_template(
  tool = wf_tool,
  template_name = "ddm_generic_dir",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path"),
  task_args = list("main_dir"),
  node_args = list("script_path", "with_shock")
)

ddm_yr_template <- task_template(
  tool = wf_tool,
  template_name = "ddm_generic_yr",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path"),
  task_args = list(),
  node_args = list("script_path", "version_id", "gbd_year", "with_shock")
)

ddm_stata_template <- task_template(
  tool = wf_tool,
  template_name = "ddm_generic_yr_stata",
  command_template = paste(
    ""
  ),
  op_args = list("script_path"),
  task_args = list(),
  node_args = list("code_path", "kwargs")
)

## Pre 5q0 ---------------------------------------------------------------------

if(ddm_step == 0L) {

  # Create task templates
  d00_population_template <- task_template(
    tool = wf_tool,
    template_name = "d00_population",
    command_template = paste(
      ""
    ),
    op_args = list("shell_path", "script_path", "image_path"),
    task_args = list("code_dir"),
    node_args = list("version_id", "empirical_population_version", "gbd_year",
                     "population_estimate_version")
  )

  d00_deaths_template <- task_template(
    tool = wf_tool,
    template_name = "d00_deaths",
    command_template = paste(
      ""
    ),
    op_args = list("shell_path", "script_path", "image_path"),
    task_args = list(),
    node_args = list("version_id", "empirical_deaths_version", "gbd_year",
                     "with_shock")
  )

  input_pre_5q0_template <- task_template(
    tool = wf_tool,
    template_name = "input_prep",
    command_template = paste(
      ""
    ),
    op_args = list("shell_path", "script_path", "image_path"),
    task_args = list(),
    node_args = list(
      "version_id", "ddm_post_5q0",
      "population_estimate_version", "gbd_year"
    )
  )


  # Create tasks
  wf <- workflow(
    tool = wf_tool,
    workflow_args = paste0("ddm_pre_5q0_", workflow_args)
  )

  wf$workflow_attributes = list()

  d00_population_task <- task(
    task_template = d00_population_template,
    name = "d00_population",
    upstream_tasks = list(),
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = run_all_image_path,
    script_path = fs::path("FILEPATH"),
    version_id = version_id,
    empirical_population_version = emp_pop_version_id,
    population_estimate_version = pop_est_version_id,
    gbd_year = gbd_year,
    code_dir = code_dir,
    compute_resources = list(
      ""
    )
  )

  wf <- add_tasks(workflow = wf, tasks = list(d00_population_task))

  d00_deaths_tasks <- lapply(0:1, function(i) {

    d00_death_task <- task(
      task_template = d00_deaths_template,
      name = "d00_deaths",
      upstream_tasks = list(d00_population_task),
      max_attempts = 3,
      shell_path = shell_path_r,
      image_path = run_all_image_path,
      script_path = fs::path("FILEPATH"),
      version_id = version_id,
      empirical_deaths_version = emp_deaths_version_id,
      gbd_year = gbd_year,
      with_shock = i,
      compute_resources = list(
        ""
      )
    )

    return(d00_death_task)

  })

  wf <- add_tasks(workflow = wf, tasks = d00_deaths_tasks)

  hyperparam_task <- task(
    task_template = ddm_ver_template,
    name = "hyperparameters",
    upstream_tasks = list(),
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = run_all_image_path,
    script_path = fs::path("FILEPATH"),
    version_id = version_id,
    compute_resources = list(
      ""
    )
  )

  wf <- add_tasks(workflow = wf, tasks = list(hyperparam_task))

  vr_both_sex_only_tasks <- lapply(0:1, function (i) {

    vr_both_sex_only_task <- task(
      task_template = ddm_shock_ver_template,
      name = "vr_both_sex_only",
      upstream_tasks = d00_deaths_tasks,
      max_attempts = 3,
      shell_path = shell_path_r,
      image_path = run_all_image_path,
      script_path = fs::path("FILEPATH"),
      version_id = version_id,
      with_shock = i,
      compute_resources = list(
        ""
      )
    )

    return(vr_both_sex_only_task)

  })

  wf <- add_tasks(workflow = wf, tasks = vr_both_sex_only_tasks)

  sex_ratio_tasks <- lapply(0:1, function (i) {

    sex_ratio_task <- task(
      task_template = ddm_yr_template,
      name = "sex_ratio",
      upstream_tasks = vr_both_sex_only_tasks,
      max_attempts = 3,
      shell_path = shell_path_r,
      image_path = run_all_image_path,
      script_path = fs::path("FILEPATH"),
      version_id = version_id,
      gbd_year = gbd_year,
      with_shock = i,
      compute_resources = list(
        ""
      )
    )

    return(sex_ratio_task)

  })

  wf <- add_tasks(workflow = wf, tasks = sex_ratio_tasks)

  input_pre_5q0_task <- task(
    task_template = input_pre_5q0_template,
    name = "input_prep",
    upstream_tasks = list(),
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = run_all_image_path,
    script_path = fs::path("FILEPATH"),
    version_id = version_id,
    ddm_post_5q0 = ddm_post_5q0,
    population_estimate_version = pop_est_version_id,
    gbd_year = gbd_year,
    compute_resources = list(
      ""
    )
  )

  wf <- add_tasks(workflow = wf, tasks = list(input_pre_5q0_task))

  c01_format_population_and_deaths_tasks <- lapply(1:nrow(locs), function(i) {

    c01_format_population_and_deaths_task <- task(
      task_template = ddm_stata_template,
      name = "c01",
      upstream_tasks = append(d00_deaths_tasks, d00_population_task),
      max_attempts = 3,
      script_path = script_path_stata,
      code_path = fs::path("FILEPATH"),
      kwargs = paste(locs[i, location_id], locs[i, with_shock], version_id, gbd_year, code_dir, sep = ","),
      compute_resources = list(
        ""
      )
    )

    return(c01_format_population_and_deaths_task)

  })

  wf <- add_tasks(workflow = wf,
                  tasks = c01_format_population_and_deaths_tasks)

  c01b_tasks <- lapply(c("d01_formatted_deaths", "d01_formatted_deaths_wshock", "d01_formatted_population"), function(file_name) {

    c01b_task <- task(
      task_template = ddm_stata_template,
      name = "c01b",
      upstream_tasks = c01_format_population_and_deaths_tasks,
      max_attempts = 3,
      script_path = script_path_stata,
      code_path = fs::path("FILEPATH"),
      kwargs = paste(version_id, file_name, sep = ","),
      compute_resources = list(
        ""
      )
    )

    return(c01b_task)

  })

  wf <- add_tasks(workflow = wf, tasks = c01b_tasks)

  c09_format_denominators_task <- task(
    task_template = ddm_stata_template,
    name = "c09",
    upstream_tasks = c01b_tasks,
    max_attempts = 3,
    script_path = script_path_stata,
    code_path = fs::path("FILEPATH"),
    kwargs = paste(version_id, gbd_year, sep = ","),
    compute_resources = list(
      ""
    )
  )

  wf <- add_tasks(workflow = wf,
                  tasks = list(c09_format_denominators_task))

}

## Mid 5q0 ---------------------------------------------------------------------

if(ddm_step == 1L) {

  # Create templates
  c04b_ccmp_template <- task_template(
    tool = wf_tool,
    template_name = "c04b_ccmp",
    command_template = paste(
      ""
    ),
    op_args = list("shell_path", "script_path", "image_path"),
    task_args = list("code_dir"),
    node_args = list("version_id", "gbd_year", "pop_sy_version_id",
                     "asfr_version_id", "srb_version_id", "with_shock")
  )

  # Create tasks
  wf <- workflow(
    tool = wf_tool,
    workflow_args = paste0("ddm_mid_5q0_", workflow_args)
  )

  wf$workflow_attributes = list()

  c02_tasks <- lapply(1:nrow(locs), function(i) {
    c02_task <- task(
      task_template = ddm_stata_template,
      name = "c02",
      upstream_tasks = list(),
      max_attempts = 3,
      script_path = script_path_stata,
      code_path = fs::path("FILEPATH"),
      kwargs = paste(locs[i, location_id], locs[i, with_shock], version_id, gbd_year, code_dir, sep = ","),
      compute_resources = list(
        ""
      )
    )

    return(c02_task)

  })

  wf <- add_tasks(workflow = wf, tasks = c02_tasks)

  c02b_tasks <- lapply(c("d02_reshaped_deaths", "d02_reshaped_deaths_wshock", "d02_reshaped_population"), function(file_name) {

    c01b_task <- task(
      task_template = ddm_stata_template,
      name = "c02b",
      upstream_tasks = c02_tasks,
      max_attempts = 3,
      script_path = script_path_stata,
      code_path = fs::path("FILEPATH"),
      kwargs = paste(version_id, file_name, sep = ","),
      compute_resources = list(
        ""
      )
    )

    return(c01b_task)

  })

  wf <- add_tasks(workflow = wf, tasks = c02b_tasks)

  c03_tasks <- lapply(1:nrow(locs), function(i) {
    c03_task <- task(
      task_template = ddm_stata_template,
      name = "c03",
      upstream_tasks = c02b_tasks,
      max_attempts = 3,
      script_path = script_path_stata,
      code_path = fs::path("FILEPATH"),
      kwargs = paste(locs[i, location_id], locs[i, with_shock], version_id, gbd_year, code_dir, sep = ","),
      compute_resources = list(
        ""
      )
    )

    return(c03_task)

  })

  wf <- add_tasks(workflow = wf, tasks = c03_tasks)

  c03b_tasks <- lapply(
    c("d03_combined_population_and_deaths", "d03_combined_population_and_deaths_wshock"),
    function(file_name) {

      c03b_task <- task(
        task_template = ddm_stata_template,
        name = "c03b",
        upstream_tasks = c03_tasks,
        max_attempts = 3,
        script_path = script_path_stata,
        code_path = fs::path("FILEPATH"),
        kwargs = paste(version_id, file_name, sep = ","),
        compute_resources = list(
          ""
        )
      )

      return(c03b_task)

    })

  wf <- add_tasks(workflow = wf, tasks = c03b_tasks)

  c04a_tasks <- lapply(0:1, function (i) {

    c04a_task <- task(
      task_template = ddm_dir_wshock_template,
      name = "c04a",
      upstream_task = c03b_tasks,
      max_attempts = 3,
      shell_path = shell_path_r,
      image_path = run_all_image_path,
      script_path = fs::path("FILEPATH"),
      main_dir = main_dir,
      with_shock = i,
      compute_resources = list(
        ""
      )
    )

    return(c04a_task)

  })

  wf <- add_tasks(workflow = wf, tasks = c04a_tasks)

  c04b_tasks <- lapply(0:1, function (i) {

    c04b_task <- task(
      task_template = c04b_ccmp_template,
      name = "c04b",
      upstream_tasks = c03_tasks,
      max_attempts = 3,
      shell_path = shell_path_r,
      image_path = run_all_image_path,
      script_path = fs::path("FILEPATH"),
      version_id = version_id,
      gbd_year = gbd_year,
      pop_sy_version_id = pop_sy_version_id,
      asfr_version_id = asfr_version_id,
      srb_version_id = srb_version_id,
      code_dir = code_dir,
      with_shock = i,
      compute_resources = list(
        ""
      )
    )

    return(c04b_task)

  })

  wf <- add_tasks(workflow = wf, tasks = c04b_tasks)

}

## Post 5q0 --------------------------------------------------------------------

if(ddm_step == 2L) {

  # Create templates
  input_post_5q0_template <- task_template(
    tool = wf_tool,
    template_name = "input_prep",
    command_template = paste(
      ""
    ),
    op_args = list("shell_path", "script_path", "image_path"),
    task_args = list(),
    node_args = list(
      "version_id", "ddm_post_5q0", "gbd_year",
      "data_5q0_version_id", "estimate_5q0_version_id"
    )
  )

  c08_template <- task_template(
    tool = wf_tool,
    template_name = "c08",
    command_template = paste(
      ""
    ),
    op_args = list("shell_path", "script_path", "image_path"),
    task_args = list("code_dir"),
    node_args = list("version_id", "gbd_year", "end_year", "with_shock")
  )

  upload_template <- task_template(
    tool = wf_tool,
    template_name = "upload",
    command_template = paste(
      ""
    ),
    op_args = list("shell_path", "image_path"),
    task_args = list(),
    node_args = list("script_path", "version_id", "gbd_year", "mark_best")
  )

  # Create tasks
  wf <- workflow(
    tool = wf_tool,
    workflow_args = paste0("ddm_post_5q0_", workflow_args)
  )

  wf$workflow_attributes = list()

  input_prep_task <- task(
    task_template = input_post_5q0_template,
    name = "input_prep",
    upstream_tasks = list(),
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = run_all_image_path,
    script_path = fs::path("FILEPATH"),
    version_id = version_id,
    ddm_post_5q0 = ddm_post_5q0,
    gbd_year = gbd_year,
    data_5q0_version_id = data_5q0_version_id,
    estimate_5q0_version_id = est_5q0_version_id,
    compute_resources = list(
      ""
    )
  )

  wf <- add_tasks(workflow = wf, tasks = list(input_prep_task))

  c06_task <- task(
    task_template = ddm_stata_template,
    name = "c06",
    upstream_tasks = list(input_prep_task),
    max_attempts = 3,
    script_path = script_path_stata,
    code_path = fs::path("FILEPATH"),
    kwargs = paste(version_id, gbd_year, sep = ","),
    compute_resources = list(
      ""
    )
  )

  wf <- add_tasks(workflow = wf, tasks = list(c06_task))

  c07_tasks <- lapply(0:1, function (i) {

    c07_task <- task(
      task_template = ddm_stata_template,
      name = "c07",
      upstream_tasks = list(input_prep_task, c06_task),
      max_attempts = 3,
      script_path = script_path_stata,
      code_path = fs::path("FILEPATH"),
      kwargs = paste(version_id, i, sep = ","),
      compute_resources = list(
        ""
      )
    )

    return(c07_task)

  })

  wf <- add_tasks(workflow = wf, tasks = c07_tasks)

  c08_tasks <- lapply(0:1, function (i) {

    c08_task <- task(
      task_template = c08_template,
      name = "c08",
      upstream_tasks = c07_tasks,
      max_attempts = 3,
      shell_path = shell_path_r,
      image_path = run_all_image_path,
      script_path = fs::path("FILEPATH"),
      version_id = version_id,
      gbd_year = gbd_year,
      end_year = end_year,
      code_dir = code_dir,
      with_shock = i,
      compute_resources = list(
        ""
      )
    )

    return(c08_task)

  })

  wf <- add_tasks(workflow = wf, tasks = c08_tasks)

  c09_replacement_task <- task(
    task_template = ddm_ver_template,
    name = "c09_replace",
    upstream_task = c08_tasks,
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = run_all_image_path,
    script_path = fs::path("FILEPATH"),
    version_id = version_id,
    compute_resources = list(
      ""
    )
  )

  wf <- add_tasks(workflow = wf, tasks = list(c09_replacement_task))

  c10_tasks <- lapply(0:1, function (i) {

    c10_task <- task(
      task_template = ddm_stata_template,
      name = "c10",
      upstream_tasks = c(c08_tasks, c09_replacement_task),
      max_attempts = 3,
      script_path = script_path_stata,
      code_path = fs::path("FILEPATH"),
      kwargs = paste(version_id, i, sep = ","),
      compute_resources = list(
        ""
      )
    )

    return(c10_task)

  })

  wf <- add_tasks(workflow = wf, tasks = c10_tasks)

  upload_ddm_data_task <- task(
    task_template = upload_template,
    name = "upload_data",
    upstream_tasks = c10_tasks,
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = run_all_image_path,
    script_path = fs::path("FILEPATH"),
    version_id = version_id,
    gbd_year = gbd_year,
    mark_best = mark_best,
    compute_resources = list(
      ""
    )
  )

  wf <- add_tasks(workflow = wf, tasks = list(upload_ddm_data_task))

  upload_ddm_estimate_task <- task(
    task_template = upload_template,
    name = "upload_estimate",
    upstream_tasks = c10_tasks,
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = run_all_image_path,
    script_path = fs::path("FILEPATH"),
    version_id = version_id,
    gbd_year = gbd_year,
    mark_best = mark_best,
    compute_resources = list(
      ""
    )
  )

  wf <- add_tasks(workflow = wf, tasks = list(upload_ddm_estimate_task))

}

## Run -------------------------------------------------------------------------

wfr <- wf$run(resume = TRUE)

if(wfr != "D") {

  # Send failure notification
  send_slack_message(
    message = paste0(""),
    channel = paste0(""),
    icon = "",
    botname = ""
  )

  # Stop process
  stop("DDM failed!")

} else {

  message("DDM completed!")

}

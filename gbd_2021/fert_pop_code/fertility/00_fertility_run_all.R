# Meta --------------------------------------------------------------------

# Description: Submit the jobmon workflow that launches all steps for
# asfr/tfr model.
# Steps: (Short summary of each general step)
#   1. Create jobmon workflow, task templates and tasks.
#   2. Send update slack message.
#   3. Run jobmon workflow.
#   4. Collect and save jobmon error logs.
# Inputs:
#   * Best asfr data
# Outputs:
#   * {base_dir}/jobmon_logs_{workflow_id}.csv: jobmon error logs.
#   *{base_dir}/loop2/gpr/compiled_gpr_results.csv
#
# NOTE: ASFR is not usually uploaded. Uploads only happen in terminator

# Load libraries ----------------------------------------------------------

library(argparse)
library(data.table)
library(demInternal)
library(fs)
library(jobmonr)
library(mortdb)
library(readr)

# Set version arguments --------------------------------------------------

# Jobmon arguments
## Load mortality production config file
username <- Sys.getenv("USER")
config <- config::get(
  file = fs::path("FILEPATH"),
  use_parent = FALSE
)

list2env(config, .GlobalEnv)

# Generate new version
comment <- "run description here"
branch <- "main"
workflow_args <- format(Sys.time(), "%Y%m%d%H%M")
gen_new <- FALSE
old_young_stgpr <- FALSE # T = use ST/GPR, F = use old old-young method

# If resuming update workflow_args
clone_dir <- paste0("FILEPATH")

if (fs::dir_exists(clone_dir)) {
  
  workflow_args <- fread(fs::path(clone_dir, "resume_file.csv"))
  workflow_args <- workflow_args$workflow_args
  
}

# Check comment
if (comment == "run description here") stop("Write an informative run comment.")

# Set round arguments
gbd_year <- x
gbd_round_id <- get_gbd_round(gbd_year = gbd_year)
year_start <- x
year_end <- x
decomp_step <- ""

# Set model
if (gen_new) {
  
  version_id <- gen_new_version(
    "asfr", 
    "estimate",
    comment = comment,
    gbd_year = gbd_year
  )
  
} else {
  
  version_id <- get_proc_version("asfr", "estimate", run_id = "recent")
  
}

split_version <- x
model_locs <- "all"

# Overwrite some args
retries <- 3
submission_project_name <- "PROJECT"
command_template_add_ons <- "PYTHONPATH= PATH=FILEPATH:$PATH"
shell_path_r <- gsub("PYTHONPATH= ", "", shell_path_r)

# Set parent versions
pop_est_id <- x # baseline
pop_syr_id <- x # baseline
ddm_est_id <- as.integer(
  get_versions(
    "ddm estimate",
    run_status = "best",
    gbd_year = get_gbd_year(gbd_round_id - 1))[, run_id]
)
birth_est_id <- as.integer(
  get_versions(
    "birth estimate",
    run_status = "best",
    gbd_year = get_gbd_year(gbd_round_id - 1))[, run_id]
)


# plot comparison versions
version1 <- version_id
version2 <- x # current best
version3 <- x # gbd 2019 best

# Send slack message
send_slack_message(message=paste0("*Fertility run started: version ",
                                  version_id, "* \n", comment),
                   channel="#f",
                   botname="FertBot", icon=":egg:")

# Create clones ----------------------------------------------------------------

if (!dir.exists(clone_dir)) {
  
  fs::dir_create(clone_dir)
  
  git_clone(clone_dir, stash_repos = c("WEBSITE" = branch))
  
  fs::file_copy(paste0("FILEPATH/00_fertility_run_all.R"),
                paste0(clone_dir, "/fert_pipeline_run_all.R"))
  
  # Open permissions so shell scripts can access py scripts
  cloned_files <- list.files(clone_dir, full.names = TRUE, recursive = TRUE)
  Sys.chmod(cloned_files, mode = "0777", use_umask = TRUE)
  
}

# Set filepaths ----------------------------------------------------------------

code_dir <- paste0("FILEPATH")
base_dir <- paste0("FILEPATH")
graph_dir <- paste0(gsub("FILEPATH"))

# Create subdirectories
if (!dir.exists(base_dir)) fs::dir_create(base_dir, mode = "u=rwx,go=rwx")

if (!dir.exists(graph_dir)) {
  
  fs::dir_create(graph_dir, mode = "u=rwx,go=rwx")
  fs::dir_create(paste0(graph_dir, "/loc_specific"), mode = "u=rwx,go=rwx")
  
}

for (loop in 1:2){
  
  new_dir <- paste0("FILEPATH")
  
  if (!dir.exists(new_dir)) {
    
    fs::dir_create(new_dir, mode = "u=rwx,go=rwx")
    input_dir <- paste0("FILEPATH")
    fs::dir_create(input_dir, mode = "u=rwx,go=rwx")
    stage1_dir <- paste0("FILEPATH")
    fs::dir_create(stage1_dir, mode = "u=rwx,go=rwx")
    stage2_dir <- paste0("FILEPATH")
    fs::dir_create(stage2_dir, mode = "u=rwx,go=rwx")
    gpr_dir <- paste0("FILEPATH")
    fs::dir_create(gpr_dir, mode = "u=rwx,go=rwx")
    unraked_dir <- paste0("FILEPATH")
    fs::dir_create(unraked_dir, mode = "u=rwx,go=rwx")
    param_dir <- paste0("FILEPATH")
    fs::dir_create(param_dir, mode = "u=rwx,go=rwx")
    
  }
  
}

# Save workflow arguments for resuming
resume_file <- fs::path("FILEPATH")

if (!fs::file_exists(resume_file)) {
  
  wf_df <- data.table("workflow_args" = workflow_args)
  readr::write_csv(wf_df, file = resume_file)
  
}

# Prep inputs ------------------------------------------------------------------

# Get locations
all_locs <- mortdb::get_locations(
  gbd_type = "mortality",
  level = "estimate", 
  gbd_year = gbd_year
)

if (model_locs == "all") {
  
  model_locs <- all_locs
  
} else {
  
  ## Need all parents and subnationals for model_locs
  model_locs <- gsub("[^A-Z|,]", "", model_locs)
  model_locs <- all_locs[ihme_loc_id %in% model_locs]
  
}

readr::write_csv(model_locs, fs::path("FILEPATH"))

# List parent locations
parents <- model_locs[grepl("_", ihme_loc_id), parent_id]
parents <- model_locs[location_id %in% parents]
parents <- parents[level == 3]
parents <- parents[, ihme_loc_id]
parents <- c("CHN", parents)

# List super-regions
## Correspond to: FILEPATH/super_regs.csv
## NOTE: the "super_regs" here are not the normal IHME super regions.
## We use bigger regions to fit the fertility model
super_regs <- 1:4

# Get SBH and TB locations
best <- get_best_versions(
  model_name = "asfr data", 
  gbd_year = gbd_year
)
data <- get_mort_outputs(
  model_name = "asfr", 
  model_type = "data", 
  gbd_year = gbd_year, 
  run_id = best, 
  outlier_run_id = "active"
)

sbh_locs <- unique(
  data[
    dem_measure_id == 5 & !is.na(ihme_loc_id) & ihme_loc_id %in% model_locs$ihme_loc_id,
    ihme_loc_id
  ]
)

tb_locs <- unique(
  data[
    dem_measure_id == 4 & !is.na(ihme_loc_id) & ihme_loc_id %in% model_locs$ihme_loc_id,
    ihme_loc_id
  ]
)

## Create Jobmon workflow, task templates, and tasks ---------------------------

# Create new workflow
wf_tool <- tool(name = "asfr")

wf_tool <- set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list("project" = submission_project_name)
)

wf <- workflow(
  tool = wf_tool,
  workflow_args = paste0("asfr_", workflow_args)
)

wf$workflow_attributes <- list()

# Setup task
for(loop in 1:2) {
  
  loop <- as.integer(loop)
  
  if (loop == 1) { hold_tasks <- list() 
  } else { 
    hold_tasks <- c(sbh_tasks, tb_tasks, hist_task) }
  
  # Run Input data step
  inputs_template <- task_template(
    tool = wf_tool,
    template_name = "inputs",
    command_template = paste(
      command_template_add_ons,
      "{shell_path} -i {image_path} -s {script_path}",
      "--version_id {version_id}",
      "--gbd_year {gbd_year}",
      "--year_start {year_start}",
      "--year_end {year_end}",
      "--loop {loop}",
      "--decomp_step {decomp_step}",
      "--split_version {split_version}",
      "--pop_est_id {pop_est_id}",
      "--pop_syr_id {pop_syr_id}",
      "--ddm_est_id {ddm_est_id}",
      "--birth_est_id {birth_est_id}",
      "--code_dir {code_dir}"
    ),
    op_args = list("shell_path", "image_path", "script_path"),
    task_args = list(),
    node_args = list("version_id", "gbd_year", "year_start", "year_end", "loop",
                     "decomp_step", "split_version", "pop_est_id",
                     "pop_syr_id", "ddm_est_id", "birth_est_id", "code_dir")
  )
  
  inputs_task <- task(
    task_template = inputs_template,
    name = paste0("inputs_", loop),
    upstream_tasks = hold_tasks,
    max_attempts = retries,
    shell_path = shell_path_r,
    image_path = image_path,
    script_path = glue::glue("{code_dir}/00_input_data.R"),
    version_id = version_id,
    gbd_year = gbd_year,
    year_start = year_start,
    year_end = year_end,
    loop = loop,
    decomp_step = decomp_step,
    split_version = split_version,
    pop_est_id = pop_est_id,
    pop_syr_id = pop_syr_id,
    ddm_est_id = ddm_est_id,
    birth_est_id = birth_est_id,
    code_dir = code_dir,
    compute_resources = list(
      "memory" = "20G",
      "cores" = 1L,
      "queue" = queue,
      "constraints" = "archive",
      "runtime" = "1800S"
    )
  )
  
  wf <- add_tasks(workflow = wf, tasks = list(inputs_task))
  
  # for 01b (i.e. using ST/GPR on old/young ages) need to loop through 1:3
  # for 06a/06b loop through 1:2
  if (old_young_stgpr) { end_part <- 3 } else { end_part <- 2 }
  
  for(part in 1:end_part) {
    
    # Determine ages and locations for task
    if (part == 1) {model_ages <- 20L} 
    if (part == 2) {model_ages <- as.integer(c(15, 25, 30, 35, 40, 45))}
    if (part == 3 & loop == 1) {model_ages <- as.integer(c(10, 50))}
    
    if (part == 1) {locs <- all_locs$ihme_loc_id} else {locs <- model_locs$ihme_loc_id}
    
    # Run first stage
    if (part == 1) {hold_tasks <- list(inputs_task)} else {hold_tasks <- raking_tasks}
    if (part == 3) {hold_tasks <- c(sbh_tasks, tb_tasks, hist_task)}
    
    s1_template <- task_template(
      tool = wf_tool,
      template_name = "s1",
      command_template = paste(
        command_template_add_ons,
        "{shell_path} -i {image_path} -s {script_path}",
        "--version_id {version_id}",
        "--gbd_year {gbd_year}",
        "--year_start {year_start}",
        "--year_end {year_end}",
        "--loop {loop}",
        "--model_age {model_age}",
        "--super_reg {super_reg}"
      ),
      op_args = list("shell_path", "image_path", "script_path"),
      task_args = list(),
      node_args = list("version_id", "gbd_year", "year_start", "year_end", 
                       "loop", "model_age", "super_reg")
    )
    
    s1_tasks <- lapply(model_ages, function(age) {
      
      lapply(super_regs, function(super_reg) {
        
        s1_task <- task(
          task_template = s1_template,
          name = paste0("s1_", loop, "_", part),
          upstream_tasks = hold_tasks,
          max_attempts = retries,
          shell_path = shell_path_r,
          image_path = image_path,
          script_path = glue::glue("{code_dir}/01_fit_first_stage.R"),
          version_id = version_id,
          gbd_year = gbd_year,
          year_start = year_start,
          year_end = year_end,
          loop = loop,
          model_age = age,
          super_reg = super_reg,
          compute_resources = list(
            "memory" = "20G",
            "cores" = 1L,
            "queue" = queue,
            "constraints" = "archive",
            "runtime" = "1800S"
          )
        )
        
        return(s1_task)
        
      })
      
    })
    
    s1_tasks <- lapply(rapply(s1_tasks, enquote, how = "unlist"), eval)
    
    wf <- add_tasks(workflow = wf, tasks = s1_tasks)
    
    # Run Stage 1 old/young
    if (old_young_stgpr) {
      
      s1_oy_template <- task_template(
        tool = wf_tool,
        template_name = "s1_oy",
        command_template = paste(
          command_template_add_ons,
          "{shell_path} -i {image_path} -s {script_path}",
          "--version_id {version_id}",
          "--gbd_year {gbd_year}",
          "--year_start {year_start}",
          "--year_end {year_end}",
          "--loop {loop}",
          "--model_age {model_age}",
        ),
        op_args = list("shell_path", "image_path", "script_path"),
        task_args = list(),
        node_args = list("version_id", "gbd_year", "year_start", "year_end", 
                         "loop", "model_age")
      )
      
      s1_oy_tasks <- lapply(c(10, 50), function(age) {
        
        s1_oy_task <- task(
          task_template = s1_template,
          name = paste0("s1_oy_", loop, "_", part),
          upstream_tasks = s1_tasks,
          max_attempts = retries,
          shell_path = shell_path_r,
          image_path = image_path,
          script_path = glue::glue("{code_dir}/01b_first_stage_old_young.R"),
          version_id = version_id,
          gbd_year = gbd_year,
          year_start = year_start,
          year_end = year_end,
          loop = loop,
          model_age = age,
          compute_resources = list(
            "memory" = "20G",
            "cores" = 1L,
            "queue" = queue,
            "constraints" = "archive",
            "runtime" = "1800S"
          )
        )
        
        return(s1_oy_task)
        
      })
      
      wf <- add_tasks(workflow = wf, tasks = s1_oy_tasks)
      
    } # Close S1 oldest and youngest
    
    # Run Stage 2
    hold_tasks <- s1_tasks
    
    if (old_young_stgpr) {hold_tasks <- c(hold_tasks, s1_oy_task)}
    
    s2_template <- task_template(
      tool = wf_tool,
      template_name = "s2",
      command_template = paste(
        command_template_add_ons,
        "{shell_path} -i {image_path} -s {script_path}",
        "--version_id {version_id}",
        "--gbd_year {gbd_year}",
        "--year_start {year_start}",
        "--year_end {year_end}",
        "--birth_est_id {birth_est_id}",
        "--loop {loop}",
        "--model_age {model_age}",
        "--code_dir {code_dir}"
      ),
      op_args = list("shell_path", "image_path", "script_path"),
      task_args = list(),
      node_args = list("version_id", "gbd_year", "year_start", "year_end", 
                       "loop", "birth_est_id", "model_age", "code_dir")
    )
    
    s2_tasks <- lapply(model_ages, function(age) {
      
      s2_task <- task(
        task_template = s2_template,
        name = paste0("s2_", loop, "_", part),
        upstream_tasks = hold_tasks,
        max_attempts = retries,
        shell_path = shell_path_r,
        image_path = image_path,
        script_path = glue::glue("{code_dir}/02_second_stage.R"),
        version_id = version_id,
        gbd_year = gbd_year,
        year_start = year_start,
        year_end = year_end,
        loop = loop,
        birth_est_id = birth_est_id,
        model_age = age,
        code_dir = code_dir,
        compute_resources = list(
          "memory" = "30G",
          "cores" = 4L,
          "queue" = queue,
          "constraints" = "archive",
          "runtime" = "9000S"
        )
      )
      
      return(s2_task)
      
    })
    
    wf <- add_tasks(workflow = wf, tasks = s2_tasks)
    
    # Run GPR
    gpr_template <- task_template(
      tool = wf_tool,
      template_name = "gpr",
      command_template = paste(
        command_template_add_ons,
        "OMP_NUM_THREADS=1",
        "{script_path} {code_path} {conda_path} {conda_env}",
        "{version_id}",
        "{year_start}",
        "{year_end}",
        "{loop}",
        "{model_age}",
        "{loc}"
      ),
      op_args = list("script_path", "code_path", "conda_path", "conda_env"),
      task_args = list(),
      node_args = list("version_id", "year_start", "year_end", "loop", 
                       "model_age", "loc")
    )
    
    gpr_tasks <- lapply(1:nrow(model_locs), function(loc) {
      
      lapply(model_ages, function(age) {
        
        gpr_task <- task(
          task_template = gpr_template,
          name = paste0("gpr_", loop, "_", part),
          upstream_tasks = s2_tasks,
          max_attempts = 2,
          code_path = glue::glue("{code_dir}/03_gpr.py"),
          conda_path = conda_path,
          conda_env = conda_env,
          script_path = glue::glue("{code_dir}/03_gpr.sh"),
          version_id = version_id,
          year_start = year_start,
          year_end = year_end,
          loop = loop,
          model_age = age,
          loc = model_locs[loc]$ihme_loc_id,
          compute_resources = list(
            "memory" = "1G",
            "cores" = 1L,
            "queue" = queue,
            "constraints" = "archive",
            "runtime" = "1200S"
          )
        )
        
        return(gpr_task)
        
      })
      
    })
    
    gpr_tasks <- lapply(rapply(gpr_tasks, enquote, how = "unlist"), eval)
    
    wf <- add_tasks(workflow = wf, tasks = gpr_tasks)
    
    # Run Raking
    raking_template <- task_template(
      tool = wf_tool,
      template_name = "raking",
      command_template = paste(
        command_template_add_ons,
        "{shell_path} -i {image_path} -s {script_path}",
        "--version_id {version_id}",
        "--parent {parent}",
        "--model_age {model_age}",
        "--gbd_year {gbd_year}",
        "--year_start {year_start}",
        "--year_end {year_end}",
        "--loop {loop}"
      ),
      op_args = list("shell_path", "image_path", "script_path"),
      task_args = list(),
      node_args = list("version_id", "parent", "model_age", "gbd_year", 
                       "year_start", "year_end", "loop")
    )
    
    raking_tasks <- lapply(parents, function(loc) {
      
      lapply(model_ages, function(age) {
        
        raking_task <- task(
          task_template = raking_template,
          name = paste0("raking_", loop, "_", part),
          upstream_tasks = gpr_tasks,
          max_attempts = retries,
          shell_path = shell_path_r,
          image_path = run_all_image_path,
          script_path = glue::glue("{code_dir}/04_rake_agg_by_loc.R"),
          version_id = version_id,
          gbd_year = gbd_year,
          year_start = year_start,
          year_end = year_end,
          model_age = age,
          parent = loc,
          loop = loop,
          compute_resources = list(
            "memory" = "10G",
            "cores" = 1L,
            "queue" = queue,
            "constraints" = "archive",
            "runtime" = "1800S"
          )
        )
        
        return(raking_task)
        
      })
      
    })
    
    raking_tasks <- lapply(rapply(raking_tasks, enquote, how = "unlist"), eval)
    
    wf <- add_tasks(workflow = wf, tasks = raking_tasks)
    
    # Reassign split version
    if (loop == 1 & part == 2) {
      
      if (gen_new) {
        
        split_version <- gen_new_version(
          "asfr",
          "data",
          comment = paste0("v", version_id, " split SBH and TB"),
          gbd_year = gbd_year
        )
        
      } else {
        
        split_version <- get_proc_version(
          "asfr", 
          "data",
          run_id = "recent")
        
      }
      
      # Run split SBH
      sbh_template <- task_template(
        tool = wf_tool,
        template_name = "sbh",
        command_template = paste(
          command_template_add_ons,
          "{shell_path} -i {image_path} -s {script_path}",
          "--version_id {version_id}",
          "--loc {loc}",
          "--gbd_year {gbd_year}",
          "--year_start {year_start}",
          "--year_end {year_end}",
          "--split_version {split_version}"
        ),
        op_args = list("shell_path", "image_path", "script_path"),
        task_args = list(),
        node_args = list("version_id", "loc", "gbd_year", "year_start", 
                         "year_end", "split_version")
      )
      
      sbh_tasks <- lapply(sbh_locs, function(loc) {
        
        sbh_task <- task(
          task_template = sbh_template,
          name = paste0("sbh_", loop, "_", part),
          upstream_tasks = raking_tasks,
          max_attempts = retries,
          shell_path = shell_path_r,
          image_path = image_path,
          script_path = glue::glue("{code_dir}/05a_split_sbh.R"),
          version_id = version_id,
          gbd_year = gbd_year,
          year_start = year_start,
          year_end = year_end,
          loc = loc,
          split_version = split_version,
          compute_resources = list(
            "memory" = "20G",
            "cores" = 1L,
            "queue" = queue,
            "constraints" = "archive",
            "runtime" = "1800S"
          )
        )
        
        return(sbh_task)
        
      })
      
      wf <- add_tasks(workflow = wf, tasks = sbh_tasks)
      
      # Run split TB
      tb_template <- task_template(
        tool = wf_tool,
        template_name = "tb",
        command_template = paste(
          command_template_add_ons,
          "{shell_path} -i {image_path} -s {script_path}",
          "--version_id {version_id}",
          "--loc {loc}",
          "--gbd_year {gbd_year}",
          "--year_start {year_start}",
          "--year_end {year_end}",
          "--split_version {split_version}"
        ),
        op_args = list("shell_path", "image_path", "script_path"),
        task_args = list(),
        node_args = list("version_id", "loc", "gbd_year", "year_start", 
                         "year_end", "split_version")
      )
      
      tb_tasks <- lapply(tb_locs, function(loc) {
        
        tb_task <- task(
          task_template = tb_template,
          name = paste0("tb_", loop, "_", part),
          upstream_tasks = raking_tasks,
          max_attempts = retries,
          shell_path = shell_path_r,
          image_path = image_path,
          script_path = glue::glue("{code_dir}/05b_split_tb.R"),
          version_id = version_id,
          gbd_year = gbd_year,
          year_start = year_start,
          year_end = year_end,
          loc = loc,
          split_version = split_version,
          compute_resources = list(
            "memory" = "20G",
            "cores" = 1L,
            "queue" = queue,
            "constraints" = "archive",
            "runtime" = "1800S"
          )
        )
        
        return(tb_task)
        
      })
      
      wf <- add_tasks(workflow = wf, tasks = tb_tasks)
      
      # Run split historic
      hist_template <- task_template(
        tool = wf_tool,
        template_name = "hist",
        command_template = paste(
          command_template_add_ons,
          "{shell_path} -i {image_path} -s {script_path}",
          "--version_id {version_id}",
          "--loc {loc}",
          "--gbd_year {gbd_year}",
          "--year_start {year_start}",
          "--year_end {year_end}",
          "--split_version {split_version}"
        ),
        op_args = list("shell_path", "image_path", "script_path"),
        task_args = list(),
        node_args = list("version_id", "loc", "gbd_year", "year_start", 
                         "year_end", "split_version")
      )
      
      hist_task <- task(
        task_template = hist_template,
        name = paste0("tb_", loop, "_", part),
        upstream_tasks = raking_tasks,
        max_attempts = retries,
        shell_path = shell_path_r,
        image_path = image_path,
        script_path = glue::glue("{code_dir}/05c_split_historic_locs.R"),
        version_id = version_id,
        gbd_year = gbd_year,
        year_start = year_start,
        year_end = year_end,
        loc = "GBR_4749_4636",
        split_version = split_version,
        compute_resources = list(
          "memory" = "20G",
          "cores" = 1L,
          "queue" = queue,
          "constraints" = "archive",
          "runtime" = "1800S"
        )
      )
      
      wf <- add_tasks(workflow = wf, tasks = list(hist_task))
      
    } # close splitting loop
    
  } # Close "part" loop
  
} # Close "loop" loop

# Run original old-young method
if (!old_young_stgpr) {
  
  # Update model ages
  model_ages <- as.integer(c(10, 50))
  
  # Run fit old-young
  oy_fit_template <- task_template(
    tool = wf_tool,
    template_name = "oy_fit",
    command_template = paste(
      command_template_add_ons,
      "{shell_path} -i {image_path} -s {script_path}",
      "--version_id {version_id}",
      "--gbd_year {gbd_year}",
      "--year_start {year_start}",
      "--year_end {year_end}"
    ),
    op_args = list("shell_path", "image_path", "script_path"),
    task_args = list(),
    node_args = list("version_id", "gbd_year", "year_start", "year_end")
  )
  
  oy_fit_task <- task(
    task_template = oy_fit_template,
    name = "oy_fit",
    upstream_tasks = raking_tasks,
    max_attempts = retries,
    shell_path = shell_path_r,
    image_path = image_path,
    script_path = glue::glue("{code_dir}/06a_fit_old_young.R"),
    version_id = version_id,
    gbd_year = gbd_year,
    year_start = year_start,
    year_end = year_end,
    compute_resources = list(
      "memory" = "20G",
      "cores" = 1L,
      "queue" = queue,
      "constraints" = "archive",
      "runtime" = "1800S"
    )
  )
  
  wf <- add_tasks(workflow = wf, tasks = list(oy_fit_task))
  
  # Run predict old-young
  oy_pred_template <- task_template(
    tool = wf_tool,
    template_name = "oy_pred",
    command_template = paste(
      command_template_add_ons,
      "{shell_path} -i {image_path} -s {script_path}",
      "--version_id {version_id}",
      "--gbd_year {gbd_year}",
      "--year_start {year_start}",
      "--year_end {year_end}",
      "--loc {loc}"
    ),
    op_args = list("shell_path", "image_path", "script_path"),
    task_args = list(),
    node_args = list("version_id", "gbd_year", "year_start", "year_end", "loc")
  )
  
  oy_pred_tasks <- lapply(1:nrow(model_locs), function(loc) {
    
    oy_pred_task <- task(
      task_template = oy_pred_template,
      name = "oy_pred",
      upstream_tasks = list(oy_fit_task),
      max_attempts = retries,
      shell_path = shell_path_r,
      image_path = image_path,
      script_path = glue::glue("{code_dir}/06b_predict_old_young.R"),
      version_id = version_id,
      gbd_year = gbd_year,
      year_start = year_start,
      year_end = year_end,
      loc = model_locs[loc]$ihme_loc_id,
      compute_resources = list(
        "memory" = "20G",
        "cores" = 1L,
        "queue" = queue,
        "constraints" = "archive",
        "runtime" = "1800S"
      )
    )
    
    return(oy_pred_task)
    
  })
  
  wf <- add_tasks(workflow = wf, tasks = oy_pred_tasks)
  
  # Run raking old-young
  oy_raking_template <- task_template(
    tool = wf_tool,
    template_name = "oy_raking",
    command_template = paste(
      command_template_add_ons,
      "{shell_path} -i {image_path} -s {script_path}",
      "--version_id {version_id}",
      "--gbd_year {gbd_year}",
      "--year_start {year_start}",
      "--year_end {year_end}",
      "--model_age {model_age}",
      "--parent {parent}",
      "--loop {loop}"
    ),
    op_args = list("shell_path", "image_path", "script_path"),
    task_args = list(),
    node_args = list("version_id", "gbd_year", "year_start", "year_end",
                     "model_age", "loop", "parent")
  )
  
  oy_raking_tasks <- lapply(parents, function(loc) {
    
    lapply(model_ages, function(age) {
      
      oy_raking_task <- task(
        task_template = oy_raking_template,
        name = "oy_raking",
        upstream_tasks = oy_pred_tasks,
        max_attempts = retries,
        shell_path = shell_path_r,
        image_path = run_all_image_path,
        script_path = glue::glue("{code_dir}/04_rake_agg_by_loc.R"),
        version_id = version_id,
        gbd_year = gbd_year,
        year_start = year_start,
        year_end = year_end,
        loop = 2L,
        model_age = age,
        parent = loc,
        compute_resources = list(
          "memory" = "10G",
          "cores" = 1L,
          "queue" = queue,
          "constraints" = "archive",
          "runtime" = "1200S"
        )
      )
      
      return(oy_raking_task)
      
    })
    
  })
  
  oy_raking_tasks <- lapply(rapply(oy_raking_tasks, enquote, how = "unlist"), eval)
  
  wf <- add_tasks(workflow = wf, tasks = oy_raking_tasks)
  
  hold_tasks <- oy_raking_tasks
  
} else { # close original method
  
  hold_tasks <- raking_tasks
  
}

# Run TFR
tfr_template <- task_template(
  tool = wf_tool,
  template_name = "tfr",
  command_template = paste(
    command_template_add_ons,
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--year_start {year_start}",
    "--year_end {year_end}",
    "--loc {loc}",
    "--loop {loop}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version_id", "gbd_year", "year_start", "year_end",
                   "loop", "loc")
)

tfr_tasks <- lapply(1:nrow(model_locs), function(loc) {
  
  tfr_task <- task(
    task_template = tfr_template,
    name = "tfr",
    upstream_tasks = hold_tasks,
    max_attempts = retries,
    shell_path = shell_path_r,
    image_path = image_path,
    script_path = glue::glue("{code_dir}/07_tfr.R"),
    version_id = version_id,
    gbd_year = gbd_year,
    year_start = year_start,
    year_end = year_end,
    loop = 2L,
    loc = model_locs[loc]$ihme_loc_id,
    compute_resources = list(
      "memory" = "20G",
      "cores" = 1L,
      "queue" = queue,
      "constraints" = "archive",
      "runtime" = "1200S"
    )
  )
  
  return(tfr_task)
  
})

wf <- add_tasks(workflow = wf, tasks = tfr_tasks)

# Run compile
compile_template <- task_template(
  tool = wf_tool,
  template_name = "compile",
  command_template = paste(
    command_template_add_ons,
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--loop {loop}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version_id", "loop")
)

compile_task <- task(
  task_template = compile_template,
  name = "compile",
  upstream_tasks = tfr_tasks,
  max_attempts = retries,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/08_compile.R"),
  version_id = version_id,
  loop = 2L,
  compute_resources = list(
    "memory" = "20G",
    "cores" = 1L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "1200S"
  )
)

wf <- add_tasks(workflow = wf, tasks = list(compile_task))

# Run split to single year
sy_split_template <- task_template(
  tool = wf_tool,
  template_name = "sy_split",
  command_template = paste(
    command_template_add_ons,
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--year_start {year_start}",
    "--year_end {year_end}",
    "--loc {loc}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version_id", "gbd_year", "year_start", "year_end",
                   "loc")
)

sy_split_tasks <- lapply(1:nrow(model_locs), function(loc) {
  
  sy_split_task <- task(
    task_template = sy_split_template,
    name = "sy_split",
    upstream_tasks = list(compile_task),
    max_attempts = retries,
    shell_path = shell_path_r,
    image_path = image_path,
    script_path = glue::glue("{code_dir}/09_split_to_single_age_asfr.R"),
    version_id = version_id,
    gbd_year = gbd_year,
    year_start = year_start,
    year_end = year_end,
    loc = model_locs[loc]$ihme_loc_id,
    compute_resources = list(
      "memory" = "20G",
      "cores" = 1L,
      "queue" = queue,
      "constraints" = "archive",
      "runtime" = "1200S"
    )
  )
  
  return(sy_split_task)
  
})

wf <- add_tasks(workflow = wf, tasks = sy_split_tasks)

# Run diagnostics
## Run plot prep -i.e. create flat files to be read into parallel plotting tasks
plot_prep_template <- task_template(
  tool = wf_tool,
  template_name = "plot_prep",
  command_template = paste(
    command_template_add_ons,
    "{shell_path} -i {image_path} -s {script_path}",
    "--gbd_year {gbd_year}",
    "--version1 {version1}",
    "--version2 {version2}",
    "--version3 {version3}"
  ),
  op_args = list("shell_path", "image_path"),
  task_args = list(),
  node_args = list("gbd_year", "version1", "version2", "version3",
                   "script_path")
)

plot_prep_task <- task(
  task_template = plot_prep_template,
  name = "plot_prep",
  upstream_tasks = list(compile_task),
  max_attempts = retries,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/plotting_old_new.R"),
  version1 = version1,
  version2 = version2,
  version3 = version3,
  gbd_year = gbd_year,
  compute_resources = list(
    "memory" = "10G",
    "cores" = 1L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "1200S"
  )
)

wf <- add_tasks(workflow = wf, tasks = list(plot_prep_task))

# Plot age-location specific ASFR and TFR
plot_template <- task_template(
  tool = wf_tool,
  template_name = "plot",
  command_template = paste(
    command_template_add_ons,
    "{shell_path} -i {image_path} -s {script_path}",
    "--gbd_year {gbd_year}",
    "--version1 {version1}",
    "--version2 {version2}",
    "--version3 {version3}",
    "--loc {loc}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("gbd_year", "version1", "version2", "version3",
                   "loc")
)

plot_tasks <- lapply(1:nrow(model_locs), function(loc) {
  
  plot_task <- task(
    task_template = plot_template,
    name = "plot",
    upstream_tasks = list(plot_prep_task),
    max_attempts = retries,
    shell_path = shell_path_r,
    image_path = image_path,
    script_path = glue::glue("{code_dir}/plot_loc_specific_old_new.R"),
    loc = model_locs[loc]$ihme_loc_id,
    version1 = version1,
    version2 = version2,
    version3 = version3,
    gbd_year = gbd_year,
    compute_resources = list(
      "memory" = "5G",
      "cores" = 1L,
      "queue" = queue,
      "constraints" = "archive",
      "runtime" = "1200S"
    )
  )
  
  return(plot_task)
  
})

wf <- add_tasks(workflow = wf, tasks = plot_tasks)

## Run append plots into a single pdf
append_template <- task_template(
  tool = wf_tool,
  template_name = "append_plots",
  command_template = paste(
    command_template_add_ons,
    "OMP_NUM_THREADS=1",
    "{script_path} {code_path} {conda_path} {conda_env}",
    "{model_version_id}",
    "{gbd_year}"
  ),
  op_args = list("script_path", "code_path", "conda_path", "conda_env"),
  task_args = list(),
  node_args = list("model_version_id", "gbd_year")
)

append_task <- task(
  task_template = append_template,
  name = "append_plots",
  upstream_tasks = plot_tasks,
  max_attempts = 2,
  code_path = glue::glue("{code_dir}/append_plots.py"),
  conda_path = conda_path,
  conda_env = conda_env,
  script_path = glue::glue("{code_dir}/append_plots.sh"),
  model_version_id = version_id,
  gbd_year = gbd_year,
  compute_resources = list(
    "memory" = "5G",
    "cores" = 1L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "1200S"
  )
)

wf <- add_tasks(workflow = wf, tasks = list(append_task))

## Run scatter
# Create a scatter plot of differences between versions of the model
scatter_template <- task_template(
  tool = wf_tool,
  template_name = "scatter",
  command_template = paste(
    command_template_add_ons,
    "{shell_path} -i {image_path} -s {script_path}",
    "--gbd_year {gbd_year}",
    "--version1 {version1}",
    "--version2 {version2}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("gbd_year","version1", "version2")
)

scatter_task <- task(
  task_template = scatter_template,
  name = "scatter",
  upstream_tasks = plot_tasks,
  max_attempts = retries,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/plot_scatter.R"),
  version1 = version1,
  version2 = version2,
  gbd_year = gbd_year,
  compute_resources = list(
    "memory" = "5G",
    "cores" = 1L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "1200S"
  )
)

wf <- add_tasks(workflow = wf, tasks = list(scatter_task))

# Run plot bot - ping the fertility channel saying plots are ready
bot_template <- task_template(
  tool = wf_tool,
  template_name = "bot",
  command_template = paste(
    command_template_add_ons,
    "{shell_path} -i {image_path} -s {script_path}",
    "--version1 {version1}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version1")
)

bot_task <- task(
  task_template = bot_template,
  name = "bot",
  upstream_tasks = list(append_task),
  max_attempts = retries,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/plot_bot.R"),
  version1 = version1,
  compute_resources = list(
    "memory" = "1G",
    "cores" = 1L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "1200S"
  )
)

wf <- add_tasks(workflow = wf, tasks = list(bot_task))

## Run workflow ----------------------------------------------------------------

Sys.setenv("" = "1")
Sys.setenv("R_LIBS" = "")
wfr <- wf$run(resume = TRUE)

## Upload to database or report model failure ----------------------------------

if (wfr != "D") {
  
  # Send failure notification
  send_slack_message(
    message = paste0("ASFR pipeline ", workflow_args, " failed."),
    channel = paste0("@", username),
    icon = ":thinking_face:",
    botname = "FertBot"
  )
  
  jobmon_errors <- get_jobmon_errors(
    wf$workflow_id,
    odbc_section = "DATABASE"
  )
  
  readr::write_csv(jobmon_errors, fs::path("FILEPATH"))
  
  # Stop process
  stop("ASFR failed!")
  
} else {
  
  message("ASFR completed!")
  
}


# Purpose: establishes the DAG and the Pipeline

############ setup pipeline, paths, parameters, etc. ############ 
user <- Sys.info()["user"]
cause <- "ntd_lf" 
source("FILEPATH")

# establish parameters
PROJECT <- "ADDRESS"
QUEUE <- "ADDRESS"

# set the working directory
setwd("FILEPATH")

# enable weights
if(utils::compareVersion(paste0(R.version$major, ".", R.version$minor),"4.1.3") == -1) {
  inla.setOption("enable.inla.argument.weights", TRUE)
}

if (rlang::is_interactive()) {
  if (rstudioapi::isAvailable()) {
    this.file <- rstudioapi::getSourceEditorContext()$path
  } else {
    warning("sorry - we don't know how to determine the file path of this script in non-RStudio interactive sessions")
    this.file <- NULL
  }
} else {
  this.file <- rprojroot::thisfile()
}

if (is.null(this.file)) {
  warning("We expect all pipeline files to be in the same directory. We cannot determine what directory this file is in, so we are assuming your getwd() IS this directory. If it isn't things will fail.")
} else {
  setwd(dirname(this.file))
}

## set directory for input data 
fp_list$input_data_root <- "FILEPATH"
## configuration file path
config <- "FILEPATH"
## covariate file path
covs <- "FILEPATH"

# create the MBGPipeline object
pipeline <- MBGPipeline$new(
  indicator_group = indicator_group,
  indicator = indicator,
  config_file = config,
  covs_file = covs
)

suppressMessages(suppressWarnings(pipeline$setup_conf(
  push_to_global_env = FALSE,
  run_tests = FALSE
)))

# create unique run_date directory
pipeline$setup_rundate(sprintf(paste0(as.character(Sys.Date()),'_', user, "_", reg_tag)),full_cleanup = TRUE)

# creates holdouts for model validation and loopvars for parallel processing
pipeline$make_holdouts()
pipeline$create_loopvars()

# save new objects and environments 
## create filepath object for output directory 
output_dir <- "FILEPATH"

## save the new pipeline object
save(pipeline, file = "FILEPATH")
## save the config file to run_date directory for posterity
write.csv("FILEPATH")
## save fixed effects (covs) file to run_date directory for records
write.csv(pipeline$fixed_effects_config, file.path("FILEPATH"))

# create personal directory for additional libraries
r_lib <- paste0("FILEPATH")
if(!exists(r_lib)){
  dir.create(r_lib)
}

# install ggnewscale 
if (!("ggnewscale" %in% installed.packages())){
  install.packages("ggnewscale", lib = r_lib)
}

# make MBG DAG
dag <- MBGDag$new(
  save_dir = sprintf(output_dir),
  dag_hash = "FAKE_HASH")

predict.ids <- vector("integer")
postest.ids <- vector("integer")

for (i in 1:nrow(pipeline$loopvars)) {
  prep.data.id <- dag$create_node(
    pipeline = pipeline,
    base_name = "j01_data_prep",
    loopvar_index = i,
    jobscript = "01_PM_Data_Prepping.R",
    project = PROJECT,
    cores = 2,
    ram_gb = 40, 
    runtime = "04:00:00",
    queue = QUEUE,
    singularity_version = "FILEPATH"
  )

  stack.id <- dag$create_node(
    pipeline,
    base_name = "j02_stacking",
    loopvar_index = i,
    jobscript = "02_PM_Stacking.R",
    project = PROJECT,
    cores = 6,
    ram_gb = 180,
    runtime = "16:00:00",
    queue = QUEUE,
    hold_job = "previous",
    singularity_version = "FILEPATH"
  )

  prep.inla.id <- dag$create_node(
    pipeline,
    base_name = "j03_prep_for_INLA",
    loopvar_index = i,
    jobscript = "03_PM_PrepForFitting.R",
    project = PROJECT,
    cores = 2,
    ram_gb = 40,
    runtime = "04:00:00",
    queue = QUEUE,
    hold_job = "previous",
    singularity_version = "FILEPATH"
  )

  fit.id <- dag$create_node(
    pipeline,
    base_name = "j04_MBG_fitting",
    loopvar_index = i,
    jobscript = "04_PM_MBGFitting.R",
    project = PROJECT,
    cores = 10,
    ram_gb = 260,
    runtime = "20:00:00",
    queue = QUEUE,
    hold_job = "previous",
    singularity_version = "FILEPATH"
  )

  predict.id <- dag$create_node(
    pipeline,
    base_name = "j05_MBG_predict",
    loopvar_index = i,
    jobscript = "05_PM_MBGPredict.R",
    project = PROJECT,
    cores = 10,
    ram_gb = 400,
    runtime = "60:00:00",
    queue = QUEUE,
    hold_job = "previous",
    singularity_version = "FILEPATH"
  )
  predict.ids <- c(predict.ids, predict.id)
  
  postest.id <- dag$create_node(
    pipeline,
    base_name = "j06_MBG_postest",
    loopvar_index = i,
    jobscript = "06_PM_FraxAgg.R",
    project = PROJECT,
    cores = 10,
    ram_gb = 400,
    runtime = "12:00:00",
    queue = QUEUE,
    hold_job = "previous",
    singularity_version = "FILEPATH"
  )
  postest.ids <- c(postest.ids, postest.id)
}

## load GBD Estimates for this indicator which will be used in raking -- ensures that final numbers align with GBD numbers
gbd <- get_gbd_estimates(gbd_name = ADDRESS,
                         region = paste(pipeline$loopvars[, region], collapse = "+"),
                         measure_id = 5,
                         age_group_id = 1,
                         metric_id = 3,
                         year_ids = pipeline$config_list$year_list,
                         shapefile_version = pipeline$config_list$raking_shapefile_version,
                         rake_subnational = pipeline$config_list$subnational_raking,
                         gbd_round_id = ADDRESS,
                         version = 'ADDRESS',
                         decomp_step = "ADDRESS")


# save objects to reload in postestfrax
prep_postest(
  indicator = pipeline$indicator,
  indicator_group = pipeline$indicator_group,
  run_date = pipeline$run_date,
  save_objs = c("gbd")
)

# set the working directory again
setwd("FILEPATH")

## save DAG
print(sprintf("Saving DAG to %s", dag$save_file))
dag$save()

## submit everything in the DAG
dag$submit_jobs()                 # submit jobs (1 job per script/node)
dag$save()                        # save now that job numbers are known
dag$wait_on_node(node_id = NULL)  # monitor DAG - NULL will monitor all nodes
dag$save()                        # save again before exiting

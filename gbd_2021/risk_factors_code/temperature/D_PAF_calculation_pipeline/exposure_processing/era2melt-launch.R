## Launches processing for location-level exposure data
# source("/ihme/code/eod/mjassmus/temperature/code_GBD2021/exposure_processing/era2melt-launch.R", echo = T)

## SYSTEM SETUP ----------------------------------------------------------
# Clear memory
rm(list=ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]


# Drives 
j <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"


# Base filepaths
work_dir <- paste0(j, '/FILEPATH/')
share_dir <- '/ihme/erf/' # only accessible from the cluster
code_dir <- if (os == "Linux") paste0("/FILEPATH/", user, "/") else if (os == "Windows") ""


## LOAD DEPENDENCIES -----------------------------------------------------
library("data.table")
library("ggplot2")
library(argparse)
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_cause_metadata.R")
source("/FILEPATH/get_ids.R")
source(paste0(code_dir, "FILEPATH/csv2objects.R"))
#source("~/R/submitExperiment.R")
Sys.setenv("RETICULATE_PYTHON"='/FILEPATH/')  # Set the Python interpreter path
#library(jobmonr)
source("submitExperiment.R")




## DEFINE ARGS -----------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--year_start", help = "first of years to evaluate",
                    default = 1990, type = "integer")
parser$add_argument("--year_end", help = "last of years to evaluate",
                    default = 2021, type = "integer")
parser$add_argument("--release_id", help = "GBD release ID",
                    default = 10, type = "integer")
parser$add_argument("--proj", help = "project to run on",
                    default = 'NAME', type = "character")
parser$add_argument("--outdir", help = "output path",
                    default = '/FILEPATH/', type = "character")
parser$add_argument("--resume", help = "whether you're resuming an existing pipeline",
                    default = F, type = "logical")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

yearList <- year_start:year_end
gbd_round_year <- strsplit(get_ids("release")[release_id == .GlobalEnv$release_id, release_name], " ")[[1]][2]


# Get locations to run on
locMeta <- get_location_metadata(location_set_id = 35, release_id = release_id)
loc_ids <- locMeta[(is_estimate==1 & most_detailed==1) | level==3 , location_id]
loc_ids <- loc_ids[order(loc_ids)]

# Generate arg list for jobs
arg.list <- list(loc_id = loc_ids, year = yearList, gbd_round_year = gbd_round_year, outdir = outdir)
name.stub <- "melt"
name.args <- c("loc_id", "year")

# Set up run environment
sge.output.dir <- paste0(" -o /FILEPATH/", user, "/output/%x.o%j -e /FILEPATH/", user, "/errors/%x.e%j ")
r.shell <- "/ihme/singularity-images/rstudio/shells/execRscript.sh"
save.script <- paste0(code_dir, "FILEPATH/exposure_processing/era2melt.R")
mem <- "75G"
slots <- 8
time <- "01:00:00"
project <- "NAME"


# Launch jobs
arg.table <- launch.jobs(arg.list = arg.list, name.stub = name.stub, name.args = name.args, outfile.suffix = ".csv",
                         pause = 1, max.attempts = 1, return = T, relaunch = T)


status <- data.table()
for (loc in loc_ids) {
  print(loc)
  for (y in yearList) {
    cat(".")
    status <- rbind(status, 
                    data.table(location_id = loc, year_id = y, 
                               complete = file.exists(paste0(outdir, "/melt_", loc, "_", y, ".csv"))))
  }
}

# # Convert the list of arguments into a data table with all combinations of all arguments
# arg.table <- data.table(merge = 1, missing = 1, running = 0, failed = 0, attempts = 0, job.name = "")
# for (arg in names(arg.list)) {
#   temp <- data.table(unlist(arg.list[arg]), 1)
#   names(temp) <- c(paste0("arg.", arg), "merge")
#   
#   arg.table <- merge(arg.table, temp, by = "merge", allow.cartesian = T, all = T)
# }
# 
# arg.vars <- grep("^arg.", names(arg.table), value = T)
# arg.table[, job.name := do.call(paste, c(name.stub, .SD, sep = "_")), .SDcols = paste0("arg.", name.args)]
# arg.table[, args := do.call(paste, .SD), .SDcols = c(arg.vars, "job.name")]
# 
# arg.table[, merge := NULL]
# 

# ## BODY ------------------------------------------------------------------
# # Set up Jobmon pipeline ---------------------------------------
# # Create a tool
# my_tool <- tool(name='temper_era2melt_tool')
# 
# # Bind a workflow to the tool
# wf <- workflow(my_tool,
#                workflow_args=paste0('temper_era2melt_workflow_', Sys.Date()),
#                name='temper_era2melt_workflow')
# 
# # Create templates
# era2melt_tt <- task_template(tool=my_tool,
#                             template_name='era2melt_templ',
#                             command_template=paste("{rshell} -s {script} --loc_id {loc_id} --year {year} --gbd_round_year {gbd_round_year} --outDir {outDir} --job.name {job.name}"),
#                             task_args = list('outDir'),
#                             node_args = list('loc_id', 'year', 'job.name'),
#                             op_args = list('rshell', 'script'))
# 
# 
# # Set the script task template compute resources
# era2melt_tt_resources <- jobmonr::set_default_template_resources(
#   task_template=era2melt_tt,
#   default_cluster_name='NAME',
#   resources=list(
#     'cores'=8,
#     'queue'='all.q',
#     'runtime'="10m",
#     'memory'='150G'
#   )
# )
# 
# 
# # Make tasks ---------------------------------------------------
# all_tasks <- list()
# 
# # Generate exposure calculation tasks
# calc_subtasks <- list()
# for (i in 1:nrow(arg.table)) {
#   # Add the associated script task
#   task <- task(task_template=era2melt_tt,
#                name = arg.table$job.name[i],
#                rshell = r.shell,
#                script = paste0(code_dir, "temperature/code_GBD2021/exposure_processing/era2melt.R"),
#                outDir = outdir,
#                gbd_round_year = gbd_round_year,
#                loc_id = arg.table$arg.loc_id[i],
#                year = arg.table$arg.year[i],
#                job.name = arg.table$job.name[i]
#   )
#   calc_subtasks <- append(calc_subtasks, task)
#   all_tasks <- append(all_tasks, task)
# }
# 
# 
# # Add tasks to the workflow
# wf <- add_tasks(wf, subtasks)
# 
# # Run tasks ----------------------------------------------------
# wfr <- run(
#   workflow=wf,
#   resume=resume)
